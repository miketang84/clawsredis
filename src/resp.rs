use std::fmt;
use std::io::{self, Read};

/// A parsed RESP2 frame.
#[derive(Debug, Clone, PartialEq)]
pub enum RespFrame {
    /// `+...\r\n`
    Simple(String),
    /// `-...\r\n`
    Error(String),
    /// `:<number>\r\n`
    Integer(i64),
    /// `$<len>\r\n...\r\n`, with `None` representing `$-1`.
    Bulk(Option<Vec<u8>>),
    /// `*<len>\r\n...`, with `None` representing `*-1`.
    Array(Option<Vec<RespFrame>>),
}

/// Error returned while parsing RESP2 data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RespParseError {
    /// The byte stream violates the RESP2 format.
    Malformed(String),
    /// The stream ended before a complete frame was available.
    UnexpectedEof,
    /// The reader returned an I/O error.
    Io(String),
}

impl fmt::Display for RespParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed(message) => write!(f, "malformed RESP: {}", message),
            Self::UnexpectedEof => write!(f, "unexpected end of RESP stream"),
            Self::Io(message) => write!(f, "I/O error while reading RESP: {}", message),
        }
    }
}

impl std::error::Error for RespParseError {}

impl From<io::Error> for RespParseError {
    fn from(value: io::Error) -> Self {
        Self::Io(value.to_string())
    }
}

/// Incremental RESP2 parser suitable for streamed TCP input.
#[derive(Debug, Default)]
pub struct RespParser {
    buffer: Vec<u8>,
}

impl RespParser {
    /// Creates an empty parser.
    pub fn new() -> Self {
        Self { buffer: Vec::new() }
    }

    /// Appends bytes read from a stream.
    pub fn feed(&mut self, bytes: &[u8]) {
        self.buffer.extend_from_slice(bytes);
    }

    /// Returns the number of currently buffered bytes.
    pub fn buffered_len(&self) -> usize {
        self.buffer.len()
    }

    /// Attempts to parse one complete RESP frame from the current buffer.
    pub fn next_frame(&mut self) -> Result<Option<RespFrame>, RespParseError> {
        match parse_frame(&self.buffer, 0)? {
            ParseResult::Complete { frame, consumed } => {
                self.buffer.drain(0..consumed);
                Ok(Some(frame))
            }
            ParseResult::Incomplete => Ok(None),
        }
    }

    /// Attempts to parse one complete command as a vector of strings.
    pub fn next_command(&mut self) -> Result<Option<Vec<String>>, RespParseError> {
        match self.next_frame()? {
            Some(frame) => frame_to_command(frame).map(Some),
            None => Ok(None),
        }
    }
}

/// Reads from `reader` until one full RESP command is available or EOF is reached.
pub fn read_resp_command<R: Read>(reader: &mut R) -> Result<Option<Vec<String>>, RespParseError> {
    let mut parser = RespParser::new();
    let mut chunk = [0_u8; 1];

    loop {
        if let Some(command) = parser.next_command()? {
            return Ok(Some(command));
        }

        match reader.read(&mut chunk) {
            Ok(0) => {
                if parser.buffered_len() == 0 {
                    return Ok(None);
                }
                return Err(RespParseError::UnexpectedEof);
            }
            Ok(n) => parser.feed(&chunk[..n]),
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(RespParseError::from(err)),
        }
    }
}

/// Parses a single complete RESP command from an in-memory byte slice.
pub fn parse_resp_command(bytes: &[u8]) -> Result<Option<Vec<String>>, RespParseError> {
    let mut parser = RespParser::new();
    parser.feed(bytes);
    parser.next_command()
}

enum ParseResult {
    Complete { frame: RespFrame, consumed: usize },
    Incomplete,
}

fn parse_frame(input: &[u8], start: usize) -> Result<ParseResult, RespParseError> {
    if start >= input.len() {
        return Ok(ParseResult::Incomplete);
    }

    match input[start] {
        b'+' => parse_line_string(input, start + 1).map_complete(RespFrame::Simple),
        b'-' => parse_line_string(input, start + 1).map_complete(RespFrame::Error),
        b':' => parse_integer(input, start + 1),
        b'$' => parse_bulk(input, start + 1),
        b'*' => parse_array(input, start + 1),
        other => Err(RespParseError::Malformed(format!(
            "unknown RESP type byte 0x{other:02x}"
        ))),
    }
}

trait MapCompleteString {
    fn map_complete<F>(self, f: F) -> Result<ParseResult, RespParseError>
    where
        F: FnOnce(String) -> RespFrame;
}

impl MapCompleteString for Result<Option<(String, usize)>, RespParseError> {
    fn map_complete<F>(self, f: F) -> Result<ParseResult, RespParseError>
    where
        F: FnOnce(String) -> RespFrame,
    {
        match self? {
            Some((value, consumed)) => Ok(ParseResult::Complete {
                frame: f(value),
                consumed,
            }),
            None => Ok(ParseResult::Incomplete),
        }
    }
}

fn parse_line_string(
    input: &[u8],
    line_start: usize,
) -> Result<Option<(String, usize)>, RespParseError> {
    let Some(line_end) = find_crlf(input, line_start) else {
        return Ok(None);
    };
    let text = std::str::from_utf8(&input[line_start..line_end])
        .map_err(|_| RespParseError::Malformed("line is not valid UTF-8".to_string()))?;
    Ok(Some((text.to_string(), line_end + 2)))
}

fn parse_integer(input: &[u8], line_start: usize) -> Result<ParseResult, RespParseError> {
    let Some((text, consumed)) = parse_line_string(input, line_start)? else {
        return Ok(ParseResult::Incomplete);
    };
    let value = text
        .parse::<i64>()
        .map_err(|_| RespParseError::Malformed(format!("invalid integer {text:?}")))?;
    Ok(ParseResult::Complete {
        frame: RespFrame::Integer(value),
        consumed,
    })
}

fn parse_length(input: &[u8], line_start: usize) -> Result<Option<(isize, usize)>, RespParseError> {
    let Some((text, consumed)) = parse_line_string(input, line_start)? else {
        return Ok(None);
    };
    let value = text
        .parse::<isize>()
        .map_err(|_| RespParseError::Malformed(format!("invalid length {text:?}")))?;
    Ok(Some((value, consumed)))
}

fn parse_bulk(input: &[u8], line_start: usize) -> Result<ParseResult, RespParseError> {
    let Some((len, data_start)) = parse_length(input, line_start)? else {
        return Ok(ParseResult::Incomplete);
    };
    if len == -1 {
        return Ok(ParseResult::Complete {
            frame: RespFrame::Bulk(None),
            consumed: data_start,
        });
    }
    if len < 0 {
        return Err(RespParseError::Malformed(format!(
            "invalid negative bulk length {len}"
        )));
    }

    let len = len as usize;
    let data_end = data_start.saturating_add(len);
    let frame_end = data_end.saturating_add(2);
    if input.len() < frame_end {
        return Ok(ParseResult::Incomplete);
    }
    if &input[data_end..frame_end] != b"\r\n" {
        return Err(RespParseError::Malformed(
            "bulk string is not terminated by CRLF".to_string(),
        ));
    }

    Ok(ParseResult::Complete {
        frame: RespFrame::Bulk(Some(input[data_start..data_end].to_vec())),
        consumed: frame_end,
    })
}

fn parse_array(input: &[u8], line_start: usize) -> Result<ParseResult, RespParseError> {
    let Some((len, mut cursor)) = parse_length(input, line_start)? else {
        return Ok(ParseResult::Incomplete);
    };
    if len == -1 {
        return Ok(ParseResult::Complete {
            frame: RespFrame::Array(None),
            consumed: cursor,
        });
    }
    if len < 0 {
        return Err(RespParseError::Malformed(format!(
            "invalid negative array length {len}"
        )));
    }

    let mut values = Vec::with_capacity(len as usize);
    for _ in 0..len {
        match parse_frame(input, cursor)? {
            ParseResult::Complete { frame, consumed } => {
                values.push(frame);
                cursor = consumed;
            }
            ParseResult::Incomplete => return Ok(ParseResult::Incomplete),
        }
    }

    Ok(ParseResult::Complete {
        frame: RespFrame::Array(Some(values)),
        consumed: cursor,
    })
}

fn find_crlf(input: &[u8], start: usize) -> Option<usize> {
    if input.len() < 2 || start >= input.len() {
        return None;
    }

    let mut i = start;
    while i + 1 < input.len() {
        if input[i] == b'\r' && input[i + 1] == b'\n' {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn frame_to_command(frame: RespFrame) -> Result<Vec<String>, RespParseError> {
    match frame {
        RespFrame::Array(Some(values)) => values.into_iter().map(frame_to_argument).collect(),
        RespFrame::Array(None) => Err(RespParseError::Malformed(
            "null array cannot be used as a command".to_string(),
        )),
        other => Ok(vec![frame_to_argument(other)?]),
    }
}

fn frame_to_argument(frame: RespFrame) -> Result<String, RespParseError> {
    match frame {
        RespFrame::Simple(value) => Ok(value),
        RespFrame::Integer(value) => Ok(value.to_string()),
        RespFrame::Bulk(Some(bytes)) => String::from_utf8(bytes)
            .map_err(|_| RespParseError::Malformed("bulk argument is not valid UTF-8".to_string())),
        RespFrame::Error(message) => Err(RespParseError::Malformed(format!(
            "error frame cannot be used as a command argument: {message}"
        ))),
        RespFrame::Bulk(None) => Err(RespParseError::Malformed(
            "null bulk cannot be used as a command argument".to_string(),
        )),
        RespFrame::Array(_) => Err(RespParseError::Malformed(
            "nested array cannot be used as a command argument".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn parses_command_array_from_bulk_strings() {
        let command = parse_resp_command(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
            .expect("parser should succeed")
            .expect("frame should be complete");

        assert_eq!(command, vec!["GET".to_string(), "foo".to_string()]);
    }

    #[test]
    fn incremental_parser_waits_for_complete_frame() {
        let mut parser = RespParser::new();
        parser.feed(b"*2\r\n$3\r\nGET\r\n");
        assert_eq!(parser.next_command().expect("partial parse should not fail"), None);

        parser.feed(b"$3\r\nfoo\r\n");
        assert_eq!(
            parser.next_command().expect("full parse should succeed"),
            Some(vec!["GET".to_string(), "foo".to_string()])
        );
    }

    #[test]
    fn parses_simple_integer_and_bulk_frames_as_single_argument_commands() {
        assert_eq!(
            parse_resp_command(b"+PING\r\n").expect("simple parse"),
            Some(vec!["PING".to_string()])
        );
        assert_eq!(
            parse_resp_command(b":42\r\n").expect("integer parse"),
            Some(vec!["42".to_string()])
        );
        assert_eq!(
            parse_resp_command(b"$4\r\nECHO\r\n").expect("bulk parse"),
            Some(vec!["ECHO".to_string()])
        );
    }

    #[test]
    fn read_resp_command_handles_streamed_reader() {
        let mut cursor = Cursor::new(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n".to_vec());
        let command = read_resp_command(&mut cursor)
            .expect("read should succeed")
            .expect("command should be present");

        assert_eq!(command, vec!["GET".to_string(), "foo".to_string()]);
    }

    #[test]
    fn malformed_bulk_terminator_is_error() {
        let err = parse_resp_command(b"$3\r\nfooXX")
            .expect_err("bulk without trailing CRLF should fail once enough bytes arrive");

        assert!(matches!(err, RespParseError::Malformed(_)));
    }

    #[test]
    fn incomplete_input_returns_none() {
        let parsed = parse_resp_command(b"*2\r\n$3\r\nGET\r\n").expect("partial data is not malformed");
        assert_eq!(parsed, None);
    }

    #[test]
    fn eof_before_complete_command_is_error() {
        let mut cursor = Cursor::new(b"*2\r\n$3\r\nGET\r\n".to_vec());
        let err = read_resp_command(&mut cursor).expect_err("EOF should be reported");

        assert_eq!(err, RespParseError::UnexpectedEof);
    }
}
