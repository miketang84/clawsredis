use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::thread;

use crate::{execute_command, write_resp_value, KVStore, RespParser, RespValue};

/// Shared store used by all TCP client handler threads.
pub type SharedStore = Arc<Mutex<KVStore>>;

/// Binds a TCP listener on `addr` and serves RESP2 clients forever.
pub fn run_server<A: ToSocketAddrs>(addr: A, store: SharedStore) -> io::Result<()> {
    let listener = TcpListener::bind(addr)?;
    serve_listener(listener, store)
}

/// Serves RESP2 clients from an already-bound listener.
pub fn serve_listener(listener: TcpListener, store: SharedStore) -> io::Result<()> {
    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                let client_store = Arc::clone(&store);
                thread::spawn(move || {
                    if let Err(err) = handle_client(stream, client_store) {
                        eprintln!("client connection ended with error: {}", err);
                    }
                });
            }
            Err(err) if is_disconnect_error(&err) => continue,
            Err(err) => return Err(err),
        }
    }

    Ok(())
}

/// Handles one RESP2 client connection until the peer disconnects or sends `QUIT`.
pub fn handle_client(mut stream: TcpStream, store: SharedStore) -> io::Result<()> {
    let mut parser = RespParser::new();
    let mut buffer = [0_u8; 4096];

    loop {
        match stream.read(&mut buffer) {
            Ok(0) => return Ok(()),
            Ok(n) => {
                parser.feed(&buffer[..n]);
                loop {
                    let command = match parser.next_command() {
                        Ok(Some(command)) => command,
                        Ok(None) => break,
                        Err(err) => {
                            let response = RespValue::Error(format!("Error: {}", err));
                            write_response(&mut stream, &response)?;
                            return Ok(());
                        }
                    };
                    let should_quit = match command.first() {
                        Some(command) => command.eq_ignore_ascii_case("QUIT"),
                        None => false,
                    };
                    let response = execute_with_store(&store, &command);
                    write_response(&mut stream, &response)?;
                    if should_quit {
                        return Ok(());
                    }
                }
            }
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) if is_disconnect_error(&err) => return Ok(()),
            Err(err) => return Err(err),
        }
    }
}

fn execute_with_store(store: &SharedStore, command: &[String]) -> RespValue {
    match store.lock() {
        Ok(mut store) => execute_command(&mut store, command),
        Err(_) => RespValue::Error("Error: storage lock poisoned".to_string()),
    }
}

fn write_response(stream: &mut TcpStream, response: &RespValue) -> io::Result<()> {
    match write_resp_value(stream, response).and_then(|_| stream.flush()) {
        Ok(()) => Ok(()),
        Err(err) if is_disconnect_error(&err) => Ok(()),
        Err(err) => Err(err),
    }
}

fn is_disconnect_error(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::UnexpectedEof
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn handle_client_executes_resp_commands() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("test listener should bind");
        let addr = listener.local_addr().expect("listener should have an addr");
        let store = Arc::new(Mutex::new(KVStore::new()));
        let server_store = Arc::clone(&store);

        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().expect("client should connect");
            handle_client(stream, server_store).expect("client should be handled");
        });

        let mut client = TcpStream::connect(addr).expect("client should connect");
        client
            .set_read_timeout(Some(Duration::from_secs(1)))
            .expect("read timeout should be set");
        client
            .write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n")
            .expect("SET should be written");
        let mut response = [0_u8; 5];
        client.read_exact(&mut response).expect("SET response should arrive");
        assert_eq!(&response, b"+OK\r\n");

        client
            .write_all(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n")
            .expect("GET should be written");
        let mut response = [0_u8; 7];
        client.read_exact(&mut response).expect("GET response should arrive");
        assert_eq!(&response, b"$1\r\nv\r\n");

        client
            .write_all(b"*1\r\n$4\r\nQUIT\r\n")
            .expect("QUIT should be written");
        let mut response = [0_u8; 11];
        client.read_exact(&mut response).expect("QUIT response should arrive");
        assert_eq!(&response, b"+Goodbye!\r\n");

        server.join().expect("server thread should finish");
    }
}
