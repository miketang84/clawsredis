use std::env;
use std::io::{self, BufRead, Write};
use std::sync::{Arc, Mutex};

use test001::{execute_command, run_server, KVStore, RespValue};

fn parse_cli_args(input: &str) -> Vec<String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }

    let mut command_and_rest = trimmed.splitn(2, char::is_whitespace);
    let command = match command_and_rest.next() {
        Some(command) => command.to_string(),
        None => return Vec::new(),
    };
    let rest = command_and_rest.next().unwrap_or("").trim_start();

    if command.eq_ignore_ascii_case("PUBLISH") {
        let mut args = vec![command];
        let mut channel_and_message = rest.splitn(2, char::is_whitespace);
        if let Some(channel) = channel_and_message.next() {
            if !channel.is_empty() {
                args.push(channel.to_string());
            }
        }
        if let Some(message) = channel_and_message.next() {
            args.push(message.trim_start().to_string());
        }
        return args;
    }

    let mut args = vec![command];
    args.extend(rest.split_whitespace().map(String::from));
    args
}

fn write_cli_value<W: Write>(writer: &mut W, value: &RespValue) -> io::Result<()> {
    match value {
        RespValue::Simple(message) | RespValue::Error(message) | RespValue::Bulk(message) => {
            writeln!(writer, "{}", message)
        }
        RespValue::Int(value) => writeln!(writer, "(integer) {}", value),
        RespValue::Nil => writeln!(writer, "(nil)"),
        RespValue::Array(values) => {
            if values.is_empty() {
                writeln!(writer, "(empty array)")
            } else {
                for item in values {
                    write_cli_array_item(writer, item)?;
                }
                Ok(())
            }
        }
    }
}

fn write_cli_array_item<W: Write>(writer: &mut W, value: &RespValue) -> io::Result<()> {
    match value {
        RespValue::Simple(message) | RespValue::Error(message) | RespValue::Bulk(message) => {
            writeln!(writer, "- {}", message)
        }
        RespValue::Int(value) => writeln!(writer, "(integer) {}", value),
        RespValue::Nil => writeln!(writer, "(nil)"),
        RespValue::Array(values) => {
            for item in values {
                write_cli_array_item(writer, item)?;
            }
            Ok(())
        }
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct CliOptions {
    server_addr: Option<String>,
    load_path: Option<String>,
}

fn parse_options(args: &[String]) -> Result<CliOptions, String> {
    let mut options = CliOptions::default();
    let mut index = 1;

    while index < args.len() {
        match args[index].as_str() {
            "--server" => {
                let value = option_value(args, index, "--server")?;
                options.server_addr = Some(value.to_string());
                index += 2;
            }
            "--load" => {
                let value = option_value(args, index, "--load")?;
                options.load_path = Some(value.to_string());
                index += 2;
            }
            "--help" | "-h" => return Err(usage(program_name(args))),
            unknown => return Err(format!(
                "Unknown argument: {}\n{}",
                unknown,
                usage(program_name(args))
            )),
        }
    }

    Ok(options)
}

fn option_value<'a>(args: &'a [String], index: usize, flag: &str) -> Result<&'a str, String> {
    match args.get(index + 1) {
        Some(value) if !value.starts_with("--") => Ok(value),
        _ => Err(format!("{} requires a value\n{}", flag, usage(program_name(args)))),
    }
}

fn program_name(args: &[String]) -> &str {
    args.first().map_or("clawsredis", String::as_str)
}

fn usage(program: &str) -> String {
    format!(
        "Usage: {} [--load <path>] [--server <addr>]\nDefault mode is interactive CLI when --server is omitted.",
        program
    )
}

fn load_store(load_path: Option<&str>) -> KVStore {
    match load_path {
        Some(path) => match KVStore::load(path) {
            Ok(store) => {
                println!("Loaded state from {}", path);
                store
            }
            Err(err) => {
                eprintln!("Failed to load state from {}: {}", path, err);
                let mut store = KVStore::new();
                store.set_persist_path(path.to_string());
                store
            }
        },
        None => KVStore::new(),
    }
}

fn main() {
    if let Err(err) = run_app(env::args().collect()) {
        eprintln!("{}", err);
        std::process::exit(1);
    }
}

fn run_app(args: Vec<String>) -> Result<(), String> {
    let options = parse_options(&args)?;
    let storage = Arc::new(Mutex::new(load_store(options.load_path.as_deref())));

    if let Some(addr) = options.server_addr {
        println!("KV Storage (Redis-like) - RESP server listening on {}", addr);
        run_server(addr.as_str(), storage).map_err(|err| format!("Server failed: {}", err))
    } else {
        run_interactive_cli(storage).map_err(|err| format!("CLI failed: {}", err))
    }
}

fn run_interactive_cli(storage: Arc<Mutex<KVStore>>) -> io::Result<()> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut stdout_lock = stdout.lock();

    println!("KV Storage (Redis-like) - Basic Edition");
    println!(
        "Commands: GET <key>, SET <key> <value> [TTL], SETNX <key> <value>, SETEX <key> <seconds> <value>, MSET <k1> <v1> [k2 v2...], MSETNX <k1> <v1> [k2 v2...], MGET <k1> [k2...], APPEND <key> <value>, STRLEN <key>, GETSET <key> <value>, INCR <key>, DECR <key>, INCRBY <key> <n>, DECRBY <key> <n>, DEL <key>, KEYS, SUBSCRIBE <channel>, PUBLISH <channel> <message>, UNSUBSCRIBE <channel> <sub_id>, TTL <key>, EXPIRE <key> <seconds>, PERSISTKEY <key>, PERSIST <path>, RPUSH <key> <v1> [v2...], LPUSH <key> <v1> [v2...], LRANGE <key> <start> <stop>, LLEN <key>, HSET <key> <field> <value> [field value...], HMSET <key> <field> <value> [field value...], HGET <key> <field>, HMGET <key> <field> [field...], HDEL <key> <field> [field...], HEXISTS <key> <field>, HLEN <key>, HKEYS <key>, HVALS <key>, HGETALL <key>, ZADD <key> <score> <member> [score member...], ZCARD <key>, ZSCORE <key> <member>, ZREM <key> <member> [member...], ZRANGE <key> <start> <stop>, ZINCRBY <key> <increment> <member>, QUIT"
    );

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(line) => line,
            Err(err) => {
                eprintln!("Failed to read input: {}", err);
                break;
            }
        };

        let command_args = parse_cli_args(&line);
        if command_args.is_empty() {
            continue;
        }

        let response = match storage.lock() {
            Ok(mut store) => execute_command(&mut store, &command_args),
            Err(_) => RespValue::Error("Error: storage lock poisoned".to_string()),
        };

        write_cli_value(&mut stdout_lock, &response)?;
        stdout_lock.flush()?;

        if command_args[0].eq_ignore_ascii_case("QUIT") {
            break;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(items: &[&str]) -> Vec<String> {
        items.iter().map(|item| (*item).to_string()).collect()
    }

    #[test]
    fn parse_options_defaults_to_cli() {
        assert_eq!(
            parse_options(&args(&["clawsredis"])).expect("default args should parse"),
            CliOptions::default()
        );
    }

    #[test]
    fn parse_options_accepts_load_and_server_in_any_order() {
        assert_eq!(
            parse_options(&args(&["clawsredis", "--load", "dump.bin", "--server", "127.0.0.1:6379"]))
                .expect("load then server should parse"),
            CliOptions {
                load_path: Some("dump.bin".to_string()),
                server_addr: Some("127.0.0.1:6379".to_string()),
            }
        );
        assert_eq!(
            parse_options(&args(&["clawsredis", "--server", "127.0.0.1:6379", "--load", "dump.bin"]))
                .expect("server then load should parse"),
            CliOptions {
                load_path: Some("dump.bin".to_string()),
                server_addr: Some("127.0.0.1:6379".to_string()),
            }
        );
    }

    #[test]
    fn parse_options_rejects_missing_values_and_unknown_flags() {
        assert!(parse_options(&args(&["clawsredis", "--server"])).is_err());
        assert!(parse_options(&args(&["clawsredis", "--load"])).is_err());
        assert!(parse_options(&args(&["clawsredis", "--bad"])).is_err());
    }
}
