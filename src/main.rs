use std::env;
use std::io::{self, BufRead, Write};
use std::sync::{Arc, Mutex};

use test001::{execute_command, KVStore, RespValue};

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

fn main() {
    let args: Vec<String> = env::args().collect();
    let storage: Arc<Mutex<KVStore>>;

    if args.len() > 1 && args[1] == "--load" {
        if args.len() < 3 {
            eprintln!("Usage: {} --load <path>", args[0]);
            std::process::exit(1);
        }
        match KVStore::load(&args[2]) {
            Ok(store) => {
                storage = Arc::new(Mutex::new(store));
                println!("Loaded state from {}", &args[2]);
            }
            Err(e) => {
                eprintln!("Failed to load state: {}", e);
                storage = Arc::new(Mutex::new(KVStore::new()));
            }
        }
    } else {
        storage = Arc::new(Mutex::new(KVStore::new()));
    }

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

        if let Err(err) = write_cli_value(&mut stdout_lock, &response) {
            eprintln!("Failed to write output: {}", err);
            break;
        }
        if let Err(err) = stdout_lock.flush() {
            eprintln!("Failed to flush output: {}", err);
            break;
        }

        if command_args[0].eq_ignore_ascii_case("QUIT") {
            break;
        }
    }
}
