use std::env;
use std::io::{self, BufRead, Write};
use std::sync::{Arc, Mutex};

use test001::KVStore;

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
            Ok(l) => l,
            Err(_) => break,
        };

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let parts: Vec<&str> = trimmed.splitn(2, ' ').collect();
        let command = parts[0].to_uppercase();

        match command.as_str() {
            "GET" => {
                if parts.len() < 2 || parts[1].is_empty() {
                    writeln!(stdout_lock, "Error: GET requires a key").unwrap();
                    continue;
                }
                let key = parts[1].trim();
                let mut storage = storage.lock().unwrap();
                match storage.get(key) {
                    Some(value) => writeln!(stdout_lock, "{}", value).unwrap(),
                    None => writeln!(stdout_lock, "(nil)").unwrap(),
                }
            }
            "SET" => {
                if parts.len() < 2 {
                    writeln!(stdout_lock, "Error: SET requires <key> <value> [TTL]").unwrap();
                    continue;
                }
                let tokens: Vec<&str> = parts[1].split_whitespace().collect();
                if tokens.len() < 2 {
                    writeln!(stdout_lock, "Error: SET requires <key> <value> [TTL]").unwrap();
                    continue;
                }
                let key = tokens[0];
                let value = tokens[1];
                let ttl = if tokens.len() >= 3 {
                    match tokens[2].parse::<u64>() {
                        Ok(v) => Some(v),
                        Err(_) => {
                            writeln!(stdout_lock, "Error: Invalid TTL value").unwrap();
                            continue;
                        }
                    }
                } else {
                    None
                };

                let mut storage = storage.lock().unwrap();
                storage.set_with_ttl(key.to_string(), value.to_string(), ttl);
                if storage.maybe_persist().is_err() {
                    writeln!(stdout_lock, "Warning: Persist failed").unwrap();
                }
                writeln!(stdout_lock, "OK").unwrap();
            }
            "SETNX" => {
                if parts.len() < 2 {
                    writeln!(stdout_lock, "Error: SETNX requires <key> <value>").unwrap();
                    continue;
                }
                let tokens: Vec<&str> = parts[1].split_whitespace().collect();
                if tokens.len() != 2 {
                    writeln!(stdout_lock, "Error: SETNX requires <key> <value>").unwrap();
                    continue;
                }
                let mut storage = storage.lock().unwrap();
                let inserted = storage.setnx(tokens[0].to_string(), tokens[1].to_string());
                if inserted && storage.maybe_persist().is_err() {
                    writeln!(stdout_lock, "Warning: Persist failed").unwrap();
                }
                writeln!(stdout_lock, "(integer) {}", if inserted { 1 } else { 0 }).unwrap();
            }
            "SETEX" => {
                if parts.len() < 2 {
                    writeln!(stdout_lock, "Error: SETEX requires <key> <seconds> <value>").unwrap();
                    continue;
                }
                let tokens: Vec<&str> = parts[1].split_whitespace().collect();
                if tokens.len() != 3 {
                    writeln!(stdout_lock, "Error: SETEX requires <key> <seconds> <value>").unwrap();
                    continue;
                }
                let ttl: u64 = match tokens[1].parse() {
                    Ok(v) => v,
                    Err(_) => {
                        writeln!(stdout_lock, "Error: Invalid TTL value").unwrap();
                        continue;
                    }
                };
                let mut storage = storage.lock().unwrap();
                storage.setex(tokens[0].to_string(), tokens[2].to_string(), ttl);
                if storage.maybe_persist().is_err() {
                    writeln!(stdout_lock, "Warning: Persist failed").unwrap();
                }
                writeln!(stdout_lock, "OK").unwrap();
            }
            "MSET" | "MSETNX" => {
                if parts.len() < 2 {
                    writeln!(
                        stdout_lock,
                        "Error: {} requires <k1> <v1> [k2 v2...]",
                        command
                    )
                    .unwrap();
                    continue;
                }
                let tokens: Vec<&str> = parts[1].split_whitespace().collect();
                if tokens.len() < 2 || tokens.len() % 2 != 0 {
                    writeln!(
                        stdout_lock,
                        "Error: {} requires even number of key/value args",
                        command
                    )
                    .unwrap();
                    continue;
                }
                let mut kvs: Vec<(String, String)> = Vec::new();
                let mut i = 0;
                while i < tokens.len() {
                    kvs.push((tokens[i].to_string(), tokens[i + 1].to_string()));
                    i += 2;
                }
                let mut storage = storage.lock().unwrap();
                if command == "MSET" {
                    storage.mset(&kvs);
                    if storage.maybe_persist().is_err() {
                        writeln!(stdout_lock, "Warning: Persist failed").unwrap();
                    }
                    writeln!(stdout_lock, "OK").unwrap();
                } else {
                    let inserted = storage.msetnx(&kvs);
                    if inserted && storage.maybe_persist().is_err() {
                        writeln!(stdout_lock, "Warning: Persist failed").unwrap();
                    }
                    writeln!(stdout_lock, "(integer) {}", if inserted { 1 } else { 0 }).unwrap();
                }
            }
            "MGET" => {
                if parts.len() < 2 {
                    writeln!(stdout_lock, "Error: MGET requires <k1> [k2...]").unwrap();
                    continue;
                }
                let keys: Vec<String> =
                    parts[1].split_whitespace().map(|s| s.to_string()).collect();
                if keys.is_empty() {
                    writeln!(stdout_lock, "Error: MGET requires <k1> [k2...]").unwrap();
                    continue;
                }

                let mut storage = storage.lock().unwrap();
                let values = storage.mget(&keys);
                for value in values {
                    match value {
                        Some(v) => writeln!(stdout_lock, "- {}", v).unwrap(),
                        None => writeln!(stdout_lock, "(nil)").unwrap(),
                    }
                }
            }
            "APPEND" => {
                if parts.len() < 2 {
                    writeln!(stdout_lock, "Error: APPEND requires <key> <value>").unwrap();
                    continue;
                }
                let tokens: Vec<&str> = parts[1].split_whitespace().collect();
                if tokens.len() != 2 {
                    writeln!(stdout_lock, "Error: APPEND requires <key> <value>").unwrap();
                    continue;
                }
                let mut storage = storage.lock().unwrap();
                if storage.is_non_string_key(tokens[0]) {
                    writeln!(
                        stdout_lock,
                        "Error: WRONGTYPE Operation against a key holding the wrong kind of value"
                    )
                    .unwrap();
                    continue;
                }
                let len = storage.append(tokens[0].to_string(), tokens[1].to_string());
                if storage.maybe_persist().is_err() {
                    writeln!(stdout_lock, "Warning: Persist failed").unwrap();
                }
                writeln!(stdout_lock, "(integer) {}", len).unwrap();
            }
            "STRLEN" => {
                if parts.len() < 2 || parts[1].is_empty() {
                    writeln!(stdout_lock, "Error: STRLEN requires a key").unwrap();
                    continue;
                }
                let key = parts[1].trim();
                let mut storage = storage.lock().unwrap();
                if storage.is_non_string_key(key) {
                    writeln!(
                        stdout_lock,
                        "Error: WRONGTYPE Operation against a key holding the wrong kind of value"
                    )
                    .unwrap();
                    continue;
                }
                writeln!(stdout_lock, "(integer) {}", storage.strlen(key)).unwrap();
            }
            "GETSET" => {
                if parts.len() < 2 {
                    writeln!(stdout_lock, "Error: GETSET requires <key> <value>").unwrap();
                    continue;
                }
                let tokens: Vec<&str> = parts[1].split_whitespace().collect();
                if tokens.len() != 2 {
                    writeln!(stdout_lock, "Error: GETSET requires <key> <value>").unwrap();
                    continue;
                }
                let mut storage = storage.lock().unwrap();
                if storage.is_non_string_key(tokens[0]) {
                    writeln!(
                        stdout_lock,
                        "Error: WRONGTYPE Operation against a key holding the wrong kind of value"
                    )
                    .unwrap();
                    continue;
                }
                match storage.getset(tokens[0].to_string(), tokens[1].to_string()) {
                    Some(v) => writeln!(stdout_lock, "{}", v).unwrap(),
                    None => writeln!(stdout_lock, "(nil)").unwrap(),
                }
                if storage.maybe_persist().is_err() {
                    writeln!(stdout_lock, "Warning: Persist failed").unwrap();
                }
            }
            "INCR" | "DECR" | "INCRBY" | "DECRBY" => {
                if parts.len() < 2 {
                    writeln!(stdout_lock, "Error: {} requires arguments", command).unwrap();
                    continue;
                }
                let tokens: Vec<&str> = parts[1].split_whitespace().collect();
                let (key, delta): (&str, i64) = match command.as_str() {
                    "INCR" | "DECR" => {
                        if tokens.len() != 1 {
                            writeln!(stdout_lock, "Error: {} requires <key>", command).unwrap();
                            continue;
                        }
                        (tokens[0], if command == "INCR" { 1 } else { -1 })
                    }
                    "INCRBY" | "DECRBY" => {
                        if tokens.len() != 2 {
                            writeln!(stdout_lock, "Error: {} requires <key> <n>", command).unwrap();
                            continue;
                        }
                        let n: i64 = match tokens[1].parse() {
                            Ok(v) => v,
                            Err(_) => {
                                writeln!(
                                    stdout_lock,
                                    "Error: {} value must be an integer",
                                    command
                                )
                                .unwrap();
                                continue;
                            }
                        };
                        (tokens[0], if command == "INCRBY" { n } else { -n })
                    }
                    _ => unreachable!(),
                };

                let mut storage = storage.lock().unwrap();
                if storage.is_non_string_key(key) {
                    writeln!(
                        stdout_lock,
                        "Error: WRONGTYPE Operation against a key holding the wrong kind of value"
                    )
                    .unwrap();
                    continue;
                }
                match storage.incrby(key.to_string(), delta) {
                    Ok(v) => {
                        if storage.maybe_persist().is_err() {
                            writeln!(stdout_lock, "Warning: Persist failed").unwrap();
                        }
                        writeln!(stdout_lock, "(integer) {}", v).unwrap();
                    }
                    Err(msg) => writeln!(stdout_lock, "{}", msg).unwrap(),
                }
            }
            "DEL" => {
                if parts.len() < 2 || parts[1].is_empty() {
                    writeln!(stdout_lock, "Error: DEL requires a key").unwrap();
                    continue;
                }
                let key = parts[1].trim();
                let mut storage = storage.lock().unwrap();
                let deleted = storage.del(key);
                if storage.maybe_persist().is_err() {
                    writeln!(stdout_lock, "Warning: Persist failed").unwrap();
                }
                writeln!(
                    stdout_lock,
                    "{}",
                    if deleted {
                        "(integer) 1"
                    } else {
                        "(integer) 0"
                    }
                )
                .unwrap();
            }
            "KEYS" => {
                let mut storage = storage.lock().unwrap();
                let keys = storage.keys();
                if keys.is_empty() {
                    writeln!(stdout_lock, "(empty array)").unwrap();
                } else {
                    for key in keys {
                        writeln!(stdout_lock, "- {}", key).unwrap();
                    }
                }
            }
            "SUBSCRIBE" => {
                if parts.len() < 2 || parts[1].is_empty() {
                    writeln!(stdout_lock, "Error: SUBSCRIBE requires a channel").unwrap();
                    continue;
                }
                let channel = parts[1].trim();
                let mut storage = storage.lock().unwrap();
                let sub_id = storage.subscribe(channel.to_string());
                writeln!(stdout_lock, "Subscribed to {} with ID {}", channel, sub_id).unwrap();
            }
            "PUBLISH" => {
                if parts.len() < 2 {
                    writeln!(stdout_lock, "Error: PUBLISH requires <channel> <message>").unwrap();
                    continue;
                }
                let rest = parts[1].trim();
                let parts2: Vec<&str> = rest.splitn(2, ' ').collect();
                if parts2.len() < 2 {
                    writeln!(stdout_lock, "Error: PUBLISH requires <channel> <message>").unwrap();
                    continue;
                }
                let channel = parts2[0].trim();
                let message = parts2[1].trim();
                let mut storage = storage.lock().unwrap();
                let count = storage.publish(channel, message.to_string());
                writeln!(
                    stdout_lock,
                    "Published to {} ({} subscribers)",
                    channel, count
                )
                .unwrap();
            }
            "UNSUBSCRIBE" => {
                if parts.len() < 2 || parts[1].is_empty() {
                    writeln!(
                        stdout_lock,
                        "Error: UNSUBSCRIBE requires <channel> <sub_id>"
                    )
                    .unwrap();
                    continue;
                }
                let rest = parts[1].trim();
                let parts2: Vec<&str> = rest.splitn(2, ' ').collect();
                if parts2.len() < 2 {
                    writeln!(
                        stdout_lock,
                        "Error: UNSUBSCRIBE requires <channel> <sub_id>"
                    )
                    .unwrap();
                    continue;
                }
                let channel = parts2[0].trim();
                let sub_id: usize = match parts2[1].trim().parse() {
                    Ok(id) => id,
                    Err(_) => {
                        writeln!(stdout_lock, "Error: Invalid subscription ID").unwrap();
                        continue;
                    }
                };
                let mut storage = storage.lock().unwrap();
                storage.unsubscribe(channel, sub_id);
                writeln!(stdout_lock, "Unsubscribed from {} (ID {})", channel, sub_id).unwrap();
            }
            "TTL" => {
                if parts.len() < 2 || parts[1].is_empty() {
                    writeln!(stdout_lock, "Error: TTL requires a key").unwrap();
                    continue;
                }
                let key = parts[1].trim();
                let mut storage = storage.lock().unwrap();
                match storage.ttl(key) {
                    Some(-1) => writeln!(stdout_lock, "(integer) -1").unwrap(),
                    Some(t) => writeln!(stdout_lock, "(integer) {}", t).unwrap(),
                    None => writeln!(stdout_lock, "(nil)").unwrap(),
                }
            }
            "EXPIRE" => {
                if parts.len() < 2 {
                    writeln!(stdout_lock, "Error: EXPIRE requires <key> <seconds>").unwrap();
                    continue;
                }
                let rest = parts[1].trim();
                let parts2: Vec<&str> = rest.splitn(2, ' ').collect();
                if parts2.len() < 2 {
                    writeln!(stdout_lock, "Error: EXPIRE requires <key> <seconds>").unwrap();
                    continue;
                }
                let key = parts2[0].trim();
                let ttl: u64 = match parts2[1].trim().parse() {
                    Ok(t) => t,
                    Err(_) => {
                        writeln!(stdout_lock, "Error: Invalid TTL value").unwrap();
                        continue;
                    }
                };
                let mut storage = storage.lock().unwrap();
                if storage.expire(key, ttl) {
                    if storage.maybe_persist().is_err() {
                        writeln!(stdout_lock, "Warning: Persist failed").unwrap();
                    }
                    writeln!(stdout_lock, "OK").unwrap();
                } else {
                    writeln!(stdout_lock, "(integer) 0").unwrap();
                }
            }
            "PERSISTKEY" => {
                if parts.len() < 2 || parts[1].is_empty() {
                    writeln!(stdout_lock, "Error: PERSISTKEY requires a key").unwrap();
                    continue;
                }
                let key = parts[1].trim();
                let mut storage = storage.lock().unwrap();
                if storage.persist_key(key) {
                    if storage.maybe_persist().is_err() {
                        writeln!(stdout_lock, "Warning: Persist failed").unwrap();
                    }
                    writeln!(stdout_lock, "OK").unwrap();
                } else {
                    writeln!(stdout_lock, "(integer) 0").unwrap();
                }
            }
            "PERSIST" => {
                if parts.len() < 2 || parts[1].is_empty() {
                    writeln!(stdout_lock, "Error: PERSIST requires <path>").unwrap();
                    continue;
                }
                let path = parts[1].trim();
                let mut storage = storage.lock().unwrap();
                storage.set_persist_path(path.to_string());
                if storage.maybe_persist().is_err() {
                    writeln!(stdout_lock, "Error: Failed to persist").unwrap();
                } else {
                    writeln!(stdout_lock, "Persistence enabled: {}", path).unwrap();
                }
            }
            "RPUSH" | "LPUSH" => {
                if parts.len() < 2 {
                    writeln!(
                        stdout_lock,
                        "Error: {} requires <key> <v1> [v2...]",
                        command
                    )
                    .unwrap();
                    continue;
                }
                let tokens: Vec<&str> = parts[1].split_whitespace().collect();
                if tokens.len() < 2 {
                    writeln!(
                        stdout_lock,
                        "Error: {} requires <key> <v1> [v2...]",
                        command
                    )
                    .unwrap();
                    continue;
                }
                let key = tokens[0];
                let values: Vec<String> = tokens[1..].iter().map(|s| (*s).to_string()).collect();
                let mut storage = storage.lock().unwrap();
                let len = if command == "RPUSH" {
                    storage.rpush(key, &values)
                } else {
                    storage.lpush(key, &values)
                };
                if storage.maybe_persist().is_err() {
                    writeln!(stdout_lock, "Warning: Persist failed").unwrap();
                }
                writeln!(stdout_lock, "(integer) {}", len).unwrap();
            }
            "LRANGE" => {
                if parts.len() < 2 {
                    writeln!(stdout_lock, "Error: LRANGE requires <key> <start> <stop>").unwrap();
                    continue;
                }
                let tokens: Vec<&str> = parts[1].split_whitespace().collect();
                if tokens.len() != 3 {
                    writeln!(stdout_lock, "Error: LRANGE requires <key> <start> <stop>").unwrap();
                    continue;
                }
                let key = tokens[0];
                let start: isize = match tokens[1].parse() {
                    Ok(v) => v,
                    Err(_) => {
                        writeln!(stdout_lock, "Error: LRANGE start must be an integer").unwrap();
                        continue;
                    }
                };
                let stop: isize = match tokens[2].parse() {
                    Ok(v) => v,
                    Err(_) => {
                        writeln!(stdout_lock, "Error: LRANGE stop must be an integer").unwrap();
                        continue;
                    }
                };
                let mut storage = storage.lock().unwrap();
                match storage.lrange(key, start, stop) {
                    Some(values) if values.is_empty() => {
                        writeln!(stdout_lock, "(empty array)").unwrap()
                    }
                    Some(values) => {
                        for value in values {
                            writeln!(stdout_lock, "- {}", value).unwrap();
                        }
                    }
                    None => writeln!(stdout_lock, "(nil)").unwrap(),
                }
            }
            "LLEN" => {
                if parts.len() < 2 || parts[1].is_empty() {
                    writeln!(stdout_lock, "Error: LLEN requires a key").unwrap();
                    continue;
                }
                let key = parts[1].trim();
                let mut storage = storage.lock().unwrap();
                match storage.llen(key) {
                    Some(len) => writeln!(stdout_lock, "(integer) {}", len).unwrap(),
                    None => writeln!(stdout_lock, "(integer) 0").unwrap(),
                }
            }
            "HSET" | "HMSET" => {
                if parts.len() < 2 {
                    writeln!(
                        stdout_lock,
                        "Error: {} requires <key> <field> <value> [field value...]",
                        command
                    )
                    .unwrap();
                    continue;
                }
                let tokens: Vec<&str> = parts[1].split_whitespace().collect();
                if tokens.len() < 3 || tokens.len() % 2 == 0 {
                    writeln!(
                        stdout_lock,
                        "Error: {} requires <key> <field> <value> [field value...]",
                        command
                    )
                    .unwrap();
                    continue;
                }
                let key = tokens[0];
                let mut items: Vec<(String, String)> = Vec::new();
                let mut i = 1;
                while i < tokens.len() {
                    items.push((tokens[i].to_string(), tokens[i + 1].to_string()));
                    i += 2;
                }
                let mut storage = storage.lock().unwrap();
                let added = storage.hset(key, &items);
                if storage.maybe_persist().is_err() {
                    writeln!(stdout_lock, "Warning: Persist failed").unwrap();
                }
                if command == "HSET" {
                    writeln!(stdout_lock, "(integer) {}", added).unwrap();
                } else {
                    writeln!(stdout_lock, "OK").unwrap();
                }
            }
            "HGET" => {
                if parts.len() < 2 {
                    writeln!(stdout_lock, "Error: HGET requires <key> <field>").unwrap();
                    continue;
                }
                let tokens: Vec<&str> = parts[1].split_whitespace().collect();
                if tokens.len() != 2 {
                    writeln!(stdout_lock, "Error: HGET requires <key> <field>").unwrap();
                    continue;
                }
                let mut storage = storage.lock().unwrap();
                match storage.hget(tokens[0], tokens[1]) {
                    Some(value) => writeln!(stdout_lock, "{}", value).unwrap(),
                    None => writeln!(stdout_lock, "(nil)").unwrap(),
                }
            }
            "HMGET" => {
                if parts.len() < 2 {
                    writeln!(
                        stdout_lock,
                        "Error: HMGET requires <key> <field> [field...]"
                    )
                    .unwrap();
                    continue;
                }
                let tokens: Vec<&str> = parts[1].split_whitespace().collect();
                if tokens.len() < 2 {
                    writeln!(
                        stdout_lock,
                        "Error: HMGET requires <key> <field> [field...]"
                    )
                    .unwrap();
                    continue;
                }
                let key = tokens[0];
                let fields: Vec<String> = tokens[1..].iter().map(|s| (*s).to_string()).collect();
                let mut storage = storage.lock().unwrap();
                let values = storage.hmget(key, &fields);
                for value in values {
                    match value {
                        Some(v) => writeln!(stdout_lock, "- {}", v).unwrap(),
                        None => writeln!(stdout_lock, "(nil)").unwrap(),
                    }
                }
            }
            "HDEL" => {
                if parts.len() < 2 {
                    writeln!(stdout_lock, "Error: HDEL requires <key> <field> [field...]").unwrap();
                    continue;
                }
                let tokens: Vec<&str> = parts[1].split_whitespace().collect();
                if tokens.len() < 2 {
                    writeln!(stdout_lock, "Error: HDEL requires <key> <field> [field...]").unwrap();
                    continue;
                }
                let key = tokens[0];
                let fields: Vec<String> = tokens[1..].iter().map(|s| (*s).to_string()).collect();
                let mut storage = storage.lock().unwrap();
                let removed = storage.hdel(key, &fields);
                if removed > 0 && storage.maybe_persist().is_err() {
                    writeln!(stdout_lock, "Warning: Persist failed").unwrap();
                }
                writeln!(stdout_lock, "(integer) {}", removed).unwrap();
            }
            "HEXISTS" => {
                if parts.len() < 2 {
                    writeln!(stdout_lock, "Error: HEXISTS requires <key> <field>").unwrap();
                    continue;
                }
                let tokens: Vec<&str> = parts[1].split_whitespace().collect();
                if tokens.len() != 2 {
                    writeln!(stdout_lock, "Error: HEXISTS requires <key> <field>").unwrap();
                    continue;
                }
                let mut storage = storage.lock().unwrap();
                writeln!(
                    stdout_lock,
                    "(integer) {}",
                    if storage.hexists(tokens[0], tokens[1]) {
                        1
                    } else {
                        0
                    }
                )
                .unwrap();
            }
            "HLEN" => {
                if parts.len() < 2 || parts[1].is_empty() {
                    writeln!(stdout_lock, "Error: HLEN requires a key").unwrap();
                    continue;
                }
                let key = parts[1].trim();
                let mut storage = storage.lock().unwrap();
                writeln!(stdout_lock, "(integer) {}", storage.hlen(key)).unwrap();
            }
            "HKEYS" => {
                if parts.len() < 2 || parts[1].is_empty() {
                    writeln!(stdout_lock, "Error: HKEYS requires a key").unwrap();
                    continue;
                }
                let key = parts[1].trim();
                let mut storage = storage.lock().unwrap();
                let fields = storage.hkeys(key);
                if fields.is_empty() {
                    writeln!(stdout_lock, "(empty array)").unwrap();
                } else {
                    for field in fields {
                        writeln!(stdout_lock, "- {}", field).unwrap();
                    }
                }
            }
            "HVALS" => {
                if parts.len() < 2 || parts[1].is_empty() {
                    writeln!(stdout_lock, "Error: HVALS requires a key").unwrap();
                    continue;
                }
                let key = parts[1].trim();
                let mut storage = storage.lock().unwrap();
                let values = storage.hvals(key);
                if values.is_empty() {
                    writeln!(stdout_lock, "(empty array)").unwrap();
                } else {
                    for value in values {
                        writeln!(stdout_lock, "- {}", value).unwrap();
                    }
                }
            }
            "HGETALL" => {
                if parts.len() < 2 || parts[1].is_empty() {
                    writeln!(stdout_lock, "Error: HGETALL requires a key").unwrap();
                    continue;
                }
                let key = parts[1].trim();
                let mut storage = storage.lock().unwrap();
                let entries = storage.hgetall(key);
                if entries.is_empty() {
                    writeln!(stdout_lock, "(empty array)").unwrap();
                } else {
                    for (field, value) in entries {
                        writeln!(stdout_lock, "- {}", field).unwrap();
                        writeln!(stdout_lock, "- {}", value).unwrap();
                    }
                }
            }
            "ZADD" => {
                if parts.len() < 2 {
                    writeln!(stdout_lock, "Error: ZADD requires <key> <score> <member> [score member...]").unwrap();
                    continue;
                }
                let tokens: Vec<&str> = parts[1].split_whitespace().collect();
                if tokens.len() < 3 || tokens.len() % 2 == 0 {
                    writeln!(stdout_lock, "Error: ZADD requires <key> <score> <member> [score member...]").unwrap();
                    continue;
                }
                let key = tokens[0];
                let mut entries: Vec<(f64, String)> = Vec::new();
                let mut parse_error = false;
                let mut i = 1;
                while i < tokens.len() {
                    let score = match tokens[i].parse::<f64>() {
                        Ok(v) => v,
                        Err(_) => {
                            writeln!(stdout_lock, "Error: ZADD score must be a number").unwrap();
                            parse_error = true;
                            break;
                        }
                    };
                    entries.push((score, tokens[i + 1].to_string()));
                    i += 2;
                }
                if parse_error {
                    continue;
                }
                let mut storage = storage.lock().unwrap();
                if storage.is_non_zset_key(key) {
                    writeln!(stdout_lock, "Error: WRONGTYPE Operation against a key holding the wrong kind of value").unwrap();
                    continue;
                }
                let added = storage.zadd(key, &entries);
                if storage.maybe_persist().is_err() {
                    writeln!(stdout_lock, "Warning: Persist failed").unwrap();
                }
                writeln!(stdout_lock, "(integer) {}", added).unwrap();
            }
            "ZCARD" => {
                if parts.len() < 2 || parts[1].is_empty() {
                    writeln!(stdout_lock, "Error: ZCARD requires a key").unwrap();
                    continue;
                }
                let key = parts[1].trim();
                let mut storage = storage.lock().unwrap();
                if storage.is_non_zset_key(key) {
                    writeln!(stdout_lock, "Error: WRONGTYPE Operation against a key holding the wrong kind of value").unwrap();
                    continue;
                }
                writeln!(stdout_lock, "(integer) {}", storage.zcard(key)).unwrap();
            }
            "ZSCORE" => {
                if parts.len() < 2 {
                    writeln!(stdout_lock, "Error: ZSCORE requires <key> <member>").unwrap();
                    continue;
                }
                let tokens: Vec<&str> = parts[1].split_whitespace().collect();
                if tokens.len() != 2 {
                    writeln!(stdout_lock, "Error: ZSCORE requires <key> <member>").unwrap();
                    continue;
                }
                let mut storage = storage.lock().unwrap();
                if storage.is_non_zset_key(tokens[0]) {
                    writeln!(stdout_lock, "Error: WRONGTYPE Operation against a key holding the wrong kind of value").unwrap();
                    continue;
                }
                match storage.zscore(tokens[0], tokens[1]) {
                    Some(score) => writeln!(stdout_lock, "{}", score).unwrap(),
                    None => writeln!(stdout_lock, "(nil)").unwrap(),
                }
            }
            "ZREM" => {
                if parts.len() < 2 {
                    writeln!(stdout_lock, "Error: ZREM requires <key> <member> [member...]").unwrap();
                    continue;
                }
                let tokens: Vec<&str> = parts[1].split_whitespace().collect();
                if tokens.len() < 2 {
                    writeln!(stdout_lock, "Error: ZREM requires <key> <member> [member...]").unwrap();
                    continue;
                }
                let key = tokens[0];
                let members: Vec<String> = tokens[1..].iter().map(|s| (*s).to_string()).collect();
                let mut storage = storage.lock().unwrap();
                if storage.is_non_zset_key(key) {
                    writeln!(stdout_lock, "Error: WRONGTYPE Operation against a key holding the wrong kind of value").unwrap();
                    continue;
                }
                let removed = storage.zrem(key, &members);
                if removed > 0 && storage.maybe_persist().is_err() {
                    writeln!(stdout_lock, "Warning: Persist failed").unwrap();
                }
                writeln!(stdout_lock, "(integer) {}", removed).unwrap();
            }
            "ZRANGE" => {
                if parts.len() < 2 {
                    writeln!(stdout_lock, "Error: ZRANGE requires <key> <start> <stop>").unwrap();
                    continue;
                }
                let tokens: Vec<&str> = parts[1].split_whitespace().collect();
                if tokens.len() != 3 {
                    writeln!(stdout_lock, "Error: ZRANGE requires <key> <start> <stop>").unwrap();
                    continue;
                }
                let start: isize = match tokens[1].parse() {
                    Ok(v) => v,
                    Err(_) => {
                        writeln!(stdout_lock, "Error: ZRANGE start must be an integer").unwrap();
                        continue;
                    }
                };
                let stop: isize = match tokens[2].parse() {
                    Ok(v) => v,
                    Err(_) => {
                        writeln!(stdout_lock, "Error: ZRANGE stop must be an integer").unwrap();
                        continue;
                    }
                };
                let mut storage = storage.lock().unwrap();
                if storage.is_non_zset_key(tokens[0]) {
                    writeln!(stdout_lock, "Error: WRONGTYPE Operation against a key holding the wrong kind of value").unwrap();
                    continue;
                }
                match storage.zrange(tokens[0], start, stop) {
                    Some(values) if values.is_empty() => writeln!(stdout_lock, "(empty array)").unwrap(),
                    Some(values) => {
                        for value in values {
                            writeln!(stdout_lock, "- {}", value).unwrap();
                        }
                    }
                    None => writeln!(stdout_lock, "(nil)").unwrap(),
                }
            }
            "ZINCRBY" => {
                if parts.len() < 2 {
                    writeln!(stdout_lock, "Error: ZINCRBY requires <key> <increment> <member>").unwrap();
                    continue;
                }
                let tokens: Vec<&str> = parts[1].split_whitespace().collect();
                if tokens.len() != 3 {
                    writeln!(stdout_lock, "Error: ZINCRBY requires <key> <increment> <member>").unwrap();
                    continue;
                }
                let increment: f64 = match tokens[1].parse() {
                    Ok(v) => v,
                    Err(_) => {
                        writeln!(stdout_lock, "Error: ZINCRBY increment must be a number").unwrap();
                        continue;
                    }
                };
                let mut storage = storage.lock().unwrap();
                if storage.is_non_zset_key(tokens[0]) {
                    writeln!(stdout_lock, "Error: WRONGTYPE Operation against a key holding the wrong kind of value").unwrap();
                    continue;
                }
                let score = storage.zincrby(tokens[0], increment, tokens[2].to_string());
                if storage.maybe_persist().is_err() {
                    writeln!(stdout_lock, "Warning: Persist failed").unwrap();
                }
                writeln!(stdout_lock, "{}", score).unwrap();
            }
            "QUIT" => {
                writeln!(stdout_lock, "Goodbye!").unwrap();
                break;
            }
            _ => {
                writeln!(stdout_lock, "Unknown command: {}", command).unwrap();
            }
        }
        stdout_lock.flush().unwrap();
    }
}
