use bincode::error::{DecodeError, EncodeError};
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use std::env;
use std::io::{self, BufRead, Read, Write};
use std::sync::{Arc, Mutex};

type Storage = HashMap<String, String>;
type ListStorage = HashMap<String, Vec<String>>;
type HashStorage = HashMap<String, HashMap<String, String>>;

#[derive(Serialize, Deserialize, Default, Encode, Decode)]
struct KVStore {
    data: Storage,
    lists: ListStorage,
    hashes: HashStorage,
    /// Expiration times per key: key -> absolute timestamp (seconds since epoch)
    #[serde(rename = "exp")]
    expiration: HashMap<String, u64>,
    subscribers: HashMap<String, BTreeSet<usize>>,
    next_sub_id: usize,
    persist_path: Option<String>,
}

impl KVStore {
    fn new() -> Self {
        Self {
            data: HashMap::new(),
            lists: HashMap::new(),
            hashes: HashMap::new(),
            expiration: HashMap::new(),
            subscribers: HashMap::new(),
            next_sub_id: 1,
            persist_path: None,
        }
    }

    fn now_seconds() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is before UNIX_EPOCH")
            .as_secs()
    }

    fn set_with_ttl(&mut self, key: String, value: String, ttl: Option<u64>) {
        self.lists.remove(&key);
        self.hashes.remove(&key);
        self.data.insert(key.clone(), value);
        if let Some(ttl_seconds) = ttl {
            let expiration = Self::now_seconds() + ttl_seconds;
            self.expiration.insert(key, expiration);
        } else {
            self.expiration.remove(&key);
        }
    }

    fn evict_if_expired(&mut self, key: &str) -> bool {
        if let Some(expiration) = self.expiration.get(key) {
            let now = Self::now_seconds();
            if now >= *expiration {
                self.data.remove(key);
                self.lists.remove(key);
                self.hashes.remove(key);
                self.expiration.remove(key);
                return true;
            }
        }
        false
    }

    fn get(&mut self, key: &str) -> Option<&String> {
        if self.evict_if_expired(key) {
            return None;
        }
        self.data.get(key)
    }

    fn del(&mut self, key: &str) -> bool {
        self.expiration.remove(key);
        self.data.remove(key).is_some()
            || self.lists.remove(key).is_some()
            || self.hashes.remove(key).is_some()
    }

    fn keys(&mut self) -> Vec<String> {
        let all_keys: Vec<String> = self
            .data
            .keys()
            .chain(self.lists.keys())
            .chain(self.hashes.keys())
            .cloned()
            .collect();
        for key in &all_keys {
            self.get(key);
        }

        let mut keys: Vec<String> = self
            .data
            .keys()
            .chain(self.lists.keys())
            .chain(self.hashes.keys())
            .cloned()
            .collect();
        keys.sort();
        keys.dedup();
        keys
    }

    fn subscribe(&mut self, channel: String) -> usize {
        let id = self.next_sub_id;
        self.next_sub_id += 1;
        self.subscribers.entry(channel).or_default().insert(id);
        id
    }

    fn unsubscribe(&mut self, channel: &str, sub_id: usize) {
        if let Some(subs) = self.subscribers.get_mut(channel) {
            subs.remove(&sub_id);
            if subs.is_empty() {
                self.subscribers.remove(channel);
            }
        }
    }

    fn publish(&mut self, channel: &str, message: String) -> usize {
        let count = self.subscribers.get(channel).map(|s| s.len()).unwrap_or(0);
        if !message.is_empty() {
            self.data.insert(format!("__pubsub__:{}", channel), message);
        }
        count
    }

    fn set_persist_path(&mut self, path: String) {
        self.persist_path = Some(path);
    }

    fn persist(&self) -> io::Result<()> {
        if let Some(ref path) = self.persist_path {
            let encoded = match bincode::encode_to_vec(self, bincode::config::standard()) {
                Ok(e) => e,
                Err(EncodeError::Io { inner, .. }) => return Err(inner),
                Err(err) => return Err(io::Error::other(err.to_string())),
            };
            std::fs::write(path, encoded)?;
        }
        Ok(())
    }

    fn load(path: &str) -> io::Result<Self> {
        let mut file = std::fs::File::open(path)?;
        let mut contents = Vec::new();
        file.read_to_end(&mut contents)?;
        let (mut store, _): (KVStore, _) =
            match bincode::decode_from_slice(&contents, bincode::config::standard()) {
                Ok(s) => s,
                Err(DecodeError::Io { inner, .. }) => return Err(inner),
                Err(err) => return Err(io::Error::other(err.to_string())),
            };
        store.expiration.clear();
        store.persist_path = Some(path.to_string());
        Ok(store)
    }

    fn maybe_persist(&mut self) -> io::Result<()> {
        if self.persist_path.is_some() {
            self.persist()
        } else {
            Ok(())
        }
    }

    fn ttl(&self, key: &str) -> Option<i64> {
        if let Some(expiration) = self.expiration.get(key) {
            let now = Self::now_seconds();
            let remaining = *expiration as i64 - now as i64;
            if remaining > 0 {
                Some(remaining)
            } else {
                None
            }
        } else if self.data.contains_key(key)
            || self.lists.contains_key(key)
            || self.hashes.contains_key(key)
        {
            Some(-1)
        } else {
            None
        }
    }

    fn expire(&mut self, key: &str, ttl_seconds: u64) -> bool {
        if self.data.contains_key(key)
            || self.lists.contains_key(key)
            || self.hashes.contains_key(key)
        {
            let expiration = Self::now_seconds() + ttl_seconds;
            self.expiration.insert(key.to_string(), expiration);
            true
        } else {
            false
        }
    }

    fn persist_key(&mut self, key: &str) -> bool {
        if self.expiration.remove(key).is_some() {
            true
        } else {
            self.data.contains_key(key)
                || self.lists.contains_key(key)
                || self.hashes.contains_key(key)
        }
    }

    fn exists_key(&mut self, key: &str) -> bool {
        if self.evict_if_expired(key) {
            return false;
        }
        self.data.contains_key(key) || self.lists.contains_key(key) || self.hashes.contains_key(key)
    }

    fn setnx(&mut self, key: String, value: String) -> bool {
        if self.exists_key(&key) {
            false
        } else {
            self.set_with_ttl(key, value, None);
            true
        }
    }

    fn setex(&mut self, key: String, value: String, ttl_seconds: u64) {
        self.set_with_ttl(key, value, Some(ttl_seconds));
    }

    fn mget(&mut self, keys: &[String]) -> Vec<Option<String>> {
        keys.iter().map(|k| self.get(k).cloned()).collect()
    }

    fn is_non_string_key(&mut self, key: &str) -> bool {
        if self.evict_if_expired(key) {
            return false;
        }
        self.lists.contains_key(key) || self.hashes.contains_key(key)
    }

    fn append(&mut self, key: String, suffix: String) -> usize {
        if let Some(existing) = self.data.get_mut(&key) {
            existing.push_str(&suffix);
            existing.len()
        } else {
            self.set_with_ttl(key, suffix.clone(), None);
            suffix.len()
        }
    }

    fn strlen(&mut self, key: &str) -> usize {
        self.get(key).map(|v| v.len()).unwrap_or(0)
    }

    fn getset(&mut self, key: String, value: String) -> Option<String> {
        let previous = self.get(&key).cloned();
        self.set_with_ttl(key, value, None);
        previous
    }

    fn incrby(&mut self, key: String, delta: i64) -> Result<i64, &'static str> {
        let current = match self.get(&key) {
            Some(v) => v
                .parse::<i64>()
                .map_err(|_| "Error: value is not an integer")?,
            None => 0,
        };

        let next = current
            .checked_add(delta)
            .ok_or("Error: increment or decrement would overflow")?;
        self.set_with_ttl(key, next.to_string(), None);
        Ok(next)
    }

    fn mset(&mut self, items: &[(String, String)]) {
        for (k, v) in items {
            self.set_with_ttl(k.clone(), v.clone(), None);
        }
    }

    fn msetnx(&mut self, items: &[(String, String)]) -> bool {
        if items.iter().any(|(k, _)| self.exists_key(k)) {
            return false;
        }
        self.mset(items);
        true
    }

    fn rpush(&mut self, key: &str, values: &[String]) -> usize {
        self.data.remove(key);
        self.hashes.remove(key);
        let list = self.lists.entry(key.to_string()).or_default();
        list.extend(values.iter().cloned());
        list.len()
    }

    fn lpush(&mut self, key: &str, values: &[String]) -> usize {
        self.data.remove(key);
        self.hashes.remove(key);
        let list = self.lists.entry(key.to_string()).or_default();
        for value in values {
            list.insert(0, value.clone());
        }
        list.len()
    }

    fn lrange(&mut self, key: &str, start: isize, stop: isize) -> Option<Vec<String>> {
        if self.evict_if_expired(key) {
            return None;
        }

        let list = self.lists.get(key)?;
        if list.is_empty() {
            return Some(vec![]);
        }

        let len = list.len() as isize;
        let mut s = if start < 0 { len + start } else { start };
        let mut e = if stop < 0 { len + stop } else { stop };

        if s < 0 {
            s = 0;
        }
        if e < 0 || s >= len {
            return Some(vec![]);
        }
        if e >= len {
            e = len - 1;
        }
        if s > e {
            return Some(vec![]);
        }

        Some(list[s as usize..=e as usize].to_vec())
    }

    fn llen(&mut self, key: &str) -> Option<usize> {
        if self.evict_if_expired(key) {
            return None;
        }
        self.lists.get(key).map(Vec::len)
    }

    fn hset(&mut self, key: &str, items: &[(String, String)]) -> usize {
        self.data.remove(key);
        self.lists.remove(key);
        let hash = self.hashes.entry(key.to_string()).or_default();
        let mut new_fields = 0;
        for (field, value) in items {
            if hash.insert(field.clone(), value.clone()).is_none() {
                new_fields += 1;
            }
        }
        new_fields
    }

    fn hget(&mut self, key: &str, field: &str) -> Option<String> {
        if self.evict_if_expired(key) {
            return None;
        }
        self.hashes.get(key).and_then(|h| h.get(field).cloned())
    }

    fn hmget(&mut self, key: &str, fields: &[String]) -> Vec<Option<String>> {
        if self.evict_if_expired(key) {
            return fields.iter().map(|_| None).collect();
        }
        let hash = match self.hashes.get(key) {
            Some(h) => h,
            None => return fields.iter().map(|_| None).collect(),
        };
        fields.iter().map(|f| hash.get(f).cloned()).collect()
    }

    fn hdel(&mut self, key: &str, fields: &[String]) -> usize {
        if self.evict_if_expired(key) {
            return 0;
        }
        let mut removed = 0;
        let mut remove_key = false;
        if let Some(hash) = self.hashes.get_mut(key) {
            for field in fields {
                if hash.remove(field).is_some() {
                    removed += 1;
                }
            }
            if hash.is_empty() {
                remove_key = true;
            }
        }
        if remove_key {
            self.hashes.remove(key);
            self.expiration.remove(key);
        }
        removed
    }

    fn hexists(&mut self, key: &str, field: &str) -> bool {
        if self.evict_if_expired(key) {
            return false;
        }
        self.hashes.get(key).is_some_and(|h| h.contains_key(field))
    }

    fn hlen(&mut self, key: &str) -> usize {
        if self.evict_if_expired(key) {
            return 0;
        }
        self.hashes.get(key).map(|h| h.len()).unwrap_or(0)
    }

    fn hkeys(&mut self, key: &str) -> Vec<String> {
        if self.evict_if_expired(key) {
            return vec![];
        }
        let mut out: Vec<String> = self
            .hashes
            .get(key)
            .map(|h| h.keys().cloned().collect())
            .unwrap_or_default();
        out.sort();
        out
    }

    fn hvals(&mut self, key: &str) -> Vec<String> {
        if self.evict_if_expired(key) {
            return vec![];
        }
        let mut entries: Vec<(String, String)> = self
            .hashes
            .get(key)
            .map(|h| h.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        entries.into_iter().map(|(_, v)| v).collect()
    }

    fn hgetall(&mut self, key: &str) -> Vec<(String, String)> {
        if self.evict_if_expired(key) {
            return vec![];
        }
        let mut entries: Vec<(String, String)> = self
            .hashes
            .get(key)
            .map(|h| h.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        entries
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
        "Commands: GET <key>, SET <key> <value> [TTL], SETNX <key> <value>, SETEX <key> <seconds> <value>, MSET <k1> <v1> [k2 v2...], MSETNX <k1> <v1> [k2 v2...], MGET <k1> [k2...], APPEND <key> <value>, STRLEN <key>, GETSET <key> <value>, INCR <key>, DECR <key>, INCRBY <key> <n>, DECRBY <key> <n>, DEL <key>, KEYS, SUBSCRIBE <channel>, PUBLISH <channel> <message>, UNSUBSCRIBE <channel> <sub_id>, TTL <key>, EXPIRE <key> <seconds>, PERSISTKEY <key>, PERSIST <path>, RPUSH <key> <v1> [v2...], LPUSH <key> <v1> [v2...], LRANGE <key> <start> <stop>, LLEN <key>, HSET <key> <field> <value> [field value...], HMSET <key> <field> <value> [field value...], HGET <key> <field>, HMGET <key> <field> [field...], HDEL <key> <field> [field...], HEXISTS <key> <field>, HLEN <key>, HKEYS <key>, HVALS <key>, HGETALL <key>, QUIT"
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
                let storage = storage.lock().unwrap();
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lrange_out_of_bounds_start_returns_empty() {
        let mut store = KVStore::new();
        let values = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        store.rpush("mylist", &values);

        assert_eq!(store.lrange("mylist", 100, 200), Some(vec![]));
    }

    #[test]
    fn lrange_negative_stop_before_start_returns_empty() {
        let mut store = KVStore::new();
        let values = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        store.rpush("mylist", &values);

        assert_eq!(store.lrange("mylist", 0, -100), Some(vec![]));
    }

    #[test]
    fn list_reads_reflect_immediate_expiration_without_get() {
        let mut store = KVStore::new();
        let values = vec!["a".to_string(), "b".to_string()];
        store.rpush("l", &values);
        assert!(store.expire("l", 0));

        assert_eq!(store.llen("l"), None);
        assert_eq!(store.lrange("l", 0, -1), None);
    }

    #[test]
    fn setnx_only_sets_when_missing() {
        let mut store = KVStore::new();
        assert!(store.setnx("k".to_string(), "v1".to_string()));
        assert!(!store.setnx("k".to_string(), "v2".to_string()));
        assert_eq!(store.get("k"), Some(&"v1".to_string()));
    }

    #[test]
    fn setnx_does_not_overwrite_existing_hash_key() {
        let mut store = KVStore::new();
        store.hset("k", &[("f".to_string(), "v".to_string())]);

        assert!(!store.setnx("k".to_string(), "string".to_string()));
        assert_eq!(store.hget("k", "f"), Some("v".to_string()));
        assert_eq!(store.get("k"), None);
    }

    #[test]
    fn msetnx_is_atomic() {
        let mut store = KVStore::new();
        store.set_with_ttl("a".to_string(), "1".to_string(), None);
        let pairs = vec![
            ("a".to_string(), "x".to_string()),
            ("b".to_string(), "y".to_string()),
        ];
        assert!(!store.msetnx(&pairs));
        assert_eq!(store.get("a"), Some(&"1".to_string()));
        assert_eq!(store.get("b"), None);
    }

    #[test]
    fn msetnx_fails_if_any_target_key_is_a_hash() {
        let mut store = KVStore::new();
        store.hset("h", &[("f".to_string(), "1".to_string())]);

        let pairs = vec![
            ("h".to_string(), "x".to_string()),
            ("other".to_string(), "y".to_string()),
        ];

        assert!(!store.msetnx(&pairs));
        assert_eq!(store.hget("h", "f"), Some("1".to_string()));
        assert_eq!(store.get("other"), None);
    }

    #[test]
    fn setex_sets_value_with_ttl() {
        let mut store = KVStore::new();
        store.setex("k".to_string(), "v".to_string(), 2);
        assert_eq!(store.get("k"), Some(&"v".to_string()));
        assert!(store.ttl("k").map(|t| t > 0).unwrap_or(false));
    }

    #[test]
    fn mget_returns_values_and_nil_for_missing() {
        let mut store = KVStore::new();
        store.set_with_ttl("a".to_string(), "1".to_string(), None);
        store.set_with_ttl("b".to_string(), "2".to_string(), None);
        let values = store.mget(&["a".to_string(), "x".to_string(), "b".to_string()]);
        assert_eq!(
            values,
            vec![Some("1".to_string()), None, Some("2".to_string())]
        );
    }

    #[test]
    fn append_and_strlen_work() {
        let mut store = KVStore::new();
        assert_eq!(store.append("k".to_string(), "he".to_string()), 2);
        assert_eq!(store.append("k".to_string(), "llo".to_string()), 5);
        assert_eq!(store.get("k"), Some(&"hello".to_string()));
        assert_eq!(store.strlen("k"), 5);
        assert_eq!(store.strlen("missing"), 0);
    }

    #[test]
    fn getset_returns_old_and_sets_new_value() {
        let mut store = KVStore::new();
        assert_eq!(store.getset("k".to_string(), "v1".to_string()), None);
        assert_eq!(
            store.getset("k".to_string(), "v2".to_string()),
            Some("v1".to_string())
        );
        assert_eq!(store.get("k"), Some(&"v2".to_string()));
    }

    #[test]
    fn incrby_and_decrby_work_for_missing_and_existing_keys() {
        let mut store = KVStore::new();
        assert_eq!(store.incrby("n".to_string(), 1), Ok(1));
        assert_eq!(store.incrby("n".to_string(), 4), Ok(5));
        assert_eq!(store.incrby("n".to_string(), -2), Ok(3));
        assert_eq!(store.get("n"), Some(&"3".to_string()));
    }

    #[test]
    fn incrby_rejects_non_integer_string() {
        let mut store = KVStore::new();
        store.set_with_ttl("n".to_string(), "abc".to_string(), None);
        assert_eq!(
            store.incrby("n".to_string(), 1),
            Err("Error: value is not an integer")
        );
    }

    #[test]
    fn hset_and_hget_work() {
        let mut store = KVStore::new();
        let pairs = vec![
            ("f1".to_string(), "v1".to_string()),
            ("f2".to_string(), "v2".to_string()),
        ];
        assert_eq!(store.hset("h", &pairs), 2);
        assert_eq!(store.hget("h", "f1"), Some("v1".to_string()));
        assert_eq!(store.hget("h", "missing"), None);
    }

    #[test]
    fn hset_counts_only_new_fields() {
        let mut store = KVStore::new();
        let a = vec![("f".to_string(), "1".to_string())];
        let b = vec![
            ("f".to_string(), "2".to_string()),
            ("g".to_string(), "3".to_string()),
        ];
        assert_eq!(store.hset("h", &a), 1);
        assert_eq!(store.hset("h", &b), 1);
        assert_eq!(store.hget("h", "f"), Some("2".to_string()));
    }

    #[test]
    fn hdel_hexists_hlen_and_hmget_work() {
        let mut store = KVStore::new();
        let pairs = vec![
            ("a".to_string(), "1".to_string()),
            ("b".to_string(), "2".to_string()),
        ];
        store.hset("h", &pairs);
        assert!(store.hexists("h", "a"));
        assert_eq!(store.hlen("h"), 2);
        let vals = store.hmget("h", &["a".to_string(), "x".to_string(), "b".to_string()]);
        assert_eq!(
            vals,
            vec![Some("1".to_string()), None, Some("2".to_string())]
        );
        assert_eq!(store.hdel("h", &["a".to_string(), "x".to_string()]), 1);
        assert_eq!(store.hlen("h"), 1);
    }

    #[test]
    fn hash_reads_reflect_immediate_expiration_without_get() {
        let mut store = KVStore::new();
        store.hset("h", &[("a".to_string(), "1".to_string())]);
        assert!(store.expire("h", 0));

        assert_eq!(store.hlen("h"), 0);
        assert_eq!(store.hget("h", "a"), None);
    }
}
