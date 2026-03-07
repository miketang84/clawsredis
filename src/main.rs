use std::collections::{BTreeSet, HashMap};
use serde::{Deserialize, Serialize};
use std::env;
use std::io::{self, BufRead, Read, Write};
use std::sync::{Arc, Mutex};
use bincode::{Encode, Decode};
use bincode::error::{EncodeError, DecodeError};

type Storage = HashMap<String, String>;

#[derive(Serialize, Deserialize, Default, Encode, Decode)]
struct KVStore {
    data: Storage,
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
            expiration: HashMap::new(),
            subscribers: HashMap::new(),
            next_sub_id: 1,
            persist_path: None,
        }
    }

    fn set_with_ttl(&mut self, key: String, value: String, ttl: Option<u64>) {
        self.data.insert(key.clone(), value);
        if let Some(ttl_seconds) = ttl {
            let expiration = std::time::Instant::now().elapsed().as_secs() + ttl_seconds;
            self.expiration.insert(key, expiration);
        } else {
            self.expiration.remove(&key);
        }
    }

    fn get(&mut self, key: &str) -> Option<&String> {
        if let Some(expiration) = self.expiration.get(key) {
            let now = std::time::Instant::now().elapsed().as_secs();
            if now >= *expiration {
                self.data.remove(key);
                self.expiration.remove(key);
                return None;
            }
        }
        self.data.get(key)
    }

    fn del(&mut self, key: &str) -> bool {
        self.expiration.remove(key);
        self.data.remove(key).is_some()
    }

    fn keys(&mut self) -> Vec<&String> {
        let keys: Vec<String> = self.data.keys().cloned().collect();
        for key in keys {
            self.get(&key);
        }
        self.data.keys().collect()
    }

    fn subscribe(&mut self, channel: String) -> usize {
        let id = self.next_sub_id;
        self.next_sub_id += 1;
        self.subscribers
            .entry(channel)
            .or_default()
            .insert(id);
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
        let (mut store, _): (KVStore, _) = match bincode::decode_from_slice(&contents, bincode::config::standard()) {
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
            let now = std::time::Instant::now().elapsed().as_secs();
            let remaining = *expiration as i64 - now as i64;
            if remaining > 0 {
                Some(remaining)
            } else {
                None
            }
        } else if self.data.contains_key(key) {
            Some(-1)
        } else {
            None
        }
    }

    fn expire(&mut self, key: &str, ttl_seconds: u64) -> bool {
        if self.data.contains_key(key) {
            let expiration = std::time::Instant::now().elapsed().as_secs() + ttl_seconds;
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
        "Commands: GET <key>, SET <key> <value> [TTL], DEL <key>, KEYS, SUBSCRIBE <channel>, PUBLISH <channel> <message>, UNSUBSCRIBE <channel> <sub_id>, TTL <key>, EXPIRE <key> <seconds>, PERSISTKEY <key>, PERSIST <path>, QUIT"
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
                let rest = parts[1].trim();
                let parts2: Vec<&str> = rest.splitn(2, ' ').collect();
                if parts2.len() < 2 {
                    writeln!(stdout_lock, "Error: SET requires <key> <value> [TTL]").unwrap();
                    continue;
                }
                let key = parts2[0].trim();
                let value = parts2[1].trim();
                
                // Check for optional TTL
                let ttl: Option<u64> = if parts2.len() >= 3 {
                    parts2[2].trim().parse().ok()
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
                    writeln!(stdout_lock, "Error: UNSUBSCRIBE requires <channel> <sub_id>").unwrap();
                    continue;
                }
                let rest = parts[1].trim();
                let parts2: Vec<&str> = rest.splitn(2, ' ').collect();
                if parts2.len() < 2 {
                    writeln!(stdout_lock, "Error: UNSUBSCRIBE requires <channel> <sub_id>").unwrap();
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
                    Some(-1) => writeln!(stdout_lock, "(integer) -1").unwrap(), // No TTL
                    Some(t) => writeln!(stdout_lock, "(integer) {}", t).unwrap(), // Remaining seconds
                    None => writeln!(stdout_lock, "(nil)").unwrap(), // Key doesn't exist or expired
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
