use bincode::error::{DecodeError, EncodeError};
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use std::io::{self, Read};

type Storage = HashMap<String, String>;
type ListStorage = HashMap<String, Vec<String>>;
type HashStorage = HashMap<String, HashMap<String, String>>;
type SortedSetStorage = HashMap<String, HashMap<String, f64>>;

/// A Redis-like in-memory key-value store with strings, lists, hashes, sorted sets, TTL,
/// pub/sub metadata, and bincode persistence.
#[derive(Serialize, Deserialize, Default, Encode, Decode)]
pub struct KVStore {
    data: Storage,
    lists: ListStorage,
    hashes: HashStorage,
    zsets: SortedSetStorage,
    /// Expiration times per key: key -> absolute timestamp (seconds since epoch)
    #[serde(rename = "exp")]
    expiration: HashMap<String, u64>,
    subscribers: HashMap<String, BTreeSet<usize>>,
    next_sub_id: usize,
    persist_path: Option<String>,
}

impl KVStore {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            lists: HashMap::new(),
            hashes: HashMap::new(),
            zsets: HashMap::new(),
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

    pub fn set_with_ttl(&mut self, key: String, value: String, ttl: Option<u64>) {
        self.lists.remove(&key);
        self.hashes.remove(&key);
        self.zsets.remove(&key);
        self.data.insert(key.clone(), value);
        if let Some(ttl_seconds) = ttl {
            let expiration = Self::now_seconds() + ttl_seconds;
            self.expiration.insert(key, expiration);
        } else {
            self.expiration.remove(&key);
        }
    }

    /// Inserts a string key-value pair without a TTL.
    pub fn set(&mut self, key: String, value: String) {
        self.set_with_ttl(key, value, None);
    }

    fn evict_if_expired(&mut self, key: &str) -> bool {
        if let Some(expiration) = self.expiration.get(key) {
            let now = Self::now_seconds();
            if now >= *expiration {
                self.data.remove(key);
                self.lists.remove(key);
                self.hashes.remove(key);
                self.zsets.remove(key);
                self.expiration.remove(key);
                return true;
            }
        }
        false
    }

    pub fn get(&mut self, key: &str) -> Option<&String> {
        if self.evict_if_expired(key) {
            return None;
        }
        self.data.get(key)
    }

    pub fn del(&mut self, key: &str) -> bool {
        self.expiration.remove(key);
        self.data.remove(key).is_some()
            || self.lists.remove(key).is_some()
            || self.hashes.remove(key).is_some()
            || self.zsets.remove(key).is_some()
    }

    pub fn keys(&mut self) -> Vec<String> {
        let all_keys: Vec<String> = self
            .data
            .keys()
            .chain(self.lists.keys())
            .chain(self.hashes.keys())
            .chain(self.zsets.keys())
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
            .chain(self.zsets.keys())
            .cloned()
            .collect();
        keys.sort();
        keys.dedup();
        keys
    }

    pub fn subscribe(&mut self, channel: String) -> usize {
        let id = self.next_sub_id;
        self.next_sub_id += 1;
        self.subscribers.entry(channel).or_default().insert(id);
        id
    }

    pub fn unsubscribe(&mut self, channel: &str, sub_id: usize) {
        if let Some(subs) = self.subscribers.get_mut(channel) {
            subs.remove(&sub_id);
            if subs.is_empty() {
                self.subscribers.remove(channel);
            }
        }
    }

    pub fn publish(&mut self, channel: &str, message: String) -> usize {
        let count = self.subscribers.get(channel).map(|s| s.len()).unwrap_or(0);
        if !message.is_empty() {
            self.data.insert(format!("__pubsub__:{}", channel), message);
        }
        count
    }

    pub fn set_persist_path(&mut self, path: String) {
        self.persist_path = Some(path);
    }

    pub fn persist(&self) -> io::Result<()> {
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

    pub fn load(path: &str) -> io::Result<Self> {
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

    pub fn maybe_persist(&mut self) -> io::Result<()> {
        if self.persist_path.is_some() {
            self.persist()
        } else {
            Ok(())
        }
    }

    pub fn ttl(&mut self, key: &str) -> Option<i64> {
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
            || self.zsets.contains_key(key)
        {
            Some(-1)
        } else {
            None
        }
    }

    pub fn expire(&mut self, key: &str, ttl_seconds: u64) -> bool {
        if self.data.contains_key(key)
            || self.lists.contains_key(key)
            || self.hashes.contains_key(key)
            || self.zsets.contains_key(key)
        {
            let expiration = Self::now_seconds() + ttl_seconds;
            self.expiration.insert(key.to_string(), expiration);
            true
        } else {
            false
        }
    }

    pub fn persist_key(&mut self, key: &str) -> bool {
        if self.expiration.remove(key).is_some() {
            true
        } else {
            self.data.contains_key(key)
                || self.lists.contains_key(key)
                || self.hashes.contains_key(key)
                || self.zsets.contains_key(key)
        }
    }

    pub fn exists_key(&mut self, key: &str) -> bool {
        if self.evict_if_expired(key) {
            return false;
        }
        self.data.contains_key(key) || self.lists.contains_key(key) || self.hashes.contains_key(key) || self.zsets.contains_key(key)
    }

    pub fn setnx(&mut self, key: String, value: String) -> bool {
        if self.exists_key(&key) {
            false
        } else {
            self.set_with_ttl(key, value, None);
            true
        }
    }

    pub fn setex(&mut self, key: String, value: String, ttl_seconds: u64) {
        self.set_with_ttl(key, value, Some(ttl_seconds));
    }

    pub fn mget(&mut self, keys: &[String]) -> Vec<Option<String>> {
        keys.iter().map(|k| self.get(k).cloned()).collect()
    }

    pub fn is_non_string_key(&mut self, key: &str) -> bool {
        if self.evict_if_expired(key) {
            return false;
        }
        self.lists.contains_key(key) || self.hashes.contains_key(key) || self.zsets.contains_key(key)
    }

    pub fn is_non_zset_key(&mut self, key: &str) -> bool {
        if self.evict_if_expired(key) {
            return false;
        }
        self.data.contains_key(key) || self.lists.contains_key(key) || self.hashes.contains_key(key)
    }

    pub fn append(&mut self, key: String, suffix: String) -> usize {
        if let Some(existing) = self.data.get_mut(&key) {
            existing.push_str(&suffix);
            existing.len()
        } else {
            self.set_with_ttl(key, suffix.clone(), None);
            suffix.len()
        }
    }

    pub fn strlen(&mut self, key: &str) -> usize {
        self.get(key).map(|v| v.len()).unwrap_or(0)
    }

    pub fn getset(&mut self, key: String, value: String) -> Option<String> {
        let previous = self.get(&key).cloned();
        self.set_with_ttl(key, value, None);
        previous
    }

    pub fn incrby(&mut self, key: String, delta: i64) -> Result<i64, &'static str> {
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

    pub fn mset(&mut self, items: &[(String, String)]) {
        for (k, v) in items {
            self.set_with_ttl(k.clone(), v.clone(), None);
        }
    }

    pub fn msetnx(&mut self, items: &[(String, String)]) -> bool {
        if items.iter().any(|(k, _)| self.exists_key(k)) {
            return false;
        }
        self.mset(items);
        true
    }

    pub fn rpush(&mut self, key: &str, values: &[String]) -> usize {
        self.data.remove(key);
        self.hashes.remove(key);
        self.zsets.remove(key);
        let list = self.lists.entry(key.to_string()).or_default();
        list.extend(values.iter().cloned());
        list.len()
    }

    pub fn lpush(&mut self, key: &str, values: &[String]) -> usize {
        self.data.remove(key);
        self.hashes.remove(key);
        self.zsets.remove(key);
        let list = self.lists.entry(key.to_string()).or_default();
        for value in values {
            list.insert(0, value.clone());
        }
        list.len()
    }

    pub fn lrange(&mut self, key: &str, start: isize, stop: isize) -> Option<Vec<String>> {
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

    pub fn llen(&mut self, key: &str) -> Option<usize> {
        if self.evict_if_expired(key) {
            return None;
        }
        self.lists.get(key).map(Vec::len)
    }

    pub fn hset(&mut self, key: &str, items: &[(String, String)]) -> usize {
        self.data.remove(key);
        self.lists.remove(key);
        self.zsets.remove(key);
        let hash = self.hashes.entry(key.to_string()).or_default();
        let mut new_fields = 0;
        for (field, value) in items {
            if hash.insert(field.clone(), value.clone()).is_none() {
                new_fields += 1;
            }
        }
        new_fields
    }

    pub fn hget(&mut self, key: &str, field: &str) -> Option<String> {
        if self.evict_if_expired(key) {
            return None;
        }
        self.hashes.get(key).and_then(|h| h.get(field).cloned())
    }

    pub fn hmget(&mut self, key: &str, fields: &[String]) -> Vec<Option<String>> {
        if self.evict_if_expired(key) {
            return fields.iter().map(|_| None).collect();
        }
        let hash = match self.hashes.get(key) {
            Some(h) => h,
            None => return fields.iter().map(|_| None).collect(),
        };
        fields.iter().map(|f| hash.get(f).cloned()).collect()
    }

    pub fn hdel(&mut self, key: &str, fields: &[String]) -> usize {
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

    pub fn hexists(&mut self, key: &str, field: &str) -> bool {
        if self.evict_if_expired(key) {
            return false;
        }
        self.hashes.get(key).is_some_and(|h| h.contains_key(field))
    }

    pub fn hlen(&mut self, key: &str) -> usize {
        if self.evict_if_expired(key) {
            return 0;
        }
        self.hashes.get(key).map(|h| h.len()).unwrap_or(0)
    }

    pub fn hkeys(&mut self, key: &str) -> Vec<String> {
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

    pub fn hvals(&mut self, key: &str) -> Vec<String> {
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

    pub fn hgetall(&mut self, key: &str) -> Vec<(String, String)> {
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

    pub fn zadd(&mut self, key: &str, entries: &[(f64, String)]) -> usize {
        self.data.remove(key);
        self.lists.remove(key);
        self.hashes.remove(key);

        let zset = self.zsets.entry(key.to_string()).or_default();
        let mut added = 0;
        for (score, member) in entries {
            if zset.insert(member.clone(), *score).is_none() {
                added += 1;
            }
        }
        added
    }

    pub fn zcard(&mut self, key: &str) -> usize {
        if self.evict_if_expired(key) {
            return 0;
        }
        self.zsets.get(key).map(|z| z.len()).unwrap_or(0)
    }

    pub fn zscore(&mut self, key: &str, member: &str) -> Option<f64> {
        if self.evict_if_expired(key) {
            return None;
        }
        self.zsets.get(key).and_then(|z| z.get(member).copied())
    }

    pub fn zrem(&mut self, key: &str, members: &[String]) -> usize {
        if self.evict_if_expired(key) {
            return 0;
        }
        let mut removed = 0;
        let mut remove_key = false;
        if let Some(zset) = self.zsets.get_mut(key) {
            for member in members {
                if zset.remove(member).is_some() {
                    removed += 1;
                }
            }
            if zset.is_empty() {
                remove_key = true;
            }
        }
        if remove_key {
            self.zsets.remove(key);
            self.expiration.remove(key);
        }
        removed
    }

    pub fn zrange(&mut self, key: &str, start: isize, stop: isize) -> Option<Vec<String>> {
        if self.evict_if_expired(key) {
            return None;
        }

        let zset = self.zsets.get(key)?;
        if zset.is_empty() {
            return Some(vec![]);
        }

        let mut entries: Vec<(String, f64)> = zset.iter().map(|(m, s)| (m.clone(), *s)).collect();
        entries.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));

        let len = entries.len() as isize;
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

        Some(
            entries[s as usize..=e as usize]
                .iter()
                .map(|(member, _)| member.clone())
                .collect(),
        )
    }

    pub fn zincrby(&mut self, key: &str, increment: f64, member: String) -> f64 {
        self.data.remove(key);
        self.lists.remove(key);
        self.hashes.remove(key);

        let zset = self.zsets.entry(key.to_string()).or_default();
        let next = zset.get(&member).copied().unwrap_or(0.0) + increment;
        zset.insert(member, next);
        next
    }
}

/// Shared command result used by the CLI today and the RESP server in later issues.
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    /// A successful status response.
    Simple(String),
    /// An error response.
    Error(String),
    /// An integer response.
    Int(i64),
    /// A non-null string response.
    Bulk(String),
    /// A null response.
    Nil,
    /// A list of response values.
    Array(Vec<RespValue>),
}

fn persist_or_error(store: &mut KVStore) -> Option<RespValue> {
    match store.maybe_persist() {
        Ok(()) => None,
        Err(_) => Some(RespValue::Error("Warning: Persist failed".to_string())),
    }
}

fn wrong_type() -> RespValue {
    RespValue::Error("Error: WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
}

/// Executes a parsed command against the provided store.
///
/// `args[0]` is the command name and remaining items are command arguments.
pub fn execute_command(store: &mut KVStore, args: &[String]) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("Error: command required".to_string());
    }

    let command = args[0].to_uppercase();
    match command.as_str() {
        "GET" => {
            if args.len() != 2 {
                return RespValue::Error("Error: GET requires a key".to_string());
            }
            match store.get(&args[1]) {
                Some(value) => RespValue::Bulk(value.clone()),
                None => RespValue::Nil,
            }
        }
        "SET" => {
            if args.len() < 3 {
                return RespValue::Error("Error: SET requires <key> <value> [TTL]".to_string());
            }
            let ttl = if args.len() >= 4 {
                match args[3].parse::<u64>() {
                    Ok(v) => Some(v),
                    Err(_) => return RespValue::Error("Error: Invalid TTL value".to_string()),
                }
            } else {
                None
            };
            store.set_with_ttl(args[1].clone(), args[2].clone(), ttl);
            persist_or_error(store).unwrap_or_else(|| RespValue::Simple("OK".to_string()))
        }
        "SETNX" => {
            if args.len() != 3 {
                return RespValue::Error("Error: SETNX requires <key> <value>".to_string());
            }
            let inserted = store.setnx(args[1].clone(), args[2].clone());
            if inserted {
                if let Some(err) = persist_or_error(store) {
                    return err;
                }
            }
            RespValue::Int(if inserted { 1 } else { 0 })
        }
        "SETEX" => {
            if args.len() != 4 {
                return RespValue::Error("Error: SETEX requires <key> <seconds> <value>".to_string());
            }
            let ttl = match args[2].parse::<u64>() {
                Ok(v) => v,
                Err(_) => return RespValue::Error("Error: Invalid TTL value".to_string()),
            };
            store.setex(args[1].clone(), args[3].clone(), ttl);
            persist_or_error(store).unwrap_or_else(|| RespValue::Simple("OK".to_string()))
        }
        "MSET" | "MSETNX" => {
            if args.len() < 3 || (args.len() - 1) % 2 != 0 {
                return RespValue::Error(format!(
                    "Error: {} requires even number of key/value args",
                    command
                ));
            }
            let mut kvs: Vec<(String, String)> = Vec::new();
            let mut i = 1;
            while i < args.len() {
                kvs.push((args[i].clone(), args[i + 1].clone()));
                i += 2;
            }
            if command == "MSET" {
                store.mset(&kvs);
                persist_or_error(store).unwrap_or_else(|| RespValue::Simple("OK".to_string()))
            } else {
                let inserted = store.msetnx(&kvs);
                if inserted {
                    if let Some(err) = persist_or_error(store) {
                        return err;
                    }
                }
                RespValue::Int(if inserted { 1 } else { 0 })
            }
        }
        "MGET" => {
            if args.len() < 2 {
                return RespValue::Error("Error: MGET requires <k1> [k2...]".to_string());
            }
            let keys: Vec<String> = args[1..].to_vec();
            RespValue::Array(
                store
                    .mget(&keys)
                    .into_iter()
                    .map(|value| match value {
                        Some(v) => RespValue::Bulk(v),
                        None => RespValue::Nil,
                    })
                    .collect(),
            )
        }
        "APPEND" => {
            if args.len() != 3 {
                return RespValue::Error("Error: APPEND requires <key> <value>".to_string());
            }
            if store.is_non_string_key(&args[1]) {
                return wrong_type();
            }
            let len = store.append(args[1].clone(), args[2].clone());
            if let Some(err) = persist_or_error(store) {
                return err;
            }
            RespValue::Int(len as i64)
        }
        "STRLEN" => {
            if args.len() != 2 {
                return RespValue::Error("Error: STRLEN requires a key".to_string());
            }
            if store.is_non_string_key(&args[1]) {
                return wrong_type();
            }
            RespValue::Int(store.strlen(&args[1]) as i64)
        }
        "GETSET" => {
            if args.len() != 3 {
                return RespValue::Error("Error: GETSET requires <key> <value>".to_string());
            }
            if store.is_non_string_key(&args[1]) {
                return wrong_type();
            }
            let previous = store.getset(args[1].clone(), args[2].clone());
            if let Some(err) = persist_or_error(store) {
                return err;
            }
            match previous {
                Some(v) => RespValue::Bulk(v),
                None => RespValue::Nil,
            }
        }
        "INCR" | "DECR" | "INCRBY" | "DECRBY" => {
            if args.len() < 2 {
                return RespValue::Error(format!("Error: {} requires arguments", command));
            }
            let (key, delta): (&str, i64) = match command.as_str() {
                "INCR" | "DECR" => {
                    if args.len() != 2 {
                        return RespValue::Error(format!("Error: {} requires <key>", command));
                    }
                    (&args[1], if command == "INCR" { 1 } else { -1 })
                }
                "INCRBY" | "DECRBY" => {
                    if args.len() != 3 {
                        return RespValue::Error(format!("Error: {} requires <key> <n>", command));
                    }
                    let n = match args[2].parse::<i64>() {
                        Ok(v) => v,
                        Err(_) => {
                            return RespValue::Error(format!(
                                "Error: {} value must be an integer",
                                command
                            ))
                        }
                    };
                    (&args[1], if command == "INCRBY" { n } else { -n })
                }
                _ => unreachable!(),
            };
            if store.is_non_string_key(key) {
                return wrong_type();
            }
            match store.incrby(key.to_string(), delta) {
                Ok(v) => {
                    if let Some(err) = persist_or_error(store) {
                        return err;
                    }
                    RespValue::Int(v)
                }
                Err(msg) => RespValue::Error(msg.to_string()),
            }
        }
        "DEL" => {
            if args.len() != 2 {
                return RespValue::Error("Error: DEL requires a key".to_string());
            }
            let deleted = store.del(&args[1]);
            if let Some(err) = persist_or_error(store) {
                return err;
            }
            RespValue::Int(if deleted { 1 } else { 0 })
        }
        "KEYS" => RespValue::Array(store.keys().into_iter().map(RespValue::Bulk).collect()),
        "SUBSCRIBE" => {
            if args.len() != 2 {
                return RespValue::Error("Error: SUBSCRIBE requires a channel".to_string());
            }
            let sub_id = store.subscribe(args[1].clone());
            RespValue::Simple(format!("Subscribed to {} with ID {}", args[1], sub_id))
        }
        "PUBLISH" => {
            if args.len() != 3 {
                return RespValue::Error("Error: PUBLISH requires <channel> <message>".to_string());
            }
            let count = store.publish(&args[1], args[2].clone());
            RespValue::Simple(format!("Published to {} ({} subscribers)", args[1], count))
        }
        "UNSUBSCRIBE" => {
            if args.len() != 3 {
                return RespValue::Error("Error: UNSUBSCRIBE requires <channel> <sub_id>".to_string());
            }
            let sub_id = match args[2].parse::<usize>() {
                Ok(id) => id,
                Err(_) => return RespValue::Error("Error: Invalid subscription ID".to_string()),
            };
            store.unsubscribe(&args[1], sub_id);
            RespValue::Simple(format!("Unsubscribed from {} (ID {})", args[1], sub_id))
        }
        "TTL" => {
            if args.len() != 2 {
                return RespValue::Error("Error: TTL requires a key".to_string());
            }
            match store.ttl(&args[1]) {
                Some(t) => RespValue::Int(t),
                None => RespValue::Nil,
            }
        }
        "EXPIRE" => {
            if args.len() != 3 {
                return RespValue::Error("Error: EXPIRE requires <key> <seconds>".to_string());
            }
            let ttl = match args[2].parse::<u64>() {
                Ok(t) => t,
                Err(_) => return RespValue::Error("Error: Invalid TTL value".to_string()),
            };
            if store.expire(&args[1], ttl) {
                if let Some(err) = persist_or_error(store) {
                    return err;
                }
                RespValue::Simple("OK".to_string())
            } else {
                RespValue::Int(0)
            }
        }
        "PERSISTKEY" => {
            if args.len() != 2 {
                return RespValue::Error("Error: PERSISTKEY requires a key".to_string());
            }
            if store.persist_key(&args[1]) {
                if let Some(err) = persist_or_error(store) {
                    return err;
                }
                RespValue::Simple("OK".to_string())
            } else {
                RespValue::Int(0)
            }
        }
        "PERSIST" => {
            if args.len() != 2 {
                return RespValue::Error("Error: PERSIST requires <path>".to_string());
            }
            store.set_persist_path(args[1].clone());
            match store.maybe_persist() {
                Ok(()) => RespValue::Simple(format!("Persistence enabled: {}", args[1])),
                Err(_) => RespValue::Error("Error: Failed to persist".to_string()),
            }
        }
        "RPUSH" | "LPUSH" => {
            if args.len() < 3 {
                return RespValue::Error(format!(
                    "Error: {} requires <key> <v1> [v2...]",
                    command
                ));
            }
            let values: Vec<String> = args[2..].to_vec();
            let len = if command == "RPUSH" {
                store.rpush(&args[1], &values)
            } else {
                store.lpush(&args[1], &values)
            };
            if let Some(err) = persist_or_error(store) {
                return err;
            }
            RespValue::Int(len as i64)
        }
        "LRANGE" => {
            if args.len() != 4 {
                return RespValue::Error("Error: LRANGE requires <key> <start> <stop>".to_string());
            }
            let start = match args[2].parse::<isize>() {
                Ok(v) => v,
                Err(_) => return RespValue::Error("Error: LRANGE start must be an integer".to_string()),
            };
            let stop = match args[3].parse::<isize>() {
                Ok(v) => v,
                Err(_) => return RespValue::Error("Error: LRANGE stop must be an integer".to_string()),
            };
            match store.lrange(&args[1], start, stop) {
                Some(values) => RespValue::Array(values.into_iter().map(RespValue::Bulk).collect()),
                None => RespValue::Nil,
            }
        }
        "LLEN" => {
            if args.len() != 2 {
                return RespValue::Error("Error: LLEN requires a key".to_string());
            }
            RespValue::Int(store.llen(&args[1]).unwrap_or(0) as i64)
        }
        "HSET" | "HMSET" => {
            if args.len() < 4 || args.len() % 2 != 0 {
                return RespValue::Error(format!(
                    "Error: {} requires <key> <field> <value> [field value...]",
                    command
                ));
            }
            let mut items: Vec<(String, String)> = Vec::new();
            let mut i = 2;
            while i < args.len() {
                items.push((args[i].clone(), args[i + 1].clone()));
                i += 2;
            }
            let added = store.hset(&args[1], &items);
            if let Some(err) = persist_or_error(store) {
                return err;
            }
            if command == "HSET" {
                RespValue::Int(added as i64)
            } else {
                RespValue::Simple("OK".to_string())
            }
        }
        "HGET" => {
            if args.len() != 3 {
                return RespValue::Error("Error: HGET requires <key> <field>".to_string());
            }
            match store.hget(&args[1], &args[2]) {
                Some(value) => RespValue::Bulk(value),
                None => RespValue::Nil,
            }
        }
        "HMGET" => {
            if args.len() < 3 {
                return RespValue::Error("Error: HMGET requires <key> <field> [field...]".to_string());
            }
            let fields: Vec<String> = args[2..].to_vec();
            RespValue::Array(
                store
                    .hmget(&args[1], &fields)
                    .into_iter()
                    .map(|value| match value {
                        Some(v) => RespValue::Bulk(v),
                        None => RespValue::Nil,
                    })
                    .collect(),
            )
        }
        "HDEL" => {
            if args.len() < 3 {
                return RespValue::Error("Error: HDEL requires <key> <field> [field...]".to_string());
            }
            let fields: Vec<String> = args[2..].to_vec();
            let removed = store.hdel(&args[1], &fields);
            if removed > 0 {
                if let Some(err) = persist_or_error(store) {
                    return err;
                }
            }
            RespValue::Int(removed as i64)
        }
        "HEXISTS" => {
            if args.len() != 3 {
                return RespValue::Error("Error: HEXISTS requires <key> <field>".to_string());
            }
            RespValue::Int(if store.hexists(&args[1], &args[2]) { 1 } else { 0 })
        }
        "HLEN" => {
            if args.len() != 2 {
                return RespValue::Error("Error: HLEN requires a key".to_string());
            }
            RespValue::Int(store.hlen(&args[1]) as i64)
        }
        "HKEYS" => {
            if args.len() != 2 {
                return RespValue::Error("Error: HKEYS requires a key".to_string());
            }
            RespValue::Array(store.hkeys(&args[1]).into_iter().map(RespValue::Bulk).collect())
        }
        "HVALS" => {
            if args.len() != 2 {
                return RespValue::Error("Error: HVALS requires a key".to_string());
            }
            RespValue::Array(store.hvals(&args[1]).into_iter().map(RespValue::Bulk).collect())
        }
        "HGETALL" => {
            if args.len() != 2 {
                return RespValue::Error("Error: HGETALL requires a key".to_string());
            }
            let values = store
                .hgetall(&args[1])
                .into_iter()
                .flat_map(|(field, value)| [RespValue::Bulk(field), RespValue::Bulk(value)])
                .collect();
            RespValue::Array(values)
        }
        "ZADD" => {
            if args.len() < 4 || args.len() % 2 != 0 {
                return RespValue::Error("Error: ZADD requires <key> <score> <member> [score member...]".to_string());
            }
            if store.is_non_zset_key(&args[1]) {
                return wrong_type();
            }
            let mut entries: Vec<(f64, String)> = Vec::new();
            let mut i = 2;
            while i < args.len() {
                let score = match args[i].parse::<f64>() {
                    Ok(v) => v,
                    Err(_) => return RespValue::Error("Error: ZADD score must be a number".to_string()),
                };
                entries.push((score, args[i + 1].clone()));
                i += 2;
            }
            let added = store.zadd(&args[1], &entries);
            if let Some(err) = persist_or_error(store) {
                return err;
            }
            RespValue::Int(added as i64)
        }
        "ZCARD" => {
            if args.len() != 2 {
                return RespValue::Error("Error: ZCARD requires a key".to_string());
            }
            if store.is_non_zset_key(&args[1]) {
                return wrong_type();
            }
            RespValue::Int(store.zcard(&args[1]) as i64)
        }
        "ZSCORE" => {
            if args.len() != 3 {
                return RespValue::Error("Error: ZSCORE requires <key> <member>".to_string());
            }
            if store.is_non_zset_key(&args[1]) {
                return wrong_type();
            }
            match store.zscore(&args[1], &args[2]) {
                Some(score) => RespValue::Bulk(score.to_string()),
                None => RespValue::Nil,
            }
        }
        "ZREM" => {
            if args.len() < 3 {
                return RespValue::Error("Error: ZREM requires <key> <member> [member...]".to_string());
            }
            if store.is_non_zset_key(&args[1]) {
                return wrong_type();
            }
            let members: Vec<String> = args[2..].to_vec();
            let removed = store.zrem(&args[1], &members);
            if removed > 0 {
                if let Some(err) = persist_or_error(store) {
                    return err;
                }
            }
            RespValue::Int(removed as i64)
        }
        "ZRANGE" => {
            if args.len() != 4 {
                return RespValue::Error("Error: ZRANGE requires <key> <start> <stop>".to_string());
            }
            if store.is_non_zset_key(&args[1]) {
                return wrong_type();
            }
            let start = match args[2].parse::<isize>() {
                Ok(v) => v,
                Err(_) => return RespValue::Error("Error: ZRANGE start must be an integer".to_string()),
            };
            let stop = match args[3].parse::<isize>() {
                Ok(v) => v,
                Err(_) => return RespValue::Error("Error: ZRANGE stop must be an integer".to_string()),
            };
            match store.zrange(&args[1], start, stop) {
                Some(values) => RespValue::Array(values.into_iter().map(RespValue::Bulk).collect()),
                None => RespValue::Nil,
            }
        }
        "ZINCRBY" => {
            if args.len() != 4 {
                return RespValue::Error("Error: ZINCRBY requires <key> <increment> <member>".to_string());
            }
            if store.is_non_zset_key(&args[1]) {
                return wrong_type();
            }
            let increment = match args[2].parse::<f64>() {
                Ok(v) => v,
                Err(_) => return RespValue::Error("Error: ZINCRBY increment must be a number".to_string()),
            };
            let score = store.zincrby(&args[1], increment, args[3].clone());
            if let Some(err) = persist_or_error(store) {
                return err;
            }
            RespValue::Bulk(score.to_string())
        }
        "QUIT" => RespValue::Simple("Goodbye!".to_string()),
        _ => RespValue::Error(format!("Unknown command: {}", command)),
    }
}

#[cfg(test)]
mod command_tests {
    use super::*;

    fn args(items: &[&str]) -> Vec<String> {
        items.iter().map(|item| (*item).to_string()).collect()
    }

    #[test]
    fn execute_command_handles_string_flow() {
        let mut store = KVStore::new();

        assert_eq!(
            execute_command(&mut store, &args(&["SET", "k", "v"])),
            RespValue::Simple("OK".to_string())
        );
        assert_eq!(
            execute_command(&mut store, &args(&["GET", "k"])),
            RespValue::Bulk("v".to_string())
        );
        assert_eq!(
            execute_command(&mut store, &args(&["DEL", "k"])),
            RespValue::Int(1)
        );
        assert_eq!(
            execute_command(&mut store, &args(&["GET", "k"])),
            RespValue::Nil
        );
    }

    #[test]
    fn execute_command_returns_arrays_for_multi_value_results() {
        let mut store = KVStore::new();
        assert_eq!(
            execute_command(&mut store, &args(&["MSET", "a", "1", "b", "2"])),
            RespValue::Simple("OK".to_string())
        );

        assert_eq!(
            execute_command(&mut store, &args(&["MGET", "a", "missing", "b"])),
            RespValue::Array(vec![
                RespValue::Bulk("1".to_string()),
                RespValue::Nil,
                RespValue::Bulk("2".to_string()),
            ])
        );
    }

    #[test]
    fn execute_command_reports_argument_errors() {
        let mut store = KVStore::new();

        assert_eq!(
            execute_command(&mut store, &args(&["SET", "only-key"])),
            RespValue::Error("Error: SET requires <key> <value> [TTL]".to_string())
        );
        assert_eq!(
            execute_command(&mut store, &args(&["NOPE"])),
            RespValue::Error("Unknown command: NOPE".to_string())
        );
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

    #[test]
    fn zadd_zscore_zcard_zrem_flow() {
        let mut store = KVStore::new();
        assert_eq!(
            store.zadd(
                "z",
                &[(1.5, "a".to_string()), (2.0, "b".to_string()), (2.0, "c".to_string())],
            ),
            3
        );
        assert_eq!(store.zcard("z"), 3);
        assert_eq!(store.zscore("z", "a"), Some(1.5));
        assert_eq!(store.zadd("z", &[(3.0, "a".to_string())]), 0);
        assert_eq!(store.zscore("z", "a"), Some(3.0));
        assert_eq!(store.zrem("z", &["a".to_string(), "missing".to_string()]), 1);
        assert_eq!(store.zcard("z"), 2);
    }

    #[test]
    fn zrange_orders_by_score_then_member() {
        let mut store = KVStore::new();
        store.zadd(
            "z",
            &[
                (2.0, "b".to_string()),
                (1.0, "c".to_string()),
                (2.0, "a".to_string()),
                (1.0, "aa".to_string()),
            ],
        );
        assert_eq!(
            store.zrange("z", 0, -1),
            Some(vec![
                "aa".to_string(),
                "c".to_string(),
                "a".to_string(),
                "b".to_string()
            ])
        );
        assert_eq!(store.zrange("z", 1, 2), Some(vec!["c".to_string(), "a".to_string()]));
    }

    #[test]
    fn zincrby_updates_and_creates_members() {
        let mut store = KVStore::new();
        store.zadd("z", &[(1.0, "a".to_string())]);
        assert_eq!(store.zincrby("z", 2.5, "a".to_string()), 3.5);
        assert_eq!(store.zincrby("z", 1.0, "b".to_string()), 1.0);
        assert_eq!(store.zscore("z", "a"), Some(3.5));
        assert_eq!(store.zscore("z", "b"), Some(1.0));
    }

    #[test]
    fn zset_reads_reflect_immediate_expiration_without_get() {
        let mut store = KVStore::new();
        store.zadd("z", &[(1.0, "a".to_string())]);
        assert!(store.expire("z", 0));

        assert_eq!(store.zcard("z"), 0);
        assert_eq!(store.zscore("z", "a"), None);
        assert_eq!(store.zrange("z", 0, -1), None);
    }

    #[test]
    fn zset_type_conflict_overwrites_and_blocks_other_types() {
        let mut store = KVStore::new();
        store.set_with_ttl("k".to_string(), "v".to_string(), None);
        store.zadd("k", &[(1.0, "a".to_string())]);
        assert_eq!(store.get("k"), None);
        assert!(store.is_non_string_key("k"));

        store.hset("h", &[("f".to_string(), "1".to_string())]);
        store.zadd("h", &[(1.0, "x".to_string())]);
        assert_eq!(store.hget("h", "f"), None);

        store.zadd("lz", &[(1.0, "m".to_string())]);
        store.rpush("lz", &["x".to_string()]);
        assert_eq!(store.zscore("lz", "m"), None);
    }

    #[test]
    fn keys_and_exists_include_zsets() {
        let mut store = KVStore::new();
        store.zadd("z", &[(1.0, "a".to_string())]);
        assert!(store.exists_key("z"));
        assert_eq!(store.keys(), vec!["z".to_string()]);
    }
}

#[cfg(test)]
mod legacy_tests {
    use super::*;

    #[test]
    fn test_set_and_get() {
        let mut store = KVStore::new();
        store.set("key1".to_string(), "value1".to_string());
        assert_eq!(store.get("key1"), Some(&"value1".to_string()));
    }

    #[test]
    fn test_get_nonexistent() {
        let mut store = KVStore::new();
        assert_eq!(store.get("nonexistent"), None);
    }

    #[test]
    fn test_delete() {
        let mut store = KVStore::new();
        store.set("key1".to_string(), "value1".to_string());
        assert!(store.del("key1"));
        assert!(!store.del("key1"));
        assert_eq!(store.get("key1"), None);
    }

    #[test]
    fn test_keys() {
        let mut store = KVStore::new();
        store.set("key1".to_string(), "value1".to_string());
        store.set("key2".to_string(), "value2".to_string());
        let keys = store.keys();
        assert_eq!(keys.len(), 2);
        let key1 = "key1".to_string();
        let key2 = "key2".to_string();
        assert!(keys.contains(&&key1));
        assert!(keys.contains(&&key2));
    }

    #[test]
    fn test_thread_safety() {
        let store = std::sync::Arc::new(std::sync::Mutex::new(KVStore::new()));
        let store_clone = std::sync::Arc::clone(&store);

        let handle = std::thread::spawn(move || {
            store_clone
                .lock()
                .unwrap()
                .set("thread_key".to_string(), "thread_value".to_string());
        });

        handle.join().unwrap();
        assert_eq!(
            store.lock().unwrap().get("thread_key"),
            Some(&"thread_value".to_string())
        );
    }

    #[test]
    fn test_ttl() {
        let mut store = KVStore::new();
        store.set_with_ttl("key1".to_string(), "value1".to_string(), Some(3600)); // 1 hour
        assert!(store.ttl("key1").map(|t| t > 0).unwrap_or(false));
        assert_eq!(store.get("key1"), Some(&"value1".to_string()));
    }

    #[test]
    fn test_ttl_decreases_after_elapsed_time() {
        let mut store = KVStore::new();
        store.set_with_ttl("key1".to_string(), "value1".to_string(), Some(3));

        std::thread::sleep(std::time::Duration::from_millis(1200));

        let remaining = store.ttl("key1").expect("key should still have TTL");
        assert!(
            remaining > 0,
            "TTL should still be non-zero after a short delay"
        );
        assert!(remaining < 3, "TTL should decrease after elapsed time");
    }

    #[test]
    fn test_expire_command() {
        let mut store = KVStore::new();
        store.set("key1".to_string(), "value1".to_string());
        assert!(store.expire("key1", 3600));
        assert!(store.ttl("key1").map(|t| t > 0).unwrap_or(false));

        // Non-existent key
        assert!(!store.expire("nonexistent", 3600));
    }

    #[test]
    fn test_persist_key() {
        let mut store = KVStore::new();
        store.set_with_ttl("key1".to_string(), "value1".to_string(), Some(3600));
        assert!(store.persist_key("key1"));
        assert_eq!(store.ttl("key1"), Some(-1));
    }
}
