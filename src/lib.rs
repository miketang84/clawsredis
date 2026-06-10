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
