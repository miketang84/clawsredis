use bincode::error::{DecodeError, EncodeError};
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::io;
use std::io::Read;
use std::time::{SystemTime, UNIX_EPOCH};

/// A simple in-memory key-value store with pub/sub support, TTL/expiration, and persistence.
#[derive(Serialize, Deserialize, Encode, Decode)]
pub struct KVStore {
    data: HashMap<String, String>,
    /// Expiration times per key: key -> absolute timestamp (seconds since epoch)
    #[serde(rename = "exp")]
    expiration: HashMap<String, u64>,
    /// Subscribers per channel: channel -> set of subscriber IDs
    subscribers: HashMap<String, BTreeSet<usize>>,
    /// Next subscriber ID
    next_sub_id: usize,
    /// Path to persistence file
    #[serde(skip)]
    persist_path: Option<String>,
}

impl Default for KVStore {
    fn default() -> Self {
        Self {
            data: HashMap::new(),
            expiration: HashMap::new(),
            subscribers: HashMap::new(),
            next_sub_id: 1,
            persist_path: None,
        }
    }
}

impl KVStore {
    /// Creates a new empty `KVStore`.

    fn now_seconds() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is before UNIX_EPOCH")
            .as_secs()
    }
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts a key-value pair into the store with optional TTL (in seconds).
    pub fn set_with_ttl(&mut self, key: String, value: String, ttl: Option<u64>) {
        self.data.insert(key.clone(), value);
        if let Some(ttl_seconds) = ttl {
            let expiration = Self::now_seconds() + ttl_seconds;
            self.expiration.insert(key, expiration);
        } else {
            self.expiration.remove(&key);
        }
    }

    /// Inserts a key-value pair into the store without TTL.
    pub fn set(&mut self, key: String, value: String) {
        self.set_with_ttl(key, value, None);
    }

    /// Gets a value by key, automatically expiring it if TTL has passed.
    pub fn get(&mut self, key: &str) -> Option<&String> {
        if let Some(expiration) = self.expiration.get(key) {
            let now = Self::now_seconds();
            if now >= *expiration {
                self.data.remove(key);
                self.expiration.remove(key);
                return None;
            }
        }
        self.data.get(key)
    }

    /// Deletes a key from the store. Returns `true` if the key existed.
    pub fn del(&mut self, key: &str) -> bool {
        self.expiration.remove(key);
        self.data.remove(key).is_some()
    }

    /// Returns all non-expired keys in the store.
    pub fn keys(&mut self) -> Vec<&String> {
        let keys: Vec<String> = self.data.keys().cloned().collect();
        for key in keys {
            self.get(&key);
        }
        self.data.keys().collect()
    }

    /// Subscribe to a channel. Returns subscriber ID.
    pub fn subscribe(&mut self, channel: String) -> usize {
        let id = self.next_sub_id;
        self.next_sub_id += 1;
        self.subscribers.entry(channel).or_default().insert(id);
        id
    }

    /// Unsubscribe from a channel.
    pub fn unsubscribe(&mut self, channel: &str, sub_id: usize) {
        if let Some(subs) = self.subscribers.get_mut(channel) {
            subs.remove(&sub_id);
            if subs.is_empty() {
                self.subscribers.remove(channel);
            }
        }
    }

    /// Publish a message to a channel. Returns number of subscribers notified.
    pub fn publish(&mut self, channel: &str, message: String) -> usize {
        let count = self.subscribers.get(channel).map(|s| s.len()).unwrap_or(0);
        // Store last message for each channel (for new subscribers)
        if !message.is_empty() {
            self.data.insert(format!("__pubsub__:{}", channel), message);
        }
        count
    }

    /// Set the persistence file path.
    pub fn set_persist_path(&mut self, path: String) {
        self.persist_path = Some(path);
    }

    /// Persist current state to file.
    pub fn persist(&self) -> io::Result<()> {
        if let Some(ref path) = self.persist_path {
            let encoded = match bincode::encode_to_vec(self, bincode::config::standard()) {
                Ok(e) => e,
                Err(EncodeError::Io { inner, .. }) => return Err(inner),
                Err(err) => return Err(io::Error::other(err.to_string())),
            };
            fs::write(path, encoded)?;
        }
        Ok(())
    }

    /// Load state from file.
    pub fn load(path: &str) -> io::Result<Self> {
        let mut file = fs::File::open(path)?;
        let mut contents = Vec::new();
        file.read_to_end(&mut contents)?;
        let (mut store, _): (KVStore, _) =
            match bincode::decode_from_slice(&contents, bincode::config::standard()) {
                Ok(s) => s,
                Err(DecodeError::Io { inner, .. }) => return Err(inner),
                Err(err) => return Err(io::Error::other(err.to_string())),
            };
        // Clear expiration times since they're relative to when the data was saved
        store.expiration.clear();
        store.persist_path = Some(path.to_string());
        Ok(store)
    }

    /// Save to file if persistence is enabled.
    pub fn maybe_persist(&mut self) -> io::Result<()> {
        if self.persist_path.is_some() {
            self.persist()
        } else {
            Ok(())
        }
    }

    /// Get remaining TTL for a key in seconds. Returns None if key doesn't exist or has no TTL.
    pub fn ttl(&self, key: &str) -> Option<i64> {
        if let Some(expiration) = self.expiration.get(key) {
            let now = Self::now_seconds();
            let remaining = *expiration as i64 - now as i64;
            if remaining > 0 {
                Some(remaining)
            } else {
                None // Already expired
            }
        } else if self.data.contains_key(key) {
            Some(-1) // Exists but no TTL
        } else {
            None // Key doesn't exist
        }
    }

    /// Set expiration for an existing key.
    pub fn expire(&mut self, key: &str, ttl_seconds: u64) -> bool {
        if self.data.contains_key(key) {
            let expiration = Self::now_seconds() + ttl_seconds;
            self.expiration.insert(key.to_string(), expiration);
            true
        } else {
            false
        }
    }

    /// Remove expiration for an existing key (make it persistent).
    pub fn persist_key(&mut self, key: &str) -> bool {
        if self.expiration.remove(key).is_some() {
            true
        } else {
            self.data.contains_key(key)
        }
    }
}

#[cfg(test)]
mod tests {
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
