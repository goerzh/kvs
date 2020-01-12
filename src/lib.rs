#![deny(missing_docs)]
//! A simple key/value store.

use std::collections::HashMap;

/// `KvStore` stores key/value pairs in Memory, not in disk.
///
///  Example:
///
/// ```rust
/// # use kvs::KvStore;
/// let mut kv = KvStore::new();
/// kv.set("hello".to_owned(), "world".to_owned());
/// let v = kv.get("hello".to_owned());
/// assert_eq!(v, Some("world".to_owned()));
/// kv.remove("hello".to_owned())
/// ```
pub struct KvStore {
    kv: HashMap<String, String>,
}

impl KvStore {
    /// Create a new KvStore
    pub fn new() -> KvStore {
        KvStore { kv: HashMap::new() }
    }

    /// Sets a value from a string key to a string.
    ///
    /// If the key already exists, the value will be overwritten.
    pub fn set(&mut self, key: String, val: String) {
        self.kv.insert(key, val);
    }

    /// Gets the string value from a given string key.
    ///
    /// Return None if the given key does not exist.
    pub fn get(&mut self, key: String) -> Option<String> {
        self.kv.get(key.as_str()).cloned()
    }

    /// Removes a given key.
    pub fn remove(&mut self, key: String) {
        self.kv.remove(key.as_str());
    }
}
