use parking_lot::Mutex;
/// Extended type registry for non-LSM data types (Stream, JSON, BF, CF, CMS, TopK, TDigest).
///
/// These types store their actual data in in-memory lazy_static stores but register
/// their key existence here so that TYPE, EXISTS, KEYS, SCAN, DEL work correctly.
use std::collections::HashMap;

lazy_static::lazy_static! {
    /// (db_index, key) -> type_name
    static ref REGISTRY: Mutex<HashMap<(usize, Vec<u8>), &'static str>> = Mutex::new(HashMap::new());
}

/// Register a key with its type. Call when a key is first created.
pub fn register(db: usize, key: &[u8], type_name: &'static str) {
    REGISTRY.lock().insert((db, key.to_vec()), type_name);
}

/// Unregister a key. Call when a key is deleted.
pub fn unregister(db: usize, key: &[u8]) {
    REGISTRY.lock().remove(&(db, key.to_vec()));
}

/// Returns the type name if the key is registered, or None.
pub fn get_type(db: usize, key: &[u8]) -> Option<&'static str> {
    REGISTRY.lock().get(&(db, key.to_vec())).copied()
}

/// Returns true if the key is registered.
pub fn exists(db: usize, key: &[u8]) -> bool {
    REGISTRY.lock().contains_key(&(db, key.to_vec()))
}

/// Returns all keys for a given db.
pub fn keys_for_db(db: usize) -> Vec<Vec<u8>> {
    REGISTRY
        .lock()
        .keys()
        .filter(|(d, _)| *d == db)
        .map(|(_, k)| k.clone())
        .collect()
}

/// Flush all keys for all databases.
pub fn flush_all() {
    REGISTRY.lock().clear();
}

/// Flush all keys for a specific database.
pub fn flush_db(db: usize) {
    REGISTRY.lock().retain(|(d, _), _| *d != db);
}
