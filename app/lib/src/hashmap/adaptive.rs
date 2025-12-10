//! Adaptive HashMap that switches between HashMap and DashMap based on size.
//!
//! This module provides `AdaptiveMap`, which automatically selects the optimal
//! hashmap implementation based on the expected dataset size:
//!
//! - For small datasets (below threshold): Uses standard `HashMap` for lower overhead
//! - For large datasets (above threshold): Uses `DashMap` for concurrent access performance
//!
//! The selection is transparent to the user and maintains consistent API behavior.

use std::collections::HashMap;
use std::hash::Hash;

use dashmap::DashMap;

/// Default threshold for switching from HashMap to DashMap.
pub const DEFAULT_THRESHOLD: usize = 10_000;

/// An adaptive map that selects between HashMap and DashMap based on capacity.
///
/// This enum provides a unified interface over two hashmap implementations:
/// - `Small`: Uses standard `HashMap` for lower overhead with small datasets
/// - `Large`: Uses `DashMap` for concurrent access performance with large datasets
///
/// # Thread Safety
///
/// `AdaptiveMap` implements `Send + Sync` when its key and value types do,
/// allowing it to be shared across threads. However, the thread safety
/// characteristics differ between variants:
///
/// ## Small Variant (HashMap)
///
/// The `Small` variant uses a standard `HashMap` which is NOT thread-safe
/// for concurrent writes. If you need to mutate a `Small` variant from
/// multiple threads, you must use external synchronization (e.g., `Mutex`).
///
/// ## Large Variant (DashMap)
///
/// The `Large` variant uses `DashMap`, which provides:
/// - **Lock-free reads**: Multiple threads can read concurrently without blocking
/// - **Fine-grained locking for writes**: Writes only lock individual shards,
///   allowing high concurrency
/// - **Atomic operations**: All operations are thread-safe
///
/// ## Choosing the Right Variant
///
/// - Use `Small` (below threshold) for single-threaded access or when you
///   control synchronization externally
/// - Use `Large` (above threshold) for concurrent access from multiple threads
///
/// The threshold can be configured via [`with_capacity_threshold`](Self::with_capacity_threshold).
///
/// # Example
///
/// ```
/// use als_compression::hashmap::AdaptiveMap;
///
/// // Create a map that will use HashMap (below threshold)
/// let small_map: AdaptiveMap<String, i32> = AdaptiveMap::with_capacity_threshold(100, 10_000);
/// assert!(small_map.is_small());
///
/// // Create a map that will use DashMap (above threshold)
/// let large_map: AdaptiveMap<String, i32> = AdaptiveMap::with_capacity_threshold(20_000, 10_000);
/// assert!(large_map.is_large());
/// ```
///
/// # Concurrent Access Example
///
/// ```rust,ignore
/// use als_compression::hashmap::AdaptiveMap;
/// use std::sync::Arc;
/// use std::thread;
///
/// // Create a large map for concurrent access
/// let map: Arc<AdaptiveMap<String, i32>> = Arc::new(
///     AdaptiveMap::with_capacity_threshold(20_000, 10_000)
/// );
/// assert!(map.is_large());
///
/// // Multiple threads can access concurrently
/// let handles: Vec<_> = (0..4).map(|i| {
///     let map = Arc::clone(&map);
///     thread::spawn(move || {
///         // Note: For mutable operations, you'd need interior mutability
///         // or use the DashMap directly
///         map.len()
///     })
/// }).collect();
/// ```
#[derive(Debug)]
pub enum AdaptiveMap<K, V>
where
    K: Eq + Hash,
{
    /// Small dataset variant using standard HashMap.
    Small(HashMap<K, V>),
    /// Large dataset variant using concurrent DashMap.
    Large(DashMap<K, V>),
}

impl<K, V> AdaptiveMap<K, V>
where
    K: Eq + Hash,
{
    /// Create a new empty AdaptiveMap using the default threshold.
    ///
    /// This creates a `Small` variant since the initial capacity is 0.
    pub fn new() -> Self {
        Self::Small(HashMap::new())
    }

    /// Create a new AdaptiveMap with the given capacity, using the default threshold.
    ///
    /// If `capacity` is below `DEFAULT_THRESHOLD`, creates a `Small` variant.
    /// Otherwise, creates a `Large` variant.
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_threshold(capacity, DEFAULT_THRESHOLD)
    }

    /// Create a new AdaptiveMap with the given capacity and threshold.
    ///
    /// # Arguments
    ///
    /// * `capacity` - Expected number of entries
    /// * `threshold` - Size threshold for switching to DashMap
    ///
    /// If `capacity < threshold`, creates a `Small` variant (HashMap).
    /// Otherwise, creates a `Large` variant (DashMap).
    pub fn with_capacity_threshold(capacity: usize, threshold: usize) -> Self {
        if capacity < threshold {
            Self::Small(HashMap::with_capacity(capacity))
        } else {
            Self::Large(DashMap::with_capacity(capacity))
        }
    }

    /// Check if this map is using the Small (HashMap) variant.
    pub fn is_small(&self) -> bool {
        matches!(self, Self::Small(_))
    }

    /// Check if this map is using the Large (DashMap) variant.
    pub fn is_large(&self) -> bool {
        matches!(self, Self::Large(_))
    }

    /// Returns the number of entries in the map.
    pub fn len(&self) -> usize {
        match self {
            Self::Small(map) => map.len(),
            Self::Large(map) => map.len(),
        }
    }

    /// Returns true if the map contains no entries.
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Small(map) => map.is_empty(),
            Self::Large(map) => map.is_empty(),
        }
    }

    /// Clears the map, removing all entries.
    pub fn clear(&mut self) {
        match self {
            Self::Small(map) => map.clear(),
            Self::Large(map) => map.clear(),
        }
    }
}

impl<K, V> AdaptiveMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    /// Insert a key-value pair into the map.
    ///
    /// If the map already contained this key, the old value is returned.
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        match self {
            Self::Small(map) => map.insert(key, value),
            Self::Large(map) => map.insert(key, value),
        }
    }

    /// Get a clone of the value associated with the key.
    ///
    /// Returns `None` if the key is not present.
    pub fn get(&self, key: &K) -> Option<V> {
        match self {
            Self::Small(map) => map.get(key).cloned(),
            Self::Large(map) => map.get(key).map(|v| v.value().clone()),
        }
    }

    /// Check if the map contains the given key.
    pub fn contains_key(&self, key: &K) -> bool {
        match self {
            Self::Small(map) => map.contains_key(key),
            Self::Large(map) => map.contains_key(key),
        }
    }

    /// Remove a key from the map, returning the value if it was present.
    pub fn remove(&mut self, key: &K) -> Option<V> {
        match self {
            Self::Small(map) => map.remove(key),
            Self::Large(map) => map.remove(key).map(|(_, v)| v),
        }
    }

    /// Get all keys in the map.
    ///
    /// Note: For the Large variant, this requires iterating and cloning all keys.
    pub fn keys(&self) -> Vec<K> {
        match self {
            Self::Small(map) => map.keys().cloned().collect(),
            Self::Large(map) => map.iter().map(|entry| entry.key().clone()).collect(),
        }
    }

    /// Get all values in the map.
    ///
    /// Note: This requires cloning all values.
    pub fn values(&self) -> Vec<V> {
        match self {
            Self::Small(map) => map.values().cloned().collect(),
            Self::Large(map) => map.iter().map(|entry| entry.value().clone()).collect(),
        }
    }

    /// Get all key-value pairs in the map.
    ///
    /// Note: This requires cloning all entries.
    pub fn entries(&self) -> Vec<(K, V)> {
        match self {
            Self::Small(map) => map.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            Self::Large(map) => map
                .iter()
                .map(|entry| (entry.key().clone(), entry.value().clone()))
                .collect(),
        }
    }

    /// Apply a function to the value associated with a key, if present.
    ///
    /// Returns the result of the function, or `None` if the key is not present.
    pub fn get_and_modify<F, R>(&mut self, key: &K, f: F) -> Option<R>
    where
        F: FnOnce(&mut V) -> R,
    {
        match self {
            Self::Small(map) => map.get_mut(key).map(f),
            Self::Large(map) => map.get_mut(key).map(|mut entry| f(entry.value_mut())),
        }
    }

    /// Insert a key-value pair if the key is not already present.
    ///
    /// Returns a clone of the value (either existing or newly inserted).
    pub fn entry_or_insert(&mut self, key: K, default: V) -> V {
        match self {
            Self::Small(map) => map.entry(key).or_insert(default).clone(),
            Self::Large(map) => map.entry(key).or_insert(default).clone(),
        }
    }

    /// Insert a key-value pair if the key is not already present, using a function to create the default.
    ///
    /// Returns a clone of the value (either existing or newly inserted).
    pub fn entry_or_insert_with<F>(&mut self, key: K, default: F) -> V
    where
        F: FnOnce() -> V,
    {
        match self {
            Self::Small(map) => map.entry(key).or_insert_with(default).clone(),
            Self::Large(map) => map.entry(key).or_insert_with(default).clone(),
        }
    }
}

impl<K, V> Default for AdaptiveMap<K, V>
where
    K: Eq + Hash,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> Clone for AdaptiveMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    fn clone(&self) -> Self {
        match self {
            Self::Small(map) => Self::Small(map.clone()),
            Self::Large(map) => {
                let new_map = DashMap::with_capacity(map.len());
                for entry in map.iter() {
                    new_map.insert(entry.key().clone(), entry.value().clone());
                }
                Self::Large(new_map)
            }
        }
    }
}

// Note: AdaptiveMap is Send + Sync only when using the Large variant,
// or when the Small variant is not accessed concurrently.
// The Large (DashMap) variant is inherently thread-safe.
// The Small (HashMap) variant requires external synchronization.
unsafe impl<K, V> Send for AdaptiveMap<K, V>
where
    K: Eq + Hash + Send,
    V: Send,
{
}

unsafe impl<K, V> Sync for AdaptiveMap<K, V>
where
    K: Eq + Hash + Send + Sync,
    V: Send + Sync,
{
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_creates_small() {
        let map: AdaptiveMap<String, i32> = AdaptiveMap::new();
        assert!(map.is_small());
        assert!(!map.is_large());
        assert!(map.is_empty());
    }

    #[test]
    fn test_with_capacity_below_threshold() {
        let map: AdaptiveMap<String, i32> = AdaptiveMap::with_capacity(100);
        assert!(map.is_small());
    }

    #[test]
    fn test_with_capacity_above_threshold() {
        let map: AdaptiveMap<String, i32> = AdaptiveMap::with_capacity(20_000);
        assert!(map.is_large());
    }

    #[test]
    fn test_with_capacity_threshold_below() {
        let map: AdaptiveMap<String, i32> = AdaptiveMap::with_capacity_threshold(500, 1000);
        assert!(map.is_small());
    }

    #[test]
    fn test_with_capacity_threshold_above() {
        let map: AdaptiveMap<String, i32> = AdaptiveMap::with_capacity_threshold(1500, 1000);
        assert!(map.is_large());
    }

    #[test]
    fn test_with_capacity_threshold_equal() {
        // When capacity equals threshold, should use Large
        let map: AdaptiveMap<String, i32> = AdaptiveMap::with_capacity_threshold(1000, 1000);
        assert!(map.is_large());
    }

    #[test]
    fn test_insert_and_get_small() {
        let mut map: AdaptiveMap<String, i32> = AdaptiveMap::new();
        assert!(map.insert("key1".to_string(), 42).is_none());
        assert_eq!(map.get(&"key1".to_string()), Some(42));
        assert_eq!(map.get(&"key2".to_string()), None);
    }

    #[test]
    fn test_insert_and_get_large() {
        let mut map: AdaptiveMap<String, i32> =
            AdaptiveMap::with_capacity_threshold(100, 50);
        assert!(map.is_large());
        assert!(map.insert("key1".to_string(), 42).is_none());
        assert_eq!(map.get(&"key1".to_string()), Some(42));
        assert_eq!(map.get(&"key2".to_string()), None);
    }

    #[test]
    fn test_insert_overwrites() {
        let mut map: AdaptiveMap<String, i32> = AdaptiveMap::new();
        map.insert("key".to_string(), 1);
        let old = map.insert("key".to_string(), 2);
        assert_eq!(old, Some(1));
        assert_eq!(map.get(&"key".to_string()), Some(2));
    }

    #[test]
    fn test_contains_key() {
        let mut map: AdaptiveMap<String, i32> = AdaptiveMap::new();
        map.insert("key".to_string(), 42);
        assert!(map.contains_key(&"key".to_string()));
        assert!(!map.contains_key(&"other".to_string()));
    }

    #[test]
    fn test_remove() {
        let mut map: AdaptiveMap<String, i32> = AdaptiveMap::new();
        map.insert("key".to_string(), 42);
        assert_eq!(map.remove(&"key".to_string()), Some(42));
        assert_eq!(map.remove(&"key".to_string()), None);
        assert!(!map.contains_key(&"key".to_string()));
    }

    #[test]
    fn test_len_and_is_empty() {
        let mut map: AdaptiveMap<String, i32> = AdaptiveMap::new();
        assert!(map.is_empty());
        assert_eq!(map.len(), 0);

        map.insert("key1".to_string(), 1);
        assert!(!map.is_empty());
        assert_eq!(map.len(), 1);

        map.insert("key2".to_string(), 2);
        assert_eq!(map.len(), 2);

        map.remove(&"key1".to_string());
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn test_clear() {
        let mut map: AdaptiveMap<String, i32> = AdaptiveMap::new();
        map.insert("key1".to_string(), 1);
        map.insert("key2".to_string(), 2);
        assert_eq!(map.len(), 2);

        map.clear();
        assert!(map.is_empty());
        assert_eq!(map.len(), 0);
    }

    #[test]
    fn test_keys() {
        let mut map: AdaptiveMap<String, i32> = AdaptiveMap::new();
        map.insert("a".to_string(), 1);
        map.insert("b".to_string(), 2);
        map.insert("c".to_string(), 3);

        let mut keys = map.keys();
        keys.sort();
        assert_eq!(keys, vec!["a".to_string(), "b".to_string(), "c".to_string()]);
    }

    #[test]
    fn test_values() {
        let mut map: AdaptiveMap<String, i32> = AdaptiveMap::new();
        map.insert("a".to_string(), 1);
        map.insert("b".to_string(), 2);
        map.insert("c".to_string(), 3);

        let mut values = map.values();
        values.sort();
        assert_eq!(values, vec![1, 2, 3]);
    }

    #[test]
    fn test_entries() {
        let mut map: AdaptiveMap<String, i32> = AdaptiveMap::new();
        map.insert("a".to_string(), 1);
        map.insert("b".to_string(), 2);

        let mut entries = map.entries();
        entries.sort_by_key(|(k, _)| k.clone());
        assert_eq!(
            entries,
            vec![("a".to_string(), 1), ("b".to_string(), 2)]
        );
    }

    #[test]
    fn test_get_and_modify() {
        let mut map: AdaptiveMap<String, i32> = AdaptiveMap::new();
        map.insert("key".to_string(), 10);

        let result = map.get_and_modify(&"key".to_string(), |v| {
            *v += 5;
            *v
        });
        assert_eq!(result, Some(15));
        assert_eq!(map.get(&"key".to_string()), Some(15));

        let result = map.get_and_modify(&"missing".to_string(), |v| *v);
        assert_eq!(result, None);
    }

    #[test]
    fn test_entry_or_insert() {
        let mut map: AdaptiveMap<String, i32> = AdaptiveMap::new();

        let val = map.entry_or_insert("key".to_string(), 42);
        assert_eq!(val, 42);

        let val = map.entry_or_insert("key".to_string(), 100);
        assert_eq!(val, 42); // Original value preserved
    }

    #[test]
    fn test_entry_or_insert_with() {
        let mut map: AdaptiveMap<String, i32> = AdaptiveMap::new();
        let mut called = false;

        let val = map.entry_or_insert_with("key".to_string(), || {
            called = true;
            42
        });
        assert_eq!(val, 42);
        assert!(called);

        called = false;
        let val = map.entry_or_insert_with("key".to_string(), || {
            called = true;
            100
        });
        assert_eq!(val, 42); // Original value preserved
        assert!(!called); // Function not called
    }

    #[test]
    fn test_clone_small() {
        let mut map: AdaptiveMap<String, i32> = AdaptiveMap::new();
        map.insert("key".to_string(), 42);

        let cloned = map.clone();
        assert!(cloned.is_small());
        assert_eq!(cloned.get(&"key".to_string()), Some(42));
    }

    #[test]
    fn test_clone_large() {
        let mut map: AdaptiveMap<String, i32> =
            AdaptiveMap::with_capacity_threshold(100, 50);
        map.insert("key".to_string(), 42);

        let cloned = map.clone();
        assert!(cloned.is_large());
        assert_eq!(cloned.get(&"key".to_string()), Some(42));
    }

    #[test]
    fn test_default() {
        let map: AdaptiveMap<String, i32> = AdaptiveMap::default();
        assert!(map.is_small());
        assert!(map.is_empty());
    }

    #[test]
    fn test_consistent_behavior_small_and_large() {
        // Test that both variants behave identically
        let mut small: AdaptiveMap<String, i32> = AdaptiveMap::with_capacity_threshold(10, 100);
        let mut large: AdaptiveMap<String, i32> = AdaptiveMap::with_capacity_threshold(200, 100);

        assert!(small.is_small());
        assert!(large.is_large());

        // Insert same data
        for i in 0..10 {
            small.insert(format!("key{}", i), i);
            large.insert(format!("key{}", i), i);
        }

        // Verify same behavior
        assert_eq!(small.len(), large.len());

        for i in 0..10 {
            let key = format!("key{}", i);
            assert_eq!(small.get(&key), large.get(&key));
            assert_eq!(small.contains_key(&key), large.contains_key(&key));
        }

        // Remove and verify
        small.remove(&"key5".to_string());
        large.remove(&"key5".to_string());

        assert_eq!(small.len(), large.len());
        assert_eq!(small.contains_key(&"key5".to_string()), large.contains_key(&"key5".to_string()));
    }

    #[test]
    fn test_types_are_send_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}

        assert_send::<AdaptiveMap<String, i32>>();
        assert_sync::<AdaptiveMap<String, i32>>();
    }

    #[test]
    fn test_large_variant_operations() {
        // Specifically test the Large variant with more operations
        let mut map: AdaptiveMap<i32, String> = AdaptiveMap::with_capacity_threshold(1000, 500);
        assert!(map.is_large());

        // Insert many entries
        for i in 0..100 {
            map.insert(i, format!("value_{}", i));
        }

        assert_eq!(map.len(), 100);

        // Verify all entries
        for i in 0..100 {
            assert!(map.contains_key(&i));
            assert_eq!(map.get(&i), Some(format!("value_{}", i)));
        }

        // Remove some entries
        for i in 0..50 {
            assert!(map.remove(&i).is_some());
        }

        assert_eq!(map.len(), 50);

        // Clear
        map.clear();
        assert!(map.is_empty());
    }
}
