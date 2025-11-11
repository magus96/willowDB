use std::collections::BTreeMap;

pub struct Memtable {
    data: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

impl Memtable {
    pub fn new() -> Self {
        Memtable {
            data: BTreeMap::new(),
        }
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.data.insert(key, Some(value));
    }

    pub fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        self.data.get(key).and_then(|v| v.clone())
    }

    pub fn delete(&mut self, key: Vec<u8>) {
        self.data.insert(key, None);
    }

    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.data.get(key).map(|v| v.is_some()).unwrap_or(false)
    }

    pub fn iter(&self) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        self.data
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    pub fn range(&self, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.data
            .range(start.to_vec()..end.to_vec())
            .filter_map(|(k, v)| v.as_ref().map(|value| (k.clone(), value.clone())))
            .collect()
    }

    pub fn snapshot(&self) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        self.iter()
    }

    pub fn clear(&mut self) {
        self.data.clear();
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    pub fn live_entries(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.data
            .iter()
            .filter_map(|(k, v)| v.as_ref().map(|value| (k.clone(), value.clone())))
            .collect()
    }

    pub fn deleted_keys(&self) -> Vec<Vec<u8>> {
        self.data
            .iter()
            .filter_map(|(k, v)| if v.is_none() { Some(k.clone()) } else { None })
            .collect()
    }
}

impl Default for Memtable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_memtable() {
        let memtable = Memtable::new();
        assert!(memtable.is_empty());
        assert_eq!(memtable.len(), 0);
    }

    #[test]
    fn test_put_and_get() {
        let mut memtable = Memtable::new();

        memtable.put(b"key1".to_vec(), b"value1".to_vec());
        assert_eq!(memtable.get(b"key1"), Some(b"value1".to_vec()));
    }

    #[test]
    fn test_put_overwrite() {
        let mut memtable = Memtable::new();

        memtable.put(b"key1".to_vec(), b"value1".to_vec());
        memtable.put(b"key1".to_vec(), b"value2".to_vec());

        assert_eq!(memtable.get(b"key1"), Some(b"value2".to_vec()));
        assert_eq!(memtable.len(), 1);
    }

    #[test]
    fn test_get_nonexistent() {
        let mut memtable = Memtable::new();
        assert_eq!(memtable.get(b"nonexistent"), None);
    }

    #[test]
    fn test_delete() {
        let mut memtable = Memtable::new();

        memtable.put(b"key1".to_vec(), b"value1".to_vec());
        assert_eq!(memtable.get(b"key1"), Some(b"value1".to_vec()));

        memtable.delete(b"key1".to_vec());
        assert_eq!(memtable.get(b"key1"), None);
        // Tombstone still exists in internal storage
        assert_eq!(memtable.len(), 1);
    }

    #[test]
    fn test_contains_key() {
        let mut memtable = Memtable::new();

        memtable.put(b"key1".to_vec(), b"value1".to_vec());
        assert!(memtable.contains_key(b"key1"));
        assert!(!memtable.contains_key(b"key2"));

        memtable.delete(b"key1".to_vec());
        assert!(!memtable.contains_key(b"key1")); // Deleted
    }

    #[test]
    fn test_iter_sorted() {
        let mut memtable = Memtable::new();

        memtable.put(b"zebra".to_vec(), b"z".to_vec());
        memtable.put(b"apple".to_vec(), b"a".to_vec());
        memtable.put(b"banana".to_vec(), b"b".to_vec());

        let entries = memtable.iter();

        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].0, b"apple");
        assert_eq!(entries[1].0, b"banana");
        assert_eq!(entries[2].0, b"zebra");
    }

    #[test]
    fn test_range_query() {
        let mut memtable = Memtable::new();

        for i in 0..10 {
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            memtable.put(key, value);
        }

        let range = memtable.range(b"key3", b"key7");

        assert_eq!(range.len(), 4); // key3, key4, key5, key6
        assert_eq!(range[0].0, b"key3");
        assert_eq!(range[3].0, b"key6");
    }

    #[test]
    fn test_range_with_deletions() {
        let mut memtable = Memtable::new();

        memtable.put(b"key1".to_vec(), b"value1".to_vec());
        memtable.put(b"key2".to_vec(), b"value2".to_vec());
        memtable.put(b"key3".to_vec(), b"value3".to_vec());

        memtable.delete(b"key2".to_vec());

        let range = memtable.range(b"key1", b"key4");

        // Should exclude deleted key2
        assert_eq!(range.len(), 2);
        assert_eq!(range[0].0, b"key1");
        assert_eq!(range[1].0, b"key3");
    }

    #[test]
    fn test_snapshot() {
        let mut memtable = Memtable::new();

        memtable.put(b"key1".to_vec(), b"value1".to_vec());
        memtable.put(b"key2".to_vec(), b"value2".to_vec());
        memtable.delete(b"key3".to_vec());

        let snapshot = memtable.snapshot();

        // Snapshot includes tombstones
        assert_eq!(snapshot.len(), 3);
    }

    #[test]
    fn test_clear() {
        let mut memtable = Memtable::new();

        memtable.put(b"key1".to_vec(), b"value1".to_vec());
        memtable.put(b"key2".to_vec(), b"value2".to_vec());
        assert_eq!(memtable.len(), 2);

        memtable.clear();
        assert!(memtable.is_empty());
        assert_eq!(memtable.len(), 0);
    }

    #[test]
    fn test_estimated_size() {
        let mut memtable = Memtable::new();

        let size_empty = memtable.estimated_size();

        memtable.put(b"key1".to_vec(), b"value1".to_vec());
        let size_one = memtable.estimated_size();

        // Size should increase after adding an entry
        assert!(
            size_one > size_empty,
            "Size should increase after adding entry"
        );

        memtable.put(b"key2".to_vec(), b"value2".to_vec());
        let size_two = memtable.estimated_size();

        assert!(
            size_two > size_one,
            "Size should increase after adding another entry"
        );
    }

    #[test]
    fn test_live_entries() {
        let mut memtable = Memtable::new();

        memtable.put(b"key1".to_vec(), b"value1".to_vec());
        memtable.put(b"key2".to_vec(), b"value2".to_vec());
        memtable.delete(b"key3".to_vec());

        let live = memtable.live_entries();

        // Should only have 2 live entries (key1, key2)
        assert_eq!(live.len(), 2);
    }

    #[test]
    fn test_deleted_keys() {
        let mut memtable = Memtable::new();

        memtable.put(b"key1".to_vec(), b"value1".to_vec());
        memtable.delete(b"key2".to_vec());
        memtable.delete(b"key3".to_vec());

        let deleted = memtable.deleted_keys();

        assert_eq!(deleted.len(), 2);
        assert!(deleted.contains(&b"key2".to_vec()));
        assert!(deleted.contains(&b"key3".to_vec()));
    }

    #[test]
    fn test_large_memtable() {
        let mut memtable = Memtable::new();

        // Insert 1000 entries
        for i in 0..1000 {
            let key = format!("key{:04}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            memtable.put(key, value);
        }

        assert_eq!(memtable.len(), 1000);

        // Verify ordering
        let iter = memtable.iter();
        for i in 0..999 {
            assert!(iter[i].0 < iter[i + 1].0);
        }

        // Verify get still works
        assert_eq!(memtable.get(b"key0500"), Some(b"value500".to_vec()));
    }

    #[test]
    fn test_multiple_operations() {
        let mut memtable = Memtable::new();

        memtable.put(b"a".to_vec(), b"1".to_vec());
        memtable.put(b"b".to_vec(), b"2".to_vec());
        memtable.put(b"c".to_vec(), b"3".to_vec());

        assert_eq!(memtable.get(b"b"), Some(b"2".to_vec()));

        memtable.delete(b"b".to_vec());
        assert_eq!(memtable.get(b"b"), None);

        memtable.put(b"b".to_vec(), b"2_new".to_vec());
        assert_eq!(memtable.get(b"b"), Some(b"2_new".to_vec()));
    }
}
