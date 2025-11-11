use crate::wal::{
    metadata::WalMetadata,
    serializer::Serializer,
    types::{BatchId, LSN, LogEntry, RecordType},
};
use std::{
    collections::HashMap,
    fs::{self, File},
    io::Read,
    path::PathBuf,
};

#[derive(Debug, Clone)]
pub struct ReaderState {
    pub log_entries: Vec<LogEntry>,
    pub committed_batches: Vec<BatchId>,
    pub uncommitted_batches: Vec<BatchId>,
    pub last_lsn: LSN,
    pub metadata: WalMetadata,
}

pub struct WalReader {
    pub log_dir: PathBuf,
    pub metadata_path: PathBuf,
}

impl WalReader {
    pub fn new(log_dir: PathBuf) -> Self {
        let metadata_path = log_dir.join("metadata.json");
        WalReader {
            log_dir,
            metadata_path: metadata_path,
        }
    }

    pub fn recover(&self) -> Result<ReaderState, String> {
        let metadata = WalMetadata::load(&self.metadata_path)
            .map_err(|e| format!("WAL error reading metadata: {}", e))?;
        let mut entries: Vec<LogEntry> = Vec::new();
        let mut batch_status: HashMap<BatchId, bool> = HashMap::new();
        let mut last_lsn: LSN = 0;

        let log_files = self
            .get_log_files()
            .map_err(|e| format!("WAL Error unable to get log files for directory :{}", e))?;

        for file in log_files {
            self.read_log_file(&file, &mut entries, &mut batch_status, &mut last_lsn)?;
        }

        let committed_batches: Vec<BatchId> = batch_status
            .iter()
            .filter(|(_, committed)| **committed)
            .map(|(batch_id, _)| *batch_id)
            .collect();

        let uncommitted_batches: Vec<BatchId> = batch_status
            .iter()
            .filter(|(_, committed)| !**committed)
            .map(|(batch_id, _)| *batch_id)
            .collect();
        Ok(ReaderState {
            log_entries: entries,
            committed_batches: committed_batches,
            uncommitted_batches: uncommitted_batches,
            last_lsn: last_lsn,
            metadata: metadata,
        })
    }

    pub fn get_log_files(&self) -> Result<Vec<PathBuf>, String> {
        let mut log_files: Vec<(u32, PathBuf)> = Vec::new();

        if !self.log_dir.exists() {
            return Ok(log_files.into_iter().map(|(_, path)| path).collect());
        }

        let entries = fs::read_dir(&self.log_dir)
            .map_err(|e| format!("WAL failed to read log directory: {}", e))?;

        for entry in entries {
            let entry =
                entry.map_err(|e| format!("WAL Error getting log directory entry: {}", e))?;
            let path = entry.path();

            if !path.is_file() {
                continue;
            }

            if let Some(fname) = path.file_name().and_then(|n| n.to_str()) {
                if fname.starts_with("log.") {
                    if let Some(index_str) = fname.strip_prefix("log.") {
                        if let Ok(index) = index_str.parse::<u32>() {
                            log_files.push((index, path));
                        }
                    }
                }
            }
        }
        log_files.sort_by_key(|(index, _)| *index);

        Ok(log_files.into_iter().map(|(_, path)| path).collect())
    }

    pub fn read_log_file(
        &self,
        log_path: &PathBuf,
        entries: &mut Vec<LogEntry>,
        batch_status: &mut HashMap<BatchId, bool>,
        last_lsn: &mut LSN,
    ) -> Result<(), String> {
        let mut file = File::open(log_path)
            .map_err(|e| format!("WAL error opening log file to read entry: {}", e))?;

        let mut buf = Vec::new();

        file.read_to_end(&mut buf)
            .map_err(|e| format!("WAL error reading log file: {}", e))?;

        let mut offset = 0;

        while offset < buf.len() {
            match self.try_read_entry(&buf[offset..]) {
                Ok((entry, size)) => {
                    match entry.record_type {
                        RecordType::Commit => {
                            batch_status.insert(entry.batch_id, true);
                        }
                        RecordType::Put | RecordType::Delete => {
                            batch_status.entry(entry.batch_id).or_insert(false);
                        }
                        RecordType::Abort => {
                            batch_status.insert(entry.batch_id, false);
                        }
                        _ => {}
                    }
                    if entry.lsn > *last_lsn {
                        *last_lsn = entry.lsn;
                    }

                    offset += size;
                    entries.push(entry);
                }

                Err(_) => {
                    break;
                }
            }
        }

        Ok(())
    }

    pub fn try_read_entry(&self, data: &[u8]) -> Result<(LogEntry, usize), String> {
        let log_entry = Serializer::deserialize(data)
            .map_err(|e| format!("WAL Error deserialising while reading log file: {}", e))?;

        let data = Serializer::serialize(&log_entry)
            .map_err(|e| format!("WAL Error serialising while reading log file: {}", e))?;

        Ok((log_entry, data.len()))
    }
    pub fn get_batch_entries(
        &self,
        recovery_state: &ReaderState,
        batch_id: BatchId,
    ) -> Vec<LogEntry> {
        recovery_state
            .log_entries
            .iter()
            .filter(|e| e.batch_id == batch_id)
            .cloned()
            .collect()
    }

    /// Check if a batch was committed
    pub fn is_batch_committed(&self, recovery_state: &ReaderState, batch_id: BatchId) -> bool {
        recovery_state.committed_batches.contains(&batch_id)
    }

    /// Get all entries that should be replayed (committed batches only, excluding commit records)
    pub fn get_committed_entries(&self, recovery_state: &ReaderState) -> Vec<LogEntry> {
        recovery_state
            .log_entries
            .iter()
            .filter(|e| {
                recovery_state.committed_batches.contains(&e.batch_id)
                    && e.record_type != RecordType::Commit
            })
            .cloned()
            .collect()
    }

    /// Validate recovery state against metadata
    pub fn validate_recovery(&self, recovery_state: &ReaderState) -> Result<(), String> {
        // The last LSN read should match or be greater than metadata LSN
        // (greater if there are uncommitted entries after the last commit)
        if recovery_state.last_lsn < recovery_state.metadata.curr_lsn {
            return Err(format!(
                "Recovery LSN mismatch: log contains LSN {}, but metadata has {}",
                recovery_state.last_lsn, recovery_state.metadata.curr_lsn
            ));
        }

        // Validate metadata integrity
        if !recovery_state.metadata.validate() {
            return Err("Metadata validation failed: magic or version mismatch".to_string());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::writer::WalWriter;
    use std::fs;

    fn setup_test_dir(name: &str) -> PathBuf {
        let dir = PathBuf::from(format!("/tmp/wal_reader_test_{}", name));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn test_recover_empty() {
        let log_dir = setup_test_dir("recover_empty");

        let reader = WalReader::new(log_dir);
        let recovery = reader.recover().unwrap();

        assert_eq!(recovery.log_entries.len(), 0);
        assert_eq!(recovery.committed_batches.len(), 0);
        assert_eq!(recovery.uncommitted_batches.len(), 0);
        assert_eq!(recovery.last_lsn, 0);
    }

    #[test]
    fn test_recover_single_committed_batch() {
        let log_dir = setup_test_dir("recover_single_committed");

        {
            let mut writer = WalWriter::new(log_dir.clone(), 100 * 1024 * 1024).unwrap();
            writer.begin_batch().unwrap();
            writer
                .write_put(b"key1".to_vec(), b"value1".to_vec())
                .unwrap();
            writer
                .write_put(b"key2".to_vec(), b"value2".to_vec())
                .unwrap();
            writer.commit().unwrap();
            writer.fsync().unwrap();
        }

        let reader = WalReader::new(log_dir);
        let recovery = reader.recover().unwrap();

        assert_eq!(recovery.log_entries.len(), 3); // 2 puts + 1 commit
        assert_eq!(recovery.committed_batches.len(), 1);
        assert_eq!(recovery.committed_batches[0], 1);
        assert_eq!(recovery.uncommitted_batches.len(), 0);
        assert_eq!(recovery.last_lsn, 3);
    }

    #[test]
    fn test_recover_uncommitted_batch() {
        let log_dir = setup_test_dir("recover_uncommitted");

        {
            let mut writer = WalWriter::new(log_dir.clone(), 100 * 1024 * 1024).unwrap();
            writer.begin_batch().unwrap();
            writer
                .write_put(b"key1".to_vec(), b"value1".to_vec())
                .unwrap();
            // No commit - batch is uncommitted
            writer.fsync().unwrap();
        }

        let reader = WalReader::new(log_dir);
        let recovery = reader.recover().unwrap();

        assert_eq!(recovery.log_entries.len(), 1); // 1 put, no commit
        assert_eq!(recovery.committed_batches.len(), 0);
        assert_eq!(recovery.uncommitted_batches.len(), 1);
        assert_eq!(recovery.uncommitted_batches[0], 1);
    }

    #[test]
    fn test_recover_multiple_batches_mixed() {
        let log_dir = setup_test_dir("recover_multiple_mixed");

        {
            let mut writer = WalWriter::new(log_dir.clone(), 100 * 1024 * 1024).unwrap();

            // Batch 1 - committed
            writer.begin_batch().unwrap();
            writer
                .write_put(b"key1".to_vec(), b"value1".to_vec())
                .unwrap();
            writer.commit().unwrap();

            // Batch 2 - uncommitted
            writer.begin_batch().unwrap();
            writer
                .write_put(b"key2".to_vec(), b"value2".to_vec())
                .unwrap();
            writer.write_delete(b"key1".to_vec()).unwrap();

            // Batch 3 - committed
            writer.begin_batch().unwrap();
            writer
                .write_put(b"key3".to_vec(), b"value3".to_vec())
                .unwrap();
            writer.commit().unwrap();

            writer.fsync().unwrap();
        }

        let reader = WalReader::new(log_dir);
        let recovery = reader.recover().unwrap();

        assert_eq!(recovery.log_entries.len(), 6); // put, commit, put, delete, put, commit
        assert_eq!(recovery.committed_batches.len(), 2);
        assert_eq!(recovery.uncommitted_batches.len(), 1);
        assert!(recovery.committed_batches.contains(&1));
        assert!(recovery.committed_batches.contains(&3));
        assert!(recovery.uncommitted_batches.contains(&2));
    }

    #[test]
    fn test_get_batch_entries() {
        let log_dir = setup_test_dir("get_batch_entries");

        {
            let mut writer = WalWriter::new(log_dir.clone(), 100 * 1024 * 1024).unwrap();

            // Batch 1
            writer.begin_batch().unwrap();
            writer
                .write_put(b"key1".to_vec(), b"value1".to_vec())
                .unwrap();
            writer
                .write_put(b"key2".to_vec(), b"value2".to_vec())
                .unwrap();
            writer.commit().unwrap();

            // Batch 2
            writer.begin_batch().unwrap();
            writer
                .write_put(b"key3".to_vec(), b"value3".to_vec())
                .unwrap();
            writer.commit().unwrap();

            writer.fsync().unwrap();
        }

        let reader = WalReader::new(log_dir);
        let recovery = reader.recover().unwrap();

        let batch1_entries = reader.get_batch_entries(&recovery, 1);
        assert_eq!(batch1_entries.len(), 3); // 2 puts + 1 commit

        let batch2_entries = reader.get_batch_entries(&recovery, 2);
        assert_eq!(batch2_entries.len(), 2); // 1 put + 1 commit

        let batch3_entries = reader.get_batch_entries(&recovery, 3);
        assert_eq!(batch3_entries.len(), 0); // doesn't exist
    }

    #[test]
    fn test_is_batch_committed() {
        let log_dir = setup_test_dir("is_batch_committed");

        {
            let mut writer = WalWriter::new(log_dir.clone(), 100 * 1024 * 1024).unwrap();

            writer.begin_batch().unwrap();
            writer
                .write_put(b"key1".to_vec(), b"value1".to_vec())
                .unwrap();
            writer.commit().unwrap();

            writer.begin_batch().unwrap();
            writer
                .write_put(b"key2".to_vec(), b"value2".to_vec())
                .unwrap();
            // No commit

            writer.fsync().unwrap();
        }

        let reader = WalReader::new(log_dir);
        let recovery = reader.recover().unwrap();

        assert!(reader.is_batch_committed(&recovery, 1));
        assert!(!reader.is_batch_committed(&recovery, 2));
        assert!(!reader.is_batch_committed(&recovery, 99)); // doesn't exist
    }

    #[test]
    fn test_get_committed_entries() {
        let log_dir = setup_test_dir("get_committed_entries");

        {
            let mut writer = WalWriter::new(log_dir.clone(), 100 * 1024 * 1024).unwrap();

            // Batch 1 - committed
            writer.begin_batch().unwrap();
            writer
                .write_put(b"key1".to_vec(), b"value1".to_vec())
                .unwrap();
            writer
                .write_put(b"key2".to_vec(), b"value2".to_vec())
                .unwrap();
            writer.commit().unwrap();

            // Batch 2 - uncommitted
            writer.begin_batch().unwrap();
            writer
                .write_put(b"key3".to_vec(), b"value3".to_vec())
                .unwrap();
            writer.write_delete(b"key1".to_vec()).unwrap();

            writer.fsync().unwrap();
        }

        let reader = WalReader::new(log_dir);
        let recovery = reader.recover().unwrap();

        let committed = reader.get_committed_entries(&recovery);

        // Should only have PUT entries from batch 1 (not COMMIT record, not batch 2)
        assert_eq!(committed.len(), 2);
        assert!(committed.iter().all(|e| e.batch_id == 1));
        assert!(committed.iter().all(|e| e.record_type == RecordType::Put));
    }

    #[test]
    fn test_get_log_files_sorted() {
        let log_dir = setup_test_dir("get_log_files_sorted");

        {
            let mut writer = WalWriter::new(log_dir.clone(), 512).unwrap(); // Very small size to force rotation

            // Write with larger values to trigger rotations
            for i in 0..20 {
                writer.begin_batch().unwrap();
                // Create larger values to exceed 512 byte limit faster
                let large_value = vec![0xAB; 100];
                writer
                    .write_put(format!("key{}", i).into_bytes(), large_value)
                    .unwrap();
                writer.commit().unwrap();
                writer.fsync().unwrap();
            }
        }

        let reader = WalReader::new(log_dir);
        let log_files = reader.get_log_files().unwrap();

        // Should have multiple log files due to small 512 byte limit
        assert!(
            log_files.len() > 1,
            "Expected multiple log files, got {}",
            log_files.len()
        );

        // Verify they're in order and named correctly
        for file in log_files.iter() {
            let fname = file.file_name().unwrap().to_str().unwrap();
            assert!(fname.starts_with("log."));
        }
    }

    #[test]
    fn test_validate_recovery_success() {
        let log_dir = setup_test_dir("validate_recovery_success");

        {
            let mut writer = WalWriter::new(log_dir.clone(), 100 * 1024 * 1024).unwrap();
            writer.begin_batch().unwrap();
            writer
                .write_put(b"key".to_vec(), b"value".to_vec())
                .unwrap();
            writer.commit().unwrap();
            writer.fsync().unwrap();
        }

        let reader = WalReader::new(log_dir);
        let recovery = reader.recover().unwrap();

        assert!(reader.validate_recovery(&recovery).is_ok());
    }

    #[test]
    fn test_validate_recovery_metadata_matches_lsn() {
        let log_dir = setup_test_dir("validate_recovery_metadata_matches_lsn");

        {
            let mut writer = WalWriter::new(log_dir.clone(), 100 * 1024 * 1024).unwrap();
            writer.begin_batch().unwrap();
            writer
                .write_put(b"key1".to_vec(), b"value1".to_vec())
                .unwrap();
            writer
                .write_put(b"key2".to_vec(), b"value2".to_vec())
                .unwrap();
            writer
                .write_put(b"key3".to_vec(), b"value3".to_vec())
                .unwrap();
            writer.commit().unwrap();
            writer.fsync().unwrap();
        }

        let reader = WalReader::new(log_dir);
        let recovery = reader.recover().unwrap();

        // Metadata should have LSN = 4 (3 puts + 1 commit)
        assert_eq!(recovery.metadata.curr_lsn, 4);
        // Last LSN read should match
        assert_eq!(recovery.last_lsn, 4);

        assert!(reader.validate_recovery(&recovery).is_ok());
    }

    #[test]
    fn test_metadata_persistence_across_recovery() {
        let log_dir = setup_test_dir("metadata_persistence");

        // First session: write and commit
        {
            let mut writer = WalWriter::new(log_dir.clone(), 100 * 1024 * 1024).unwrap();
            writer.begin_batch().unwrap();
            writer
                .write_put(b"key1".to_vec(), b"value1".to_vec())
                .unwrap();
            writer.commit().unwrap();
            writer.fsync().unwrap();

            assert_eq!(writer.curr_lsn(), 2);
            assert_eq!(writer.curr_batch_id(), 1);
        }

        // Recovery and verify state
        let reader = WalReader::new(log_dir.clone());
        let recovery = reader.recover().unwrap();

        assert_eq!(recovery.metadata.curr_lsn, 2);
        assert_eq!(recovery.metadata.curr_batch_id, 1);
    }

    #[test]
    fn test_uncommitted_entries_excluded_from_replay() {
        let log_dir = setup_test_dir("uncommitted_excluded");

        {
            let mut writer = WalWriter::new(log_dir.clone(), 100 * 1024 * 1024).unwrap();

            // Batch 1: committed
            writer.begin_batch().unwrap();
            writer
                .write_put(b"committed_key".to_vec(), b"committed_value".to_vec())
                .unwrap();
            writer.commit().unwrap();

            // Batch 2: uncommitted (crash)
            writer.begin_batch().unwrap();
            writer
                .write_put(b"lost_key".to_vec(), b"lost_value".to_vec())
                .unwrap();
            writer.fsync().unwrap(); // fsync but no commit
        }

        let reader = WalReader::new(log_dir);
        let recovery = reader.recover().unwrap();

        let to_replay = reader.get_committed_entries(&recovery);

        // Should only have 1 entry (the committed put), not the lost one
        assert_eq!(to_replay.len(), 1);
        assert_eq!(to_replay[0].key, b"committed_key");
    }

    #[test]
    fn test_try_read_entry_deterministic_size() {
        let log_dir = setup_test_dir("try_read_entry_size");

        let entry = LogEntry::new_put(1, 1, b"test_key".to_vec(), b"test_value".to_vec());
        let serialized = Serializer::serialize(&entry).unwrap();

        let reader = WalReader::new(log_dir);
        let (read_entry, size) = reader.try_read_entry(&serialized).unwrap();

        // Re-serializing should give same size
        assert_eq!(size, serialized.len());
        // Entry should be identical
        assert_eq!(read_entry, entry);
    }

    #[test]
    fn test_multiple_log_files_recovery() {
        let log_dir = setup_test_dir("multiple_log_files");

        {
            let mut writer = WalWriter::new(log_dir.clone(), 512).unwrap(); // Very small to force rotations

            // Write across multiple files
            for batch_num in 1..=5 {
                writer.begin_batch().unwrap();
                writer
                    .write_put(
                        format!("key{}", batch_num).into_bytes(),
                        format!("value{}", batch_num).into_bytes(),
                    )
                    .unwrap();
                writer.commit().unwrap();
                writer.fsync().unwrap();
            }
        }

        let reader = WalReader::new(log_dir);
        let recovery = reader.recover().unwrap();

        // Should have recovered all 5 batches
        assert_eq!(recovery.committed_batches.len(), 5);
        assert_eq!(recovery.uncommitted_batches.len(), 0);
        // Should have 5 batches * 2 entries (put + commit) = 10 entries
        assert_eq!(recovery.log_entries.len(), 10);
    }
}
