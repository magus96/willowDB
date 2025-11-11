use crate::wal::{
    metadata::WalMetadata,
    serializer::Serializer,
    types::{BatchId, LSN, LogEntry},
};
use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::PathBuf,
};

pub struct WalWriter {
    log_dir: PathBuf,
    curr_file: Option<File>,
    metadata_path: PathBuf,
    metadata: WalMetadata,
    curr_file_index: u32,
    max_file_size: u64,
    curr_file_size: u64,
    curr_lsn: LSN,
    curr_batch_id: BatchId,
    active_batch_id: Option<BatchId>,
    write_buffer: Vec<u8>,
}

impl WalWriter {
    pub fn new(log_dir: PathBuf, max_file_size: u64) -> Result<Self, String> {
        std::fs::create_dir_all(&log_dir).unwrap();

        // IMPORTANT ADD WAL METADATA
        let metadata_path = log_dir.join("metadata.json");
        let metadata = WalMetadata::load(&metadata_path)?;

        Ok(WalWriter {
            log_dir,
            curr_file: None,
            metadata_path: metadata_path,
            metadata: metadata,
            curr_file_index: metadata.curr_file_index,
            max_file_size,
            curr_file_size: 0,
            curr_lsn: metadata.curr_lsn,
            curr_batch_id: metadata.curr_batch_id,
            active_batch_id: None,
            write_buffer: Vec::new(),
        })
    }

    pub fn next_lsn(&mut self) -> LSN {
        self.curr_lsn += 1;
        self.curr_lsn
    }

    pub fn next_batch_id(&mut self) -> BatchId {
        self.curr_batch_id += 1;
        self.curr_batch_id
    }

    pub fn ensure_log_file(&mut self) -> Result<(), ()> {
        if self.curr_file.is_some() {
            return Ok(());
        }

        let path = self.log_dir.join(format!("log.{}", self.curr_file_index));
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        self.curr_file = Some(file);
        Ok(())
    }

    pub fn rotate_if_needed(&mut self) -> Result<(), ()> {
        if self.curr_file_size >= self.max_file_size {
            self.curr_file_size = 0;
            self.curr_file = None;
            self.curr_file_index += 1;
            self.metadata.curr_file_index = self.curr_file_index;
            let _ = self.metadata.save(&self.metadata_path);
        }
        Ok(())
    }

    pub fn write_entry(&mut self, entry: LogEntry) -> Result<(), ()> {
        self.ensure_log_file()?;
        let serialized = Serializer::serialize(&entry).unwrap();
        self.write_buffer.extend_from_slice(&serialized);
        self.curr_file_size += serialized.len() as u64;

        self.rotate_if_needed()?;
        Ok(())
    }

    pub fn write_put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<LSN, ()> {
        let lsn = self.next_lsn();
        let batch_id = self.active_batch_id.ok_or(())?;
        let put_entry = LogEntry::new_put(lsn, batch_id, key, value);
        self.write_entry(put_entry)?;

        Ok(lsn)
    }

    pub fn write_delete(&mut self, key: Vec<u8>) -> Result<LSN, ()> {
        let lsn = self.next_lsn();
        let batch_id = self.active_batch_id.ok_or(())?;
        let delete_entry = LogEntry::new_delete(lsn, batch_id, key);
        self.write_entry(delete_entry)?;

        Ok(lsn)
    }

    pub fn begin_batch(&mut self) -> Result<BatchId, ()> {
        self.curr_batch_id += 1;
        self.active_batch_id = Some(self.curr_batch_id);
        Ok(self.curr_batch_id)
    }

    pub fn commit(&mut self) -> Result<LSN, ()> {
        let batch_id = self.active_batch_id.ok_or(())?;
        let lsn = self.next_lsn();
        let commit_entry = LogEntry::new_commit(lsn, batch_id);
        self.write_entry(commit_entry)?;
        self.active_batch_id = None;
        self.fsync()?;
        self.metadata.curr_lsn = self.curr_lsn;
        self.metadata.curr_batch_id = self.curr_batch_id;
        self.metadata.curr_file_index = self.curr_file_index;
        let _ = self.metadata.save(&self.metadata_path);
        Ok(lsn)
    }

    pub fn flush(&mut self) -> Result<(), ()> {
        if let Some(ref mut file) = self.curr_file {
            file.write_all(&self.write_buffer).unwrap();
            self.write_buffer.clear();
        }
        Ok(())
    }

    pub fn fsync(&mut self) -> Result<(), ()> {
        self.flush()?;
        if let Some(ref mut file) = self.curr_file {
            file.sync_all().unwrap();
        }
        Ok(())
    }

    pub fn curr_lsn(&self) -> LSN {
        self.curr_lsn
    }

    pub fn curr_batch_id(&self) -> BatchId {
        self.curr_batch_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_new() {
        let log_dir = PathBuf::from("/tmp/test_wal");
        let _ = std::fs::remove_dir_all(&log_dir);

        let result = WalWriter::new(log_dir, 100 * 1024 * 1024);
        assert!(result.is_ok());
    }

    #[test]
    fn test_begin_batch() {
        let log_dir = PathBuf::from("/tmp/test_wal_batch");
        let _ = std::fs::remove_dir_all(&log_dir);

        let mut writer = WalWriter::new(log_dir, 100 * 1024 * 1024).unwrap();
        let result = writer.begin_batch();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);
    }

    #[test]
    fn test_write_without_batch() {
        let log_dir = PathBuf::from("/tmp/test_wal_no_batch");
        let _ = std::fs::remove_dir_all(&log_dir);

        let mut writer = WalWriter::new(log_dir, 100 * 1024 * 1024).unwrap();

        // Should fail - no batch
        let result = writer.write_put(b"key".to_vec(), b"value".to_vec());
        assert!(result.is_err());
    }

    #[test]
    fn test_write_with_batch() {
        let log_dir = PathBuf::from("/tmp/test_wal_with_batch");
        let _ = std::fs::remove_dir_all(&log_dir);

        let mut writer = WalWriter::new(log_dir, 100 * 1024 * 1024).unwrap();

        writer.begin_batch().unwrap();
        let result = writer.write_put(b"key".to_vec(), b"value".to_vec());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);
    }

    #[test]
    fn test_fsync() {
        let log_dir = PathBuf::from("/tmp/test_wal_fsync");
        let _ = std::fs::remove_dir_all(&log_dir);

        let mut writer = WalWriter::new(log_dir.clone(), 100 * 1024 * 1024).unwrap();

        writer.begin_batch().unwrap();
        writer
            .write_put(b"key".to_vec(), b"value".to_vec())
            .unwrap();
        writer.commit().unwrap();
        writer.fsync().unwrap();

        // Verify file exists
        let log_file = log_dir.join("log.0");
        assert!(log_file.exists());
    }
}
