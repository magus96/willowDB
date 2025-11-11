use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::PathBuf,
};

use serde::{Deserialize, Serialize};

use crate::wal::types::{BatchId, LSN};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalMetadata {
    pub curr_lsn: LSN,
    pub curr_batch_id: BatchId,
    pub curr_file_index: u32,
    #[serde(default = "default_magic")]
    pub magic: u32,
    #[serde(default = "default_version")]
    pub version: u8,
}

pub fn default_magic() -> u32 {
    0xABCDEF
}

pub fn default_version() -> u8 {
    1
}

impl Default for WalMetadata {
    fn default() -> Self {
        WalMetadata {
            curr_lsn: 0,
            curr_batch_id: 0,
            curr_file_index: 0,
            magic: default_magic(),
            version: default_version(),
        }
    }
}

impl WalMetadata {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn validate(&self) -> bool {
        self.magic == default_magic() && self.version == default_version()
    }

    pub fn load(metadata_path: &PathBuf) -> Result<Self, String> {
        if !metadata_path.exists() {
            return Ok(Self::default());
        }
        let mut file = File::open(metadata_path)
            .map_err(|e| format!("WAL error opening metadata file: {}", e))?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)
            .map_err(|e| format!("WAL error reading contents of metadata: {}", e))?;
        let metadata: WalMetadata = serde_json::from_str(&contents)
            .map_err(|e| format!("WAL error deserialising metadata contents: {}", e))?;

        if !metadata.validate() {
            return Err("WAL Invalid metadata".to_string());
        }
        Ok(metadata)
    }

    pub fn save(&self, metadata_path: &PathBuf) -> Result<(), String> {
        if !self.validate() {
            return Err("WAL invalid metadata".to_string());
        }
        let md_json = serde_json::to_string_pretty(self)
            .map_err(|e| format!("WAL Error serialising metadata: {}", e))?;
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(metadata_path)
            .map_err(|e| format!("WAL error saving metadata file: {}", e))?;
        file.write_all(md_json.as_bytes())
            .map_err(|e| format!("WAL error writing to file: {}", e))?;
        file.sync_all()
            .map_err(|e| format!("WAL error syncing file: {}", e))?;
        Ok(())
    }

    pub fn update_lsn(&mut self, lsn: LSN, metadata_path: &PathBuf) -> Result<(), String> {
        self.curr_lsn = lsn;
        self.save(metadata_path)
    }

    pub fn update_batch_id(
        &mut self,
        batch_id: BatchId,
        metadata_path: &PathBuf,
    ) -> Result<(), String> {
        self.curr_batch_id = batch_id;
        self.save(metadata_path)
    }

    pub fn update_file_index(
        &mut self,
        file_index: u32,
        metadata_path: &PathBuf,
    ) -> Result<(), String> {
        self.curr_file_index = file_index;
        self.save(metadata_path)
    }

    pub fn update_all(
        &mut self,
        lsn: LSN,
        batch_id: BatchId,
        file_index: u32,
        metadata_path: &PathBuf,
    ) -> Result<(), String> {
        self.curr_lsn = lsn;
        self.curr_batch_id = batch_id;
        self.curr_file_index = file_index;
        self.save(metadata_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_metadata_default() {
        let metadata = WalMetadata::default();
        assert_eq!(metadata.curr_lsn, 0);
        assert_eq!(metadata.curr_batch_id, 0);
        assert_eq!(metadata.curr_file_index, 0);
        assert_eq!(metadata.magic, default_magic());
        assert_eq!(metadata.version, 1);
    }

    #[test]
    fn test_metadata_validate() {
        let metadata = WalMetadata::default();
        assert!(metadata.validate());

        let mut invalid = metadata;
        invalid.magic = 0xDEADCAFE;
        assert!(!invalid.validate());

        let mut invalid_version = metadata;
        invalid_version.version = 99;
        assert!(!invalid_version.validate());
    }

    #[test]
    fn test_metadata_save_load() {
        let temp_dir = PathBuf::from("/tmp/test_wal_metadata");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let metadata_path = temp_dir.join("metadata.json");

        let mut metadata = WalMetadata::new();
        metadata.curr_lsn = 12345;
        metadata.curr_batch_id = 67;
        metadata.curr_file_index = 3;

        // Save
        metadata.save(&metadata_path).unwrap();
        assert!(metadata_path.exists());

        // Load
        let loaded = WalMetadata::load(&metadata_path).unwrap();
        assert_eq!(loaded.curr_lsn, 12345);
        assert_eq!(loaded.curr_batch_id, 67);
        assert_eq!(loaded.curr_file_index, 3);
        assert_eq!(loaded.magic, default_magic());
        assert_eq!(loaded.version, 1);
    }

    #[test]
    fn test_metadata_load_nonexistent() {
        let metadata_path = PathBuf::from("/tmp/nonexistent_metadata_file.json");
        let metadata = WalMetadata::load(&metadata_path).unwrap();

        assert_eq!(metadata, WalMetadata::default());
    }

    #[test]
    fn test_metadata_update_lsn() {
        let temp_dir = PathBuf::from("/tmp/test_wal_metadata_update_lsn");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let metadata_path = temp_dir.join("metadata.json");

        let mut metadata = WalMetadata::new();
        metadata.update_lsn(999, &metadata_path).unwrap();

        let loaded = WalMetadata::load(&metadata_path).unwrap();
        assert_eq!(loaded.curr_lsn, 999);
    }

    #[test]
    fn test_metadata_update_batch_id() {
        let temp_dir = PathBuf::from("/tmp/test_wal_metadata_update_batch_id");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let metadata_path = temp_dir.join("metadata.json");

        let mut metadata = WalMetadata::new();
        metadata.update_batch_id(42, &metadata_path).unwrap();

        let loaded = WalMetadata::load(&metadata_path).unwrap();
        assert_eq!(loaded.curr_batch_id, 42);
    }

    #[test]
    fn test_metadata_update_file_index() {
        let temp_dir = PathBuf::from("/tmp/test_wal_metadata_update_file_index");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let metadata_path = temp_dir.join("metadata.json");

        let mut metadata = WalMetadata::new();
        metadata.update_file_index(5, &metadata_path).unwrap();

        let loaded = WalMetadata::load(&metadata_path).unwrap();
        assert_eq!(loaded.curr_file_index, 5);
    }

    #[test]
    fn test_metadata_update_all() {
        let temp_dir = PathBuf::from("/tmp/test_wal_metadata_update_all");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let metadata_path = temp_dir.join("metadata.json");

        let mut metadata = WalMetadata::new();
        metadata.update_all(5000, 100, 7, &metadata_path).unwrap();

        let loaded = WalMetadata::load(&metadata_path).unwrap();
        assert_eq!(loaded.curr_lsn, 5000);
        assert_eq!(loaded.curr_batch_id, 100);
        assert_eq!(loaded.curr_file_index, 7);
    }

    #[test]
    fn test_metadata_json_format() {
        let temp_dir = PathBuf::from("/tmp/test_wal_metadata_json");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let metadata_path = temp_dir.join("metadata.json");

        let mut metadata = WalMetadata::new();
        metadata.curr_lsn = 100;
        metadata.curr_batch_id = 5;
        metadata.curr_file_index = 2;
        metadata.save(&metadata_path).unwrap();

        let contents = fs::read_to_string(&metadata_path).unwrap();
        assert!(contents.contains("\"curr_lsn\": 100"));
        assert!(contents.contains("\"curr_batch_id\": 5"));
        assert!(contents.contains("\"curr_file_index\": 2"));
        assert!(contents.contains("\"magic\":"));
        assert!(contents.contains("\"version\": 1"));
    }
}
