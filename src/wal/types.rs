// Types Module for the Write-Ahead-Log(WAL) for WillowDB.

use std::time::{self, UNIX_EPOCH};

pub type LSN = u64;
pub type BatchId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordType {
    Put = 1,
    Delete = 2,
    Abort = 3,
    Commit = 4,
    Checkpoint = 5,
    MemtableFlush = 6,
}

impl RecordType {
    pub fn to_byte(&self) -> u8 {
        match self {
            RecordType::Put => 1,
            RecordType::Delete => 2,
            RecordType::Abort => 3,
            RecordType::Commit => 4,
            RecordType::Checkpoint => 5,
            RecordType::MemtableFlush => 6,
        }
    }

    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            1 => Some(RecordType::Put),
            2 => Some(RecordType::Delete),
            3 => Some(RecordType::Abort),
            4 => Some(RecordType::Commit),
            5 => Some(RecordType::Checkpoint),
            6 => Some(RecordType::MemtableFlush),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub lsn: LSN,                // 8 bytes
    pub batch_id: BatchId,       // 8 bytes
    pub record_type: RecordType, // 1 byte
    pub key: Vec<u8>,            // variable
    pub value: Option<Vec<u8>>,  // variable
    pub timestamp: u64,          // 8 bytes
}

impl LogEntry {
    pub fn new_put(lsn: LSN, batch_id: BatchId, key: Vec<u8>, value: Vec<u8>) -> Self {
        LogEntry {
            lsn,
            batch_id,
            record_type: RecordType::Put,
            key,
            value: Some(value),
            timestamp: time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("IMPOSSIBLE ERROR, ABORT")
                .as_secs(),
        }
    }

    pub fn new_delete(lsn: LSN, batch_id: BatchId, key: Vec<u8>) -> Self {
        LogEntry {
            lsn,
            batch_id,
            record_type: RecordType::Delete,
            key,
            value: None,
            timestamp: time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("IMPOSSIBLE ERROR, ABORT")
                .as_secs(),
        }
    }

    pub fn new_commit(lsn: LSN, batch_id: BatchId) -> Self {
        LogEntry {
            lsn,
            batch_id,
            record_type: RecordType::Commit,
            key: Vec::new(),
            value: None,
            timestamp: time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("IMPOSSIBLE ERROR, ABORT")
                .as_secs(),
        }
    }
}
