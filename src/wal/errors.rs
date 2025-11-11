use std::io;

pub type Result<T> = std::result::Result<T, WalError>;

#[derive(Debug)]
pub enum WalError {
    IOError(io::Error),

    CorruptedRecord(String),

    InvalidRecordType(u8),

    ChecksumMismatch { expected: u32, actual: u32 },

    LogFileNotFound(String),

    InvalidLogFile(String),

    WriteError(String),

    ReadError(String),

    SerializationError(String),

    NoCheckpoint,

    InvalidLSN(u64),
}

impl std::fmt::Display for WalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalError::IOError(err) => {
                write!(f, "WAL IO Error :{}", err)
            }
            WalError::CorruptedRecord(msg) => {
                write!(f, "Corrupted WAL Record :{}", msg)
            }
            WalError::InvalidRecordType(byte) => {
                write!(f, "Invalid WAL record type: 0x{:02X}", byte)
            }
            WalError::ChecksumMismatch { expected, actual } => {
                write!(
                    f,
                    "WAL record checksum mismatch, expected: 0x{:08X}, got 0x{:08X}",
                    expected, actual
                )
            }
            WalError::LogFileNotFound(path) => {
                write!(f, "WAL Log file not found :{}", path)
            }
            WalError::InvalidLogFile(msg) => {
                write!(f, "Invalid LOG File :{}", msg)
            }
            WalError::WriteError(msg) => {
                write!(f, "WAL Write Error :{}", msg)
            }
            WalError::ReadError(msg) => {
                write!(f, "WAL Read Error :{}", msg)
            }
            WalError::SerializationError(msg) => {
                write!(f, "WAL Record serialization error: {}", msg)
            }
            WalError::NoCheckpoint => {
                write!(f, "WAL Checkpoint not found")
            }
            WalError::InvalidLSN(lsn) => {
                write!(f, "WAL Invalid LSN: {}", lsn)
            }
        }
    }
}

impl std::error::Error for WalError {}

impl From<io::Error> for WalError {
    fn from(value: io::Error) -> Self {
        WalError::IOError(value)
    }
}
