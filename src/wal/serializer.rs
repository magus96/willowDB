use super::errors::{Result, WalError};
use super::types::{LogEntry, RecordType};
use crc32fast::Hasher;

pub struct Serializer;

impl Serializer {
    const MAGIC: u16 = 0xCDEF;
    const VERSION: u8 = 1;

    pub fn serialize(entry: &LogEntry) -> Result<Vec<u8>> {
        let mut buf = Vec::new();

        // HEADER(29 bytes fixed)

        // MAGIC(2 bytes)
        buf.extend_from_slice(&Self::MAGIC.to_le_bytes());

        // VERSION(1 byte)
        buf.extend_from_slice(&Self::VERSION.to_le_bytes());

        // LSN(8 bytes)
        buf.extend_from_slice(&entry.lsn.to_le_bytes());

        // BATCH ID(8 bytes)
        buf.extend_from_slice(&entry.batch_id.to_le_bytes());

        // RECORD TYPE(1 byte)
        buf.push(entry.record_type.to_byte());

        // TIMESTAMP(8 bytes)
        buf.extend_from_slice(&entry.timestamp.to_le_bytes());

        // FLAG(1 byte) for future use
        buf.push(0);

        //BODY (VARIABLE LENGTH)

        // KEY(Variable length)
        Self::encode_varint(&mut buf, entry.key.len() as u64);
        buf.extend_from_slice(&entry.key);

        // Value(Variable length)
        if let Some(ref value) = entry.value {
            Self::encode_varint(&mut buf, value.len() as u64);
            buf.extend_from_slice(value);
        } else {
            Self::encode_varint(&mut buf, 0);
        }

        // FOOTER ( 12 bytes)

        // Compute checksum of data(4 bytes)
        let crc = Self::compute_checksum(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());

        // Frame length(4 bytes)
        let frame_len = buf.len() as u32;
        buf.extend_from_slice(&frame_len.to_le_bytes());

        // Reserved(4 bytes, zero for now)
        buf.extend_from_slice(&0u32.to_le_bytes());

        Ok(buf)
    }

    pub fn deserialize(data: &[u8]) -> Result<LogEntry> {
        if data.len() < 41 {
            return Err(WalError::InvalidLogFile(
                "Data too short for frame".to_string(),
            ));
        }

        let mut offset = 0;

        // READ HEADER

        // Read Magic(2 bytes from buffer)
        let magic = u16::from_le_bytes([data[offset], data[offset + 1]]);
        offset += 2;
        if magic != Self::MAGIC {
            return Err(WalError::InvalidLogFile(format!(
                "Invalid WAL MAGIC number, expected: 0x{:04X}, got: 0x{:04X}",
                Self::MAGIC,
                magic
            )));
        }

        // Read version (1 byte)
        let version = data[offset];
        if version != Self::VERSION {
            return Err(WalError::InvalidLogFile(format!(
                "Invalid WAL VERSION number, expected: {}, got: {}",
                Self::VERSION,
                version
            )));
        }
        offset += 1;

        // Read LSN(8 bytes)
        let lsn = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // Read Batch Id(8 bytes)
        let batch_id = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // Read record type(1 byte) IMPLEMENT ERROR TYPE
        let record_type = data[offset];
        offset += 1;

        // Read timestamp(8 bytes)
        let timestamp = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // Read flag(1 byte)
        let _flag = data[offset];
        offset += 1;

        // READ DATA SECTION

        // Read key(variable)
        let (key_len, new_offset) = Self::decode_varint(data, offset)?;
        offset = new_offset;
        if offset + key_len as usize > data.len() {
            return Err(WalError::CorruptedRecord(
                "WAL Key extends beyond buffer".to_string(),
            ));
        }
        let key = data[offset..offset + key_len as usize].to_vec();
        offset += key_len as usize;

        // Read data(variable)
        let (value_len, new_offset) = Self::decode_varint(data, offset)?;
        offset = new_offset;
        if offset + value_len as usize > data.len() {
            return Err(WalError::CorruptedRecord(
                "WAL Value extends beyond buffer".to_string(),
            ));
        }
        let value = if value_len > 0 {
            Some(data[offset..offset + value_len as usize].to_vec())
        } else {
            None
        };
        offset += value_len as usize;

        // READ FOOTER

        // Read Checksum(4 bytes)
        if offset + 4 > data.len() {
            return Err(WalError::CorruptedRecord(
                "WAL Checksum extends beyond buffer".to_string(),
            ));
        }
        let _checksum = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]);

        // REFACTOR THIS LATER TO COMPUTE CHECKSUMS INDIVIDUALLY
        let stored_checksum = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]);

        // Verify checksum over everything BEFORE the checksum itself
        let data_to_check = &data[..offset];
        let computed_checksum = Self::compute_checksum(data_to_check);

        if stored_checksum != computed_checksum {
            return Err(WalError::ChecksumMismatch {
                expected: stored_checksum,
                actual: computed_checksum,
            });
        }
        offset += 4;

        // Read Reserved(4 bytes)
        if offset + 4 > data.len() {
            return Err(WalError::CorruptedRecord(
                "WAL Reserved Bits extend beyond buffer".to_string(),
            ));
        }
        // SKIP READING

        Ok(LogEntry {
            lsn,
            batch_id,
            record_type: RecordType::from_byte(record_type).unwrap(),
            key,
            value: value,
            timestamp,
        })
    }

    pub fn encode_varint(buf: &mut Vec<u8>, mut value: u64) {
        loop {
            let bytes = (value & 0x7f) as u8;
            value >>= 7;
            if value == 0 {
                buf.push(bytes);
                break;
            } else {
                buf.push(bytes | 0x80)
            }
        }
    }

    pub fn decode_varint(data: &[u8], mut offset: usize) -> Result<(u64, usize)> {
        let mut shift = 0;
        let mut value: u64 = 0;

        loop {
            if offset > data.len() {
                return Err(WalError::CorruptedRecord("WAL Buffer overflow".to_string()));
            }

            let bytes = data[offset];
            offset += 1;
            value |= ((bytes & 0x7f) as u64) << shift;

            if bytes & 0x80 == 0 {
                break;
            }
            shift += 7;
        }
        Ok((value, offset))
    }

    pub fn compute_checksum(data: &[u8]) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(data);
        hasher.finalize()
    }

    pub fn verify_checksum(data: &[u8], expected: u32) -> bool {
        Self::compute_checksum(data) == expected
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_deserialize_put() {
        let entry = LogEntry::new_put(
            100,
            1,
            b"user:123".to_vec(),
            b"{\"name\": \"Alice\"}".to_vec(),
        );

        let serialized = Serializer::serialize(&entry).unwrap();
        let deserialized = Serializer::deserialize(&serialized).unwrap();

        assert_eq!(entry, deserialized);
    }

    #[test]
    fn test_serialize_deserialize_delete() {
        let entry = LogEntry::new_delete(200, 2, b"user:456".to_vec());

        let serialized = Serializer::serialize(&entry).unwrap();
        let deserialized = Serializer::deserialize(&serialized).unwrap();

        assert_eq!(entry, deserialized);
        assert_eq!(deserialized.value, None);
    }

    #[test]
    fn test_serialize_deserialize_commit() {
        let entry = LogEntry::new_commit(300, 3);

        let serialized = Serializer::serialize(&entry).unwrap();
        let deserialized = Serializer::deserialize(&serialized).unwrap();

        assert_eq!(entry, deserialized);
        assert!(deserialized.key.is_empty());
        assert_eq!(deserialized.value, None);
    }

    #[test]
    fn test_large_value() {
        let large_value = vec![0xAB; 100_000]; // 100KB value
        let entry = LogEntry::new_put(400, 4, b"large_key".to_vec(), large_value.clone());

        let serialized = Serializer::serialize(&entry).unwrap();
        let deserialized = Serializer::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.value, Some(large_value));
    }

    #[test]
    fn test_varint_small() {
        let mut buf = Vec::new();
        Serializer::encode_varint(&mut buf, 42);
        let (val, _) = Serializer::decode_varint(&buf, 0).unwrap();
        assert_eq!(val, 42);
        assert_eq!(buf.len(), 1); // Small number = 1 byte
    }

    #[test]
    fn test_varint_medium() {
        let mut buf = Vec::new();
        Serializer::encode_varint(&mut buf, 300);
        let (val, _) = Serializer::decode_varint(&buf, 0).unwrap();
        assert_eq!(val, 300);
        assert_eq!(buf.len(), 2); // Medium number = 2 bytes
    }

    #[test]
    fn test_varint_large() {
        let mut buf = Vec::new();
        Serializer::encode_varint(&mut buf, 1_000_000);
        let (val, _) = Serializer::decode_varint(&buf, 0).unwrap();
        assert_eq!(val, 1_000_000);
        assert_eq!(buf.len(), 3); // Large number = 3 bytes
    }

    #[test]
    fn test_checksum_catches_corruption() {
        let entry = LogEntry::new_put(500, 5, b"key".to_vec(), b"value".to_vec());
        let mut serialized = Serializer::serialize(&entry).unwrap();

        // Corrupt one byte in the middle
        if serialized.len() > 10 {
            serialized[10] ^= 0xFF;
        }

        let result = Serializer::deserialize(&serialized);
        assert!(matches!(result, Err(WalError::ChecksumMismatch { .. })));
    }

    #[test]
    fn test_magic_validation() {
        let entry = LogEntry::new_put(600, 6, b"key".to_vec(), b"value".to_vec());
        let mut serialized = Serializer::serialize(&entry).unwrap();

        // Corrupt magic number
        serialized[0] = 0xFF;
        serialized[1] = 0xFF;

        let result = Serializer::deserialize(&serialized);
        assert!(matches!(result, Err(WalError::InvalidLogFile(_))));
    }

    #[test]
    fn test_version_validation() {
        let entry = LogEntry::new_put(700, 7, b"key".to_vec(), b"value".to_vec());
        let mut serialized = Serializer::serialize(&entry).unwrap();

        // Corrupt version
        serialized[2] = 99;

        let result = Serializer::deserialize(&serialized);
        assert!(matches!(result, Err(WalError::InvalidLogFile(_))));
    }
}
