//! Variable-length integer encoding (SQLite-style varint).
//!
//! Varints are a compact way to encode integers using 1-10 bytes.
//! Smaller values use fewer bytes.

/// Encode a u64 as a variable-length integer.
///
/// Returns the encoded bytes. Uses LEB128-style encoding:
/// - Each byte uses 7 bits for data and 1 bit (MSB) as continuation flag
/// - MSB = 1 means more bytes follow
/// - MSB = 0 means this is the last byte
pub fn encode_varint(mut value: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(10);

    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;

        if value != 0 {
            byte |= 0x80; // Set continuation bit
        }

        buf.push(byte);

        if value == 0 {
            break;
        }
    }

    buf
}

/// Decode a variable-length integer from a byte slice.
///
/// Returns the decoded value and the number of bytes consumed.
/// Returns `None` if the encoding is invalid or incomplete.
pub fn decode_varint(bytes: &[u8]) -> Option<(u64, usize)> {
    if bytes.is_empty() {
        return None;
    }

    let mut value: u64 = 0;
    let mut shift = 0;

    for (i, &byte) in bytes.iter().enumerate() {
        if i >= 10 {
            // Maximum 10 bytes for a 64-bit value with LEB128
            return None;
        }

        value |= ((byte & 0x7F) as u64) << shift;

        if byte & 0x80 == 0 {
            // This is the last byte
            return Some((value, i + 1));
        }

        shift += 7;
        if shift > 63 {
            // Overflow protection
            return None;
        }
    }

    // Incomplete varint
    None
}

/// Calculate the number of bytes needed to encode a value.
pub fn varint_size(value: u64) -> usize {
    if value <= 127 {
        1
    } else if value <= 16383 {
        2
    } else if value <= 2097151 {
        3
    } else if value <= 268435455 {
        4
    } else if value <= 34359738367 {
        5
    } else if value <= 4398046511103 {
        6
    } else if value <= 562949953421311 {
        7
    } else if value <= 72057594037927935 {
        8
    } else {
        9
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint_roundtrip() {
        let test_values = [
            0u64,
            1,
            127,
            128,
            255,
            256,
            16383,
            16384,
            2097151,
            2097152,
            u32::MAX as u64,
            u64::MAX,
        ];

        for &value in &test_values {
            let encoded = encode_varint(value);
            let (decoded, size) = decode_varint(&encoded).unwrap();
            assert_eq!(decoded, value, "Failed for value {}", value);
            assert_eq!(size, encoded.len());
        }
    }

    #[test]
    fn test_varint_size() {
        assert_eq!(varint_size(0), 1);
        assert_eq!(varint_size(127), 1);
        assert_eq!(varint_size(128), 2);
        assert_eq!(encode_varint(127).len(), 1);
        assert_eq!(encode_varint(128).len(), 2);
    }

    #[test]
    fn test_decode_empty() {
        assert!(decode_varint(&[]).is_none());
    }
}
