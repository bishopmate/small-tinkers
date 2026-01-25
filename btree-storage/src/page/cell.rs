//! Cell encoding and decoding.
//!
//! Cells are the variable-length records stored within B-tree pages.
//! Each cell contains a key and optionally a value (for leaf pages)
//! or a child page pointer (for interior pages).

use crate::types::{decode_varint, encode_varint, PageId};

/// Type of cell stored in a page
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CellType {
    /// Leaf cell: contains key + value
    Leaf,
    /// Interior cell: contains key + left child pointer
    Interior,
}

/// A cell within a B-tree page
#[derive(Debug, Clone)]
pub struct Cell {
    /// Type of this cell
    pub cell_type: CellType,
    /// The key bytes
    pub key: Vec<u8>,
    /// The value bytes (only for leaf cells)
    pub value: Vec<u8>,
    /// Left child page pointer (only for interior cells)
    pub left_child: PageId,
}

impl Cell {
    /// Create a new leaf cell with key and value
    pub fn new_leaf(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self {
            cell_type: CellType::Leaf,
            key,
            value,
            left_child: PageId::INVALID,
        }
    }

    /// Create a new interior cell with key and left child pointer
    pub fn new_interior(key: Vec<u8>, left_child: PageId) -> Self {
        Self {
            cell_type: CellType::Interior,
            key,
            value: Vec::new(),
            left_child,
        }
    }

    /// Calculate the encoded size of this cell in bytes
    pub fn encoded_size(&self) -> usize {
        match self.cell_type {
            CellType::Leaf => {
                // key_len (varint) + value_len (varint) + key + value
                let key_len_size = varint_len(self.key.len() as u64);
                let value_len_size = varint_len(self.value.len() as u64);
                key_len_size + value_len_size + self.key.len() + self.value.len()
            }
            CellType::Interior => {
                // left_child (4 bytes) + key_len (varint) + key
                let key_len_size = varint_len(self.key.len() as u64);
                4 + key_len_size + self.key.len()
            }
        }
    }

    /// Encode this cell into bytes
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.encoded_size());

        match self.cell_type {
            CellType::Leaf => {
                // Leaf cell format:
                // - key_len: varint
                // - value_len: varint
                // - key: [u8; key_len]
                // - value: [u8; value_len]
                buf.extend(encode_varint(self.key.len() as u64));
                buf.extend(encode_varint(self.value.len() as u64));
                buf.extend(&self.key);
                buf.extend(&self.value);
            }
            CellType::Interior => {
                // Interior cell format:
                // - left_child: u32 (big-endian)
                // - key_len: varint
                // - key: [u8; key_len]
                buf.extend(&self.left_child.value().to_be_bytes());
                buf.extend(encode_varint(self.key.len() as u64));
                buf.extend(&self.key);
            }
        }

        buf
    }

    /// Decode a leaf cell from bytes
    ///
    /// Returns the cell and the number of bytes consumed.
    pub fn decode_leaf(bytes: &[u8]) -> Option<(Self, usize)> {
        let mut offset = 0;

        // Read key length
        let (key_len, n) = decode_varint(&bytes[offset..])?;
        offset += n;

        // Read value length
        let (value_len, n) = decode_varint(&bytes[offset..])?;
        offset += n;

        // Read key
        let key_len = key_len as usize;
        if offset + key_len > bytes.len() {
            return None;
        }
        let key = bytes[offset..offset + key_len].to_vec();
        offset += key_len;

        // Read value
        let value_len = value_len as usize;
        if offset + value_len > bytes.len() {
            return None;
        }
        let value = bytes[offset..offset + value_len].to_vec();
        offset += value_len;

        Some((Self::new_leaf(key, value), offset))
    }

    /// Decode an interior cell from bytes
    ///
    /// Returns the cell and the number of bytes consumed.
    pub fn decode_interior(bytes: &[u8]) -> Option<(Self, usize)> {
        if bytes.len() < 4 {
            return None;
        }

        let mut offset = 0;

        // Read left child pointer
        let left_child = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        offset += 4;

        // Read key length
        let (key_len, n) = decode_varint(&bytes[offset..])?;
        offset += n;

        // Read key
        let key_len = key_len as usize;
        if offset + key_len > bytes.len() {
            return None;
        }
        let key = bytes[offset..offset + key_len].to_vec();
        offset += key_len;

        Some((Self::new_interior(key, PageId::new(left_child)), offset))
    }
}

/// Calculate the number of bytes needed to encode a varint
fn varint_len(value: u64) -> usize {
    encode_varint(value).len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_leaf_cell_roundtrip() {
        let cell = Cell::new_leaf(b"hello".to_vec(), b"world".to_vec());
        let encoded = cell.encode();
        let (decoded, size) = Cell::decode_leaf(&encoded).unwrap();

        assert_eq!(size, encoded.len());
        assert_eq!(decoded.cell_type, CellType::Leaf);
        assert_eq!(decoded.key, b"hello");
        assert_eq!(decoded.value, b"world");
    }

    #[test]
    fn test_interior_cell_roundtrip() {
        let cell = Cell::new_interior(b"separator".to_vec(), PageId::new(42));
        let encoded = cell.encode();
        let (decoded, size) = Cell::decode_interior(&encoded).unwrap();

        assert_eq!(size, encoded.len());
        assert_eq!(decoded.cell_type, CellType::Interior);
        assert_eq!(decoded.key, b"separator");
        assert_eq!(decoded.left_child, PageId::new(42));
    }

    #[test]
    fn test_encoded_size() {
        let cell = Cell::new_leaf(b"key".to_vec(), b"value".to_vec());
        assert_eq!(cell.encoded_size(), cell.encode().len());

        let cell = Cell::new_interior(b"key".to_vec(), PageId::new(100));
        assert_eq!(cell.encoded_size(), cell.encode().len());
    }

    #[test]
    fn test_empty_value() {
        let cell = Cell::new_leaf(b"key".to_vec(), Vec::new());
        let encoded = cell.encode();
        let (decoded, _) = Cell::decode_leaf(&encoded).unwrap();
        assert!(decoded.value.is_empty());
    }
}
