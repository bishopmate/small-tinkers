//! Common types used throughout the storage engine.

mod page_id;
mod varint;

pub use page_id::PageId;
pub use varint::{decode_varint, encode_varint, varint_size};

use serde::{Deserialize, Serialize};

/// Page size in bytes (4KB)
pub const PAGE_SIZE: usize = 4096;

/// Maximum key size (to ensure at least 2 cells fit per page)
/// With header ~12 bytes, cell pointers 2 bytes each, and cell overhead ~10 bytes,
/// we allow keys up to 1/4 of page size
pub const MAX_KEY_SIZE: usize = PAGE_SIZE / 4;

/// Maximum value size for inline storage
/// Larger values would need overflow pages (not implemented in v1)
pub const MAX_VALUE_SIZE: usize = PAGE_SIZE / 2;

/// Minimum number of keys per node (B-tree order property)
/// A node must have at least MIN_KEYS keys (except root)
pub const MIN_KEYS: usize = 2;

/// Default maximum keys per leaf node (for visualization-friendly defaults)
pub const DEFAULT_MAX_LEAF_KEYS: usize = 4;

/// Default maximum keys per interior node (for visualization-friendly defaults)
pub const DEFAULT_MAX_INTERIOR_KEYS: usize = 3;

/// BTree configuration for customizable node limits
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BTreeConfig {
    /// Maximum keys per leaf node
    pub max_leaf_keys: usize,
    /// Maximum keys per interior node
    pub max_interior_keys: usize,
}

impl Default for BTreeConfig {
    fn default() -> Self {
        Self {
            max_leaf_keys: DEFAULT_MAX_LEAF_KEYS,
            max_interior_keys: DEFAULT_MAX_INTERIOR_KEYS,
        }
    }
}

impl BTreeConfig {
    /// Create a new config with custom limits
    pub fn new(max_leaf_keys: usize, max_interior_keys: usize) -> Self {
        Self {
            max_leaf_keys: max_leaf_keys.max(MIN_KEYS),
            max_interior_keys: max_interior_keys.max(MIN_KEYS),
        }
    }

    /// Create a config optimized for maximum capacity (page-based limits only)
    pub fn high_capacity() -> Self {
        Self {
            // Use a large number to effectively disable key-count based splits
            max_leaf_keys: 1000,
            max_interior_keys: 1000,
        }
    }
}

/// Page types
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageType {
    /// Free/unallocated page
    Free = 0x00,
    /// Interior node of table B-tree (keys + child pointers)
    InteriorTable = 0x02,
    /// Interior node of index B-tree
    InteriorIndex = 0x05,
    /// Leaf node of table B-tree (keys + values)
    LeafTable = 0x0D,
    /// Leaf node of index B-tree
    LeafIndex = 0x0A,
    /// Overflow page for large payloads
    Overflow = 0x0F,
}

impl PageType {
    /// Check if this is a leaf page type
    pub fn is_leaf(self) -> bool {
        matches!(self, Self::LeafTable | Self::LeafIndex)
    }

    /// Check if this is an interior page type
    pub fn is_interior(self) -> bool {
        matches!(self, Self::InteriorTable | Self::InteriorIndex)
    }

    /// Convert from byte value
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0x00 => Some(Self::Free),
            0x02 => Some(Self::InteriorTable),
            0x05 => Some(Self::InteriorIndex),
            0x0D => Some(Self::LeafTable),
            0x0A => Some(Self::LeafIndex),
            0x0F => Some(Self::Overflow),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_type_conversions() {
        assert!(PageType::LeafTable.is_leaf());
        assert!(PageType::LeafIndex.is_leaf());
        assert!(!PageType::InteriorTable.is_leaf());

        assert!(PageType::InteriorTable.is_interior());
        assert!(!PageType::LeafTable.is_interior());

        assert_eq!(PageType::from_byte(0x0D), Some(PageType::LeafTable));
        assert_eq!(PageType::from_byte(0xFF), None);
    }
}
