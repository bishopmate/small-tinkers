//! Page header structure.
//!
//! The page header occupies the first bytes of each B-tree page and contains
//! metadata about the page contents.

use crate::types::PageType;

/// Size of the page header for leaf pages (no right child pointer)
pub const LEAF_HEADER_SIZE: usize = 8;

/// Size of the page header for interior pages (includes right child pointer)
pub const INTERIOR_HEADER_SIZE: usize = 12;

/// Page header structure
///
/// Layout (for leaf pages, 8 bytes):
/// ```text
/// Offset  Size  Description
/// 0       1     Page type flag
/// 1       2     Offset to first freeblock (0 if none)
/// 3       2     Number of cells on this page
/// 5       2     Offset to start of cell content area
/// 7       1     Number of fragmented free bytes
/// ```
///
/// For interior pages, add 4 bytes at offset 8:
/// ```text
/// 8       4     Right-most child page pointer
/// ```
#[derive(Debug, Clone, Copy)]
pub struct PageHeader {
    /// Type of this page (leaf, interior, etc.)
    pub page_type: PageType,
    /// Offset to the first freeblock, or 0 if there are no freeblocks
    pub first_freeblock: u16,
    /// Number of cells on this page
    pub cell_count: u16,
    /// Offset to the start of the cell content area
    pub cell_content_start: u16,
    /// Number of fragmented free bytes within the cell content area
    pub fragmented_bytes: u8,
    /// Right-most child pointer (only valid for interior pages)
    pub right_child: u32,
}

impl PageHeader {
    /// Create a new page header for a leaf page
    pub fn new_leaf() -> Self {
        use crate::types::PAGE_SIZE;
        Self {
            page_type: PageType::LeafTable,
            first_freeblock: 0,
            cell_count: 0,
            cell_content_start: PAGE_SIZE as u16,
            fragmented_bytes: 0,
            right_child: 0,
        }
    }

    /// Create a new page header for an interior page
    pub fn new_interior() -> Self {
        use crate::types::PAGE_SIZE;
        Self {
            page_type: PageType::InteriorTable,
            first_freeblock: 0,
            cell_count: 0,
            cell_content_start: PAGE_SIZE as u16,
            fragmented_bytes: 0,
            right_child: 0,
        }
    }

    /// Get the size of this header in bytes
    pub fn size(&self) -> usize {
        if self.page_type.is_interior() {
            INTERIOR_HEADER_SIZE
        } else {
            LEAF_HEADER_SIZE
        }
    }

    /// Read a page header from bytes
    pub fn read(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < LEAF_HEADER_SIZE {
            return None;
        }

        let page_type = PageType::from_byte(bytes[0])?;
        let first_freeblock = u16::from_be_bytes([bytes[1], bytes[2]]);
        let cell_count = u16::from_be_bytes([bytes[3], bytes[4]]);
        let cell_content_start = u16::from_be_bytes([bytes[5], bytes[6]]);
        let fragmented_bytes = bytes[7];

        let right_child = if page_type.is_interior() && bytes.len() >= INTERIOR_HEADER_SIZE {
            u32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]])
        } else {
            0
        };

        Some(Self {
            page_type,
            first_freeblock,
            cell_count,
            cell_content_start,
            fragmented_bytes,
            right_child,
        })
    }

    /// Write this header to bytes
    pub fn write(&self, bytes: &mut [u8]) {
        bytes[0] = self.page_type as u8;
        bytes[1..3].copy_from_slice(&self.first_freeblock.to_be_bytes());
        bytes[3..5].copy_from_slice(&self.cell_count.to_be_bytes());
        bytes[5..7].copy_from_slice(&self.cell_content_start.to_be_bytes());
        bytes[7] = self.fragmented_bytes;

        if self.page_type.is_interior() && bytes.len() >= INTERIOR_HEADER_SIZE {
            bytes[8..12].copy_from_slice(&self.right_child.to_be_bytes());
        }
    }

    /// Calculate the offset where cell pointers start
    pub fn cell_pointer_offset(&self) -> usize {
        self.size()
    }

    /// Calculate the end of the cell pointer array
    pub fn cell_pointer_array_end(&self) -> usize {
        self.cell_pointer_offset() + (self.cell_count as usize * 2)
    }

    /// Calculate free space available for new cells
    pub fn free_space(&self) -> usize {
        let used_by_pointers = self.cell_pointer_array_end();
        (self.cell_content_start as usize).saturating_sub(used_by_pointers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PAGE_SIZE;

    #[test]
    fn test_header_roundtrip() {
        let header = PageHeader {
            page_type: PageType::LeafTable,
            first_freeblock: 100,
            cell_count: 5,
            cell_content_start: 3500,
            fragmented_bytes: 10,
            right_child: 0,
        };

        let mut bytes = [0u8; LEAF_HEADER_SIZE];
        header.write(&mut bytes);

        let read_header = PageHeader::read(&bytes).unwrap();
        assert_eq!(read_header.page_type, PageType::LeafTable);
        assert_eq!(read_header.first_freeblock, 100);
        assert_eq!(read_header.cell_count, 5);
        assert_eq!(read_header.cell_content_start, 3500);
        assert_eq!(read_header.fragmented_bytes, 10);
    }

    #[test]
    fn test_interior_header() {
        let mut header = PageHeader::new_interior();
        header.right_child = 42;

        let mut bytes = [0u8; INTERIOR_HEADER_SIZE];
        header.write(&mut bytes);

        let read_header = PageHeader::read(&bytes).unwrap();
        assert_eq!(read_header.page_type, PageType::InteriorTable);
        assert_eq!(read_header.right_child, 42);
    }

    #[test]
    fn test_free_space() {
        let header = PageHeader::new_leaf();
        // Fresh leaf page: all space after header is free
        assert_eq!(header.free_space(), PAGE_SIZE - LEAF_HEADER_SIZE);
    }
}
