//! Database file header.
//!
//! The first page (page 0) of the database file contains metadata
//! about the database.

use crate::error::{Result, StorageError};
use crate::types::{PageId, PAGE_SIZE};

/// Magic bytes to identify a valid database file
pub const MAGIC: &[u8; 16] = b"BTreeStorageV01\0";

/// File header size (uses first page)
pub const FILE_HEADER_SIZE: usize = PAGE_SIZE;

/// Database file header
///
/// Layout:
/// ```text
/// Offset  Size  Description
/// 0       16    Magic string "BTreeStorageV01\0"
/// 16      4     Page size (currently always 4096)
/// 20      4     Total page count
/// 24      4     First free page ID (0 if none)
/// 28      4     Free page count
/// 32      4     Root page ID of the main B-tree
/// 36      4     Tree height
/// 40      4     Checksum of header (CRC32)
/// ```
#[derive(Debug, Clone, Copy)]
pub struct FileHeader {
    /// Page size in bytes
    pub page_size: u32,
    /// Total number of pages in the file (including header page)
    pub page_count: u32,
    /// First page in the free list (0 if no free pages)
    pub first_free_page: PageId,
    /// Number of free pages
    pub free_page_count: u32,
    /// Root page of the main B-tree
    pub root_page: PageId,
    /// Height of the B-tree
    pub tree_height: u32,
}

impl FileHeader {
    /// Create a new file header for an empty database
    pub fn new() -> Self {
        Self {
            page_size: PAGE_SIZE as u32,
            page_count: 1, // Just the header page initially
            first_free_page: PageId::new(0),
            free_page_count: 0,
            root_page: PageId::new(0), // No root yet
            tree_height: 0,
        }
    }

    /// Read a file header from bytes
    pub fn read(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 44 {
            return Err(StorageError::invalid_db("header too short"));
        }

        // Check magic
        if &bytes[0..16] != MAGIC {
            return Err(StorageError::invalid_db("invalid magic bytes"));
        }

        let page_size = u32::from_be_bytes([bytes[16], bytes[17], bytes[18], bytes[19]]);
        let page_count = u32::from_be_bytes([bytes[20], bytes[21], bytes[22], bytes[23]]);
        let first_free_page = u32::from_be_bytes([bytes[24], bytes[25], bytes[26], bytes[27]]);
        let free_page_count = u32::from_be_bytes([bytes[28], bytes[29], bytes[30], bytes[31]]);
        let root_page = u32::from_be_bytes([bytes[32], bytes[33], bytes[34], bytes[35]]);
        let tree_height = u32::from_be_bytes([bytes[36], bytes[37], bytes[38], bytes[39]]);

        // Verify checksum
        let stored_checksum = u32::from_be_bytes([bytes[40], bytes[41], bytes[42], bytes[43]]);
        let computed_checksum = crc32fast::hash(&bytes[0..40]);
        if stored_checksum != computed_checksum {
            return Err(StorageError::corruption("header checksum mismatch"));
        }

        if page_size != PAGE_SIZE as u32 {
            return Err(StorageError::invalid_db(format!(
                "unsupported page size: {} (expected {})",
                page_size, PAGE_SIZE
            )));
        }

        Ok(Self {
            page_size,
            page_count,
            first_free_page: PageId::new(first_free_page),
            free_page_count,
            root_page: PageId::new(root_page),
            tree_height,
        })
    }

    /// Write this header to bytes
    pub fn write(&self, bytes: &mut [u8]) {
        // Clear the page first
        bytes[..FILE_HEADER_SIZE].fill(0);

        // Magic
        bytes[0..16].copy_from_slice(MAGIC);

        // Fields
        bytes[16..20].copy_from_slice(&self.page_size.to_be_bytes());
        bytes[20..24].copy_from_slice(&self.page_count.to_be_bytes());
        bytes[24..28].copy_from_slice(&self.first_free_page.value().to_be_bytes());
        bytes[28..32].copy_from_slice(&self.free_page_count.to_be_bytes());
        bytes[32..36].copy_from_slice(&self.root_page.value().to_be_bytes());
        bytes[36..40].copy_from_slice(&self.tree_height.to_be_bytes());

        // Checksum
        let checksum = crc32fast::hash(&bytes[0..40]);
        bytes[40..44].copy_from_slice(&checksum.to_be_bytes());
    }

    /// Allocate a new page ID
    pub fn allocate_page(&mut self) -> PageId {
        let page_id = PageId::new(self.page_count);
        self.page_count += 1;
        page_id
    }
}

impl Default for FileHeader {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_roundtrip() {
        let header = FileHeader {
            page_size: PAGE_SIZE as u32,
            page_count: 100,
            first_free_page: PageId::new(50),
            free_page_count: 5,
            root_page: PageId::new(1),
            tree_height: 3,
        };

        let mut bytes = vec![0u8; FILE_HEADER_SIZE];
        header.write(&mut bytes);

        let restored = FileHeader::read(&bytes).unwrap();
        assert_eq!(restored.page_size, header.page_size);
        assert_eq!(restored.page_count, header.page_count);
        assert_eq!(restored.first_free_page, header.first_free_page);
        assert_eq!(restored.free_page_count, header.free_page_count);
        assert_eq!(restored.root_page, header.root_page);
        assert_eq!(restored.tree_height, header.tree_height);
    }

    #[test]
    fn test_invalid_magic() {
        let mut bytes = vec![0u8; FILE_HEADER_SIZE];
        bytes[0..16].copy_from_slice(b"InvalidMagic0000");

        assert!(FileHeader::read(&bytes).is_err());
    }

    #[test]
    fn test_checksum_validation() {
        let header = FileHeader::new();
        let mut bytes = vec![0u8; FILE_HEADER_SIZE];
        header.write(&mut bytes);

        // Corrupt a byte
        bytes[20] ^= 0xFF;

        assert!(FileHeader::read(&bytes).is_err());
    }

    #[test]
    fn test_allocate_page() {
        let mut header = FileHeader::new();
        assert_eq!(header.page_count, 1);

        let p1 = header.allocate_page();
        assert_eq!(p1, PageId::new(1));
        assert_eq!(header.page_count, 2);

        let p2 = header.allocate_page();
        assert_eq!(p2, PageId::new(2));
        assert_eq!(header.page_count, 3);
    }
}
