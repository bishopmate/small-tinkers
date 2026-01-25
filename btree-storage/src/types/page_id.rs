//! Page identifier type.

use std::fmt;

/// Unique identifier for a page in the database file.
///
/// Page IDs are 0-indexed. Page 0 is reserved for the file header.
/// Valid B-tree pages start from page 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct PageId(pub u32);

impl PageId {
    /// Invalid page ID, used as a sentinel value
    pub const INVALID: PageId = PageId(u32::MAX);

    /// Page ID for the file header (page 0)
    pub const HEADER: PageId = PageId(0);

    /// Create a new page ID
    pub const fn new(id: u32) -> Self {
        Self(id)
    }

    /// Get the raw page ID value
    pub const fn value(self) -> u32 {
        self.0
    }

    /// Check if this is a valid page ID
    pub const fn is_valid(self) -> bool {
        self.0 != u32::MAX
    }

    /// Calculate the byte offset of this page in the file
    pub const fn file_offset(self, page_size: usize) -> u64 {
        self.0 as u64 * page_size as u64
    }
}

impl fmt::Display for PageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if *self == Self::INVALID {
            write!(f, "INVALID")
        } else {
            write!(f, "{}", self.0)
        }
    }
}

impl From<u32> for PageId {
    fn from(id: u32) -> Self {
        Self(id)
    }
}

impl From<PageId> for u32 {
    fn from(id: PageId) -> Self {
        id.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PAGE_SIZE;

    #[test]
    fn test_page_id_basics() {
        let id = PageId::new(42);
        assert_eq!(id.value(), 42);
        assert!(id.is_valid());
        assert!(!PageId::INVALID.is_valid());
    }

    #[test]
    fn test_page_id_file_offset() {
        let id = PageId::new(3);
        assert_eq!(id.file_offset(PAGE_SIZE), 3 * PAGE_SIZE as u64);
    }

    #[test]
    fn test_page_id_display() {
        assert_eq!(format!("{}", PageId::new(42)), "42");
        assert_eq!(format!("{}", PageId::INVALID), "INVALID");
    }
}
