//! Page layer: slotted page format with cell-based layout.
//!
//! This module implements the fundamental on-disk page structure for the B-tree.
//! Pages use a slotted format where:
//! - A fixed header contains metadata
//! - Cell pointers grow from the header toward the end
//! - Cell content grows from the end toward the header
//! - Free space is in the middle

mod cell;
mod header;
mod slotted;

pub use cell::{Cell, CellType};
pub use header::PageHeader;
pub use slotted::SlottedPage;

use crate::types::PAGE_SIZE;

/// A raw page buffer
#[derive(Clone)]
pub struct PageBuf {
    data: [u8; PAGE_SIZE],
}

impl PageBuf {
    /// Create a new zeroed page buffer
    pub fn new() -> Self {
        Self {
            data: [0u8; PAGE_SIZE],
        }
    }

    /// Create a page buffer from raw bytes
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut data = [0u8; PAGE_SIZE];
        let len = bytes.len().min(PAGE_SIZE);
        data[..len].copy_from_slice(&bytes[..len]);
        Self { data }
    }

    /// Get a reference to the raw bytes
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Get a mutable reference to the raw bytes
    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
}

impl Default for PageBuf {
    fn default() -> Self {
        Self::new()
    }
}

impl std::ops::Deref for PageBuf {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl std::ops::DerefMut for PageBuf {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl AsRef<[u8]> for PageBuf {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl AsMut<[u8]> for PageBuf {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
}
