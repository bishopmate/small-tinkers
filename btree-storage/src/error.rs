//! Error types for the storage engine.

use thiserror::Error;
use crate::types::PageId;

/// Result type alias for storage operations
pub type Result<T> = std::result::Result<T, StorageError>;

/// Errors that can occur in the storage engine
#[derive(Error, Debug)]
pub enum StorageError {
    /// I/O error from the underlying file system
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Requested page was not found
    #[error("Page {0} not found")]
    PageNotFound(PageId),

    /// Page does not have enough space for the operation
    #[error("Page {page_id} is full, need {needed} bytes but only {available} available")]
    PageFull {
        page_id: PageId,
        needed: usize,
        available: usize,
    },

    /// Key exceeds maximum allowed size
    #[error("Key too large: {size} bytes (max: {max})")]
    KeyTooLarge { size: usize, max: usize },

    /// Value exceeds maximum allowed size
    #[error("Value too large: {size} bytes (max: {max})")]
    ValueTooLarge { size: usize, max: usize },

    /// Data corruption detected (e.g., checksum mismatch)
    #[error("Corruption detected: {0}")]
    Corruption(String),

    /// Invalid page format or type
    #[error("Invalid page: {0}")]
    InvalidPage(String),

    /// Buffer pool has no available frames
    #[error("Buffer pool exhausted: no available frames")]
    BufferPoolExhausted,

    /// Invalid operation for the current state
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    /// Key not found (for operations that require existing key)
    #[error("Key not found")]
    KeyNotFound,

    /// Database file is corrupted or has invalid format
    #[error("Invalid database file: {0}")]
    InvalidDatabaseFile(String),
}

impl StorageError {
    /// Create a corruption error with a message
    pub fn corruption(msg: impl Into<String>) -> Self {
        Self::Corruption(msg.into())
    }

    /// Create an invalid page error
    pub fn invalid_page(msg: impl Into<String>) -> Self {
        Self::InvalidPage(msg.into())
    }

    /// Create an invalid operation error
    pub fn invalid_operation(msg: impl Into<String>) -> Self {
        Self::InvalidOperation(msg.into())
    }

    /// Create an invalid database file error
    pub fn invalid_db(msg: impl Into<String>) -> Self {
        Self::InvalidDatabaseFile(msg.into())
    }
}
