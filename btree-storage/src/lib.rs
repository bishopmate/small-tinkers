//! # BTree Storage Engine
//!
//! A high-performance, disk-based B-tree storage engine designed for relational databases.
//!
//! ## Architecture
//!
//! The storage engine is composed of modular, swappable components:
//!
//! - **Page Layer** (`page`): Slotted page format with cell-based layout
//! - **Storage Layer** (`storage`): Disk I/O abstraction and page management
//! - **Buffer Pool** (`buffer`): LRU page cache with dirty tracking
//! - **B-Tree Layer** (`btree`): Core B-tree operations and cursor iteration
//!
//! ## Usage
//!
//! ```rust,ignore
//! use btree_storage::{Db, Config};
//!
//! let config = Config::new("my_database.db");
//! let db = Db::open(config)?;
//!
//! // Put a key-value pair
//! db.put(b"hello", b"world")?;
//!
//! // Get a value
//! let value = db.get(b"hello")?;
//!
//! // Delete a key
//! db.delete(b"hello")?;
//!
//! // Range scan
//! for result in db.range(b"a"..b"z")? {
//!     let (key, value) = result?;
//!     println!("{:?} -> {:?}", key, value);
//! }
//! ```

pub mod buffer;
pub mod btree;
pub mod error;
pub mod page;
pub mod storage;
pub mod types;

pub use error::{Result, StorageError};
pub use types::{PageId, PAGE_SIZE};

// Re-export main public API
pub use btree::BTree;
pub use buffer::{BufferPool, BufferPoolImpl};
pub use storage::{DiskManager, DiskManagerImpl};

use std::path::PathBuf;
use std::sync::Arc;
use parking_lot::RwLock;

/// Database configuration
#[derive(Debug, Clone)]
pub struct Config {
    /// Path to the database file
    pub path: PathBuf,
    /// Buffer pool size in number of pages (default: 1000)
    pub buffer_pool_size: usize,
    /// Whether to sync writes immediately (default: false for performance)
    pub sync_on_write: bool,
}

impl Config {
    /// Create a new configuration with default settings
    pub fn new<P: Into<PathBuf>>(path: P) -> Self {
        Self {
            path: path.into(),
            buffer_pool_size: 1000,
            sync_on_write: false,
        }
    }

    /// Set buffer pool size
    pub fn buffer_pool_size(mut self, size: usize) -> Self {
        self.buffer_pool_size = size;
        self
    }

    /// Enable sync on write for durability
    pub fn sync_on_write(mut self, enabled: bool) -> Self {
        self.sync_on_write = enabled;
        self
    }
}

/// Main database handle providing key-value storage backed by a B-tree
///
/// This is the primary public interface for the storage engine.
/// It provides a clean API for other database layers to use.
pub struct Db {
    btree: Arc<RwLock<BTree>>,
    buffer_pool: Arc<BufferPoolImpl>,
    #[allow(dead_code)]
    disk_manager: Arc<DiskManagerImpl>,
}

impl Db {
    /// Open or create a database at the given path
    pub fn open(config: Config) -> Result<Self> {
        let disk_manager = Arc::new(DiskManagerImpl::open(&config.path, config.sync_on_write)?);
        let buffer_pool = Arc::new(BufferPoolImpl::new(
            disk_manager.clone(),
            config.buffer_pool_size,
        ));
        let btree = Arc::new(RwLock::new(BTree::new(buffer_pool.clone())?));

        Ok(Self {
            btree,
            buffer_pool,
            disk_manager,
        })
    }

    /// Get a value by key
    ///
    /// Returns `None` if the key does not exist.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let btree = self.btree.read();
        btree.get(key)
    }

    /// Insert or update a key-value pair
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut btree = self.btree.write();
        btree.put(key, value)
    }

    /// Delete a key-value pair
    ///
    /// Returns `true` if the key existed and was deleted.
    pub fn delete(&self, key: &[u8]) -> Result<bool> {
        let mut btree = self.btree.write();
        btree.delete(key)
    }

    /// Check if a key exists
    pub fn contains(&self, key: &[u8]) -> Result<bool> {
        let btree = self.btree.read();
        Ok(btree.get(key)?.is_some())
    }

    /// Iterate over all key-value pairs in sorted order
    pub fn iter(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let btree = self.btree.read();
        btree.scan(None, None)
    }

    /// Iterate over key-value pairs in a range
    ///
    /// Both bounds are optional; `None` means unbounded on that side.
    pub fn range(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let btree = self.btree.read();
        btree.scan(start, end)
    }

    /// Flush all dirty pages to disk
    pub fn flush(&self) -> Result<()> {
        self.buffer_pool.flush_all()
    }

    /// Debug trace a key lookup
    pub fn debug_get(&self, key: &[u8]) -> Result<Vec<String>> {
        let btree = self.btree.read();
        btree.debug_get(key)
    }

    /// Get statistics about the database
    pub fn stats(&self) -> DbStats {
        let btree = self.btree.read();
        DbStats {
            page_count: self.buffer_pool.page_count(),
            buffer_pool_size: self.buffer_pool.capacity(),
            tree_height: btree.height(),
        }
    }
}

/// Database statistics
#[derive(Debug, Clone)]
pub struct DbStats {
    /// Total number of pages in the database
    pub page_count: usize,
    /// Buffer pool capacity
    pub buffer_pool_size: usize,
    /// Height of the B-tree
    pub tree_height: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_basic_operations() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let config = Config::new(&path);
        let db = Db::open(config)?;

        // Test put and get
        db.put(b"key1", b"value1")?;
        assert_eq!(db.get(b"key1")?, Some(b"value1".to_vec()));

        // Test update
        db.put(b"key1", b"value2")?;
        assert_eq!(db.get(b"key1")?, Some(b"value2".to_vec()));

        // Test delete
        assert!(db.delete(b"key1")?);
        assert_eq!(db.get(b"key1")?, None);

        // Test non-existent key
        assert_eq!(db.get(b"nonexistent")?, None);
        assert!(!db.delete(b"nonexistent")?);

        Ok(())
    }

    #[test]
    fn test_range_scan() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let config = Config::new(&path);
        let db = Db::open(config)?;

        // Insert some data
        db.put(b"apple", b"1")?;
        db.put(b"banana", b"2")?;
        db.put(b"cherry", b"3")?;
        db.put(b"date", b"4")?;

        // Full scan
        let all = db.iter()?;
        assert_eq!(all.len(), 4);

        // Range scan
        let range = db.range(Some(b"banana"), Some(b"date"))?;
        assert_eq!(range.len(), 2);
        assert_eq!(range[0].0, b"banana".to_vec());
        assert_eq!(range[1].0, b"cherry".to_vec());

        Ok(())
    }
}
