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
pub use types::{BTreeConfig, PageId, PAGE_SIZE};

// Re-export main public API
pub use btree::BTree;
pub use buffer::{BufferPool, BufferPoolImpl};
pub use storage::{DiskManager, DiskManagerImpl};

use serde::{Deserialize, Serialize};
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
    /// B-tree configuration for node limits
    pub btree_config: BTreeConfig,
}

impl Config {
    /// Create a new configuration with default settings
    pub fn new<P: Into<PathBuf>>(path: P) -> Self {
        Self {
            path: path.into(),
            buffer_pool_size: 1000,
            sync_on_write: false,
            btree_config: BTreeConfig::default(),
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

    /// Set B-tree configuration
    pub fn btree_config(mut self, config: BTreeConfig) -> Self {
        self.btree_config = config;
        self
    }
}

/// Node type for visualization
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TreeNode {
    /// Page ID
    pub page_id: u32,
    /// Whether this is a leaf node
    pub is_leaf: bool,
    /// Keys in this node
    pub keys: Vec<String>,
    /// Values (only for leaf nodes)
    pub values: Vec<String>,
    /// Child nodes (only for interior nodes)
    pub children: Vec<TreeNode>,
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
    config: Config,
}

impl Db {
    /// Open or create a database at the given path
    pub fn open(config: Config) -> Result<Self> {
        let disk_manager = Arc::new(DiskManagerImpl::open(&config.path, config.sync_on_write)?);
        let buffer_pool = Arc::new(BufferPoolImpl::new(
            disk_manager.clone(),
            config.buffer_pool_size,
        ));
        let btree = Arc::new(RwLock::new(BTree::with_config(
            buffer_pool.clone(),
            config.btree_config.clone(),
        )?));

        Ok(Self {
            btree,
            buffer_pool,
            disk_manager,
            config,
        })
    }

    /// Get the current B-tree configuration
    pub fn btree_config(&self) -> BTreeConfig {
        self.config.btree_config.clone()
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

    /// Export the tree structure for visualization
    pub fn export_tree(&self) -> Result<Option<TreeNode>> {
        let btree = self.btree.read();
        let root_page = btree.root_page();

        if root_page.value() == 0 {
            return Ok(None);
        }

        self.export_node(root_page)
    }

    fn export_node(&self, page_id: PageId) -> Result<Option<TreeNode>> {
        // First, read the page and determine if it's a leaf
        let is_leaf = {
            let guard = self.buffer_pool.fetch_page(page_id)?;
            let page = guard.read();
            page.is_leaf()
        };

        if is_leaf {
            self.export_leaf_node(page_id)
        } else {
            self.export_interior_node(page_id)
        }
    }

    fn export_leaf_node(&self, page_id: PageId) -> Result<Option<TreeNode>> {
        let guard = self.buffer_pool.fetch_page(page_id)?;
        let page = guard.read();

        let mut keys = Vec::new();
        let mut values = Vec::new();

        for i in 0..page.cell_count() {
            let cell = page.get_cell(i)?;
            keys.push(String::from_utf8_lossy(&cell.key).to_string());
            values.push(String::from_utf8_lossy(&cell.value).to_string());
        }

        Ok(Some(TreeNode {
            page_id: page_id.value(),
            is_leaf: true,
            keys,
            values,
            children: Vec::new(),
        }))
    }

    fn export_interior_node(&self, page_id: PageId) -> Result<Option<TreeNode>> {
        let guard = self.buffer_pool.fetch_page(page_id)?;
        let page = guard.read();

        let mut keys = Vec::new();
        let mut child_ids = Vec::new();

        // Collect right_child first (leftmost child)
        let right_child = page.right_child();
        if right_child.value() != 0 {
            child_ids.push(right_child);
        }

        // Collect all keys and their left_child pointers
        for i in 0..page.cell_count() {
            let cell = page.get_cell(i)?;
            keys.push(String::from_utf8_lossy(&cell.key).to_string());
            if cell.left_child.value() != 0 {
                child_ids.push(cell.left_child);
            }
        }

        drop(page);
        drop(guard);

        // Now export all children
        let mut children = Vec::new();
        for child_id in child_ids {
            if let Some(child_node) = self.export_node(child_id)? {
                children.push(child_node);
            }
        }

        Ok(Some(TreeNode {
            page_id: page_id.value(),
            is_leaf: false,
            keys,
            values: Vec::new(),
            children,
        }))
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
