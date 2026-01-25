//! B-tree core implementation.
//!
//! This module provides the main BTree struct with operations for:
//! - get: Point lookups
//! - put: Insertions and updates
//! - delete: Removals
//! - scan: Range queries

use crate::buffer::{BufferPool, BufferPoolImpl};
use crate::error::{Result, StorageError};
use crate::page::{Cell, SlottedPage};
use crate::types::{PageId, MAX_KEY_SIZE, MAX_VALUE_SIZE};
use std::sync::Arc;

/// A disk-based B-tree
pub struct BTree {
    /// Buffer pool for page access
    buffer_pool: Arc<BufferPoolImpl>,
    /// Root page ID (0 means empty tree)
    root_page: PageId,
    /// Current height of the tree
    height: usize,
}

impl BTree {
    /// Create a new B-tree or load existing one
    pub fn new(buffer_pool: Arc<BufferPoolImpl>) -> Result<Self> {
        // Read root page and height from the persisted file header
        let root_page = buffer_pool.root_page();
        let height = buffer_pool.tree_height() as usize;

        Ok(Self {
            buffer_pool,
            root_page,
            height,
        })
    }

    /// Get the height of the tree
    pub fn height(&self) -> usize {
        self.height
    }

    /// Get the root page ID
    pub fn root_page(&self) -> PageId {
        self.root_page
    }

    /// Look up a key and return its value
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if self.root_page.value() == 0 {
            return Ok(None);
        }

        self.search(self.root_page, key)
    }

    /// Debug search - traces the path through the tree
    pub fn debug_get(&self, key: &[u8]) -> Result<Vec<String>> {
        let mut trace = Vec::new();
        if self.root_page.value() == 0 {
            trace.push("Tree is empty (root_page = 0)".to_string());
            return Ok(trace);
        }

        trace.push(format!("Searching for key: {:?}", String::from_utf8_lossy(key)));
        trace.push(format!("Root page: {}, Height: {}", self.root_page.value(), self.height));
        
        self.search_with_trace(self.root_page, key, &mut trace)?;
        Ok(trace)
    }

    fn search_with_trace(&self, page_id: PageId, key: &[u8], trace: &mut Vec<String>) -> Result<Option<Vec<u8>>> {
        let guard = self.buffer_pool.fetch_page(page_id)?;
        let page = guard.read();

        trace.push(format!("  Page {}: is_leaf={}, cell_count={}", 
            page_id.value(), page.is_leaf(), page.cell_count()));

        if page.is_leaf() {
            // Dump all keys in this page
            for i in 0..page.cell_count() {
                if let Ok(cell) = page.get_cell(i) {
                    let key_str = String::from_utf8_lossy(&cell.key);
                    trace.push(format!("    Cell {}: key={}", i, key_str));
                }
            }
            
            if let Some(idx) = page.search(key)? {
                let cell = page.get_cell(idx)?;
                trace.push(format!("  FOUND at index {}", idx));
                return Ok(Some(cell.value));
            }
            trace.push("  NOT FOUND in leaf".to_string());
            Ok(None)
        } else {
            // Dump interior node structure (new semantics)
            // right_child = keys < first separator
            trace.push(format!("    right_child={} (keys < first sep)", page.right_child().value()));
            for i in 0..page.cell_count() {
                if let Ok(cell) = page.get_cell(i) {
                    let key_str = String::from_utf8_lossy(&cell.key);
                    trace.push(format!("    Cell {}: sep={}, child={} (keys >= sep)", 
                        i, key_str, cell.left_child.value()));
                }
            }
            
            let child_id = page.find_child(key)?;
            trace.push(format!("  -> Descending to child page {}", child_id.value()));
            drop(page);
            drop(guard);
            self.search_with_trace(child_id, key, trace)
        }
    }

    /// Insert or update a key-value pair
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        // Validate key and value sizes
        if key.len() > MAX_KEY_SIZE {
            return Err(StorageError::KeyTooLarge {
                size: key.len(),
                max: MAX_KEY_SIZE,
            });
        }
        if value.len() > MAX_VALUE_SIZE {
            return Err(StorageError::ValueTooLarge {
                size: value.len(),
                max: MAX_VALUE_SIZE,
            });
        }

        if self.root_page.value() == 0 {
            // Create root page
            let (page_id, guard) = self.buffer_pool.new_page()?;
            {
                let mut page = guard.write();
                let cell = Cell::new_leaf(key.to_vec(), value.to_vec());
                page.insert_cell(&cell)?;
            }
            self.root_page = page_id;
            self.height = 1;
            // Persist the new root
            self.buffer_pool.set_root_page(page_id, self.height as u32)?;
            self.buffer_pool.flush_page(page_id)?;
            return Ok(());
        }

        // Insert into existing tree
        let result = self.insert_recursive(self.root_page, key, value)?;

        // Handle root split
        if let Some((separator, new_page_id)) = result {
            self.split_root(separator, new_page_id)?;
        }

        Ok(())
    }

    /// Delete a key from the tree
    ///
    /// Returns true if the key was found and deleted.
    pub fn delete(&mut self, key: &[u8]) -> Result<bool> {
        if self.root_page.value() == 0 {
            return Ok(false);
        }

        self.delete_recursive(self.root_page, key)
    }

    /// Scan a range of keys
    ///
    /// Returns all key-value pairs where start <= key < end.
    /// If start is None, scan from the beginning.
    /// If end is None, scan to the end.
    pub fn scan(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if self.root_page.value() == 0 {
            return Ok(Vec::new());
        }

        let mut results = Vec::new();
        self.scan_recursive(self.root_page, start, end, &mut results)?;
        Ok(results)
    }

    /// Recursive search for a key
    fn search(&self, page_id: PageId, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let guard = self.buffer_pool.fetch_page(page_id)?;
        let page = guard.read();

        if page.is_leaf() {
            // Search in leaf
            if let Some(idx) = page.search(key)? {
                let cell = page.get_cell(idx)?;
                return Ok(Some(cell.value));
            }
            Ok(None)
        } else {
            // Find child to descend into
            let child_id = page.find_child(key)?;
            drop(page);
            drop(guard);
            self.search(child_id, key)
        }
    }

    /// Recursive insert
    ///
    /// Returns Some((separator_key, new_page_id)) if a split occurred.
    fn insert_recursive(
        &self,
        page_id: PageId,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<(Vec<u8>, PageId)>> {
        let guard = self.buffer_pool.fetch_page_mut(page_id)?;

        {
            let page = guard.read();

            if page.is_leaf() {
                drop(page);
                // Insert into leaf
                return self.insert_into_leaf(guard, key, value);
            }

            // Interior node - find child
            let child_id = page.find_child(key)?;
            drop(page);
            drop(guard);

            // Recursive insert into child
            let result = self.insert_recursive(child_id, key, value)?;

            // Handle child split
            if let Some((separator, new_child_id)) = result {
                let guard = self.buffer_pool.fetch_page_mut(page_id)?;
                return self.insert_into_interior(guard, &separator, new_child_id);
            }
        }

        Ok(None)
    }

    /// Insert into a leaf page
    fn insert_into_leaf(
        &self,
        guard: crate::buffer::PageGuardMut<'_>,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<(Vec<u8>, PageId)>> {
        let cell = Cell::new_leaf(key.to_vec(), value.to_vec());
        let cell_size = cell.encoded_size();

        {
            let page = guard.read();

            // Check if key already exists
            if let Some(idx) = page.search(key)? {
                // Update existing
                drop(page);
                let mut page = guard.write();
                page.update_cell(idx, value)?;
                return Ok(None);
            }

            // Check if we have space
            if page.can_fit(cell_size) {
                drop(page);
                let mut page = guard.write();
                page.insert_cell(&cell)?;
                return Ok(None);
            }
        }

        // Need to split
        let mut page = guard.write();

        // Insert the cell first (may trigger split)
        let split_result = self.split_and_insert_leaf(&mut page, cell)?;

        Ok(Some(split_result))
    }

    /// Split a leaf page and insert a cell
    fn split_and_insert_leaf(
        &self,
        page: &mut SlottedPage,
        cell: Cell,
    ) -> Result<(Vec<u8>, PageId)> {
        // First insert the cell (page will be overfull but we handle it)
        // Actually, let's split first then figure out which side gets the new cell

        let (mut new_page, separator) = page.split()?;

        // Determine which page gets the new cell
        if cell.key.as_slice() < separator.as_slice() {
            page.insert_cell(&cell)?;
        } else {
            new_page.insert_cell(&cell)?;
        }

        // Write new page to disk
        let (new_page_id, new_guard) = self.buffer_pool.new_page()?;
        {
            let mut new_page_mut = new_guard.write();
            // Copy the data from new_page to the allocated page
            *new_page_mut = new_page;
        }

        Ok((separator, new_page_id))
    }

    /// Insert into an interior page
    fn insert_into_interior(
        &self,
        guard: crate::buffer::PageGuardMut<'_>,
        separator: &[u8],
        new_child_id: PageId,
    ) -> Result<Option<(Vec<u8>, PageId)>> {
        let cell = Cell::new_interior(separator.to_vec(), new_child_id);
        let cell_size = cell.encoded_size();

        {
            let page = guard.read();

            if page.can_fit(cell_size) {
                drop(page);
                let mut page = guard.write();
                page.insert_cell(&cell)?;

                // The new child becomes the left child of this separator
                // and the old child at that position becomes... wait, we need to handle pointers

                // Actually for interior nodes, when we insert a separator:
                // - The separator's left_child is the new_child_id
                // - The existing child to the right of insertion point remains correct

                return Ok(None);
            }
        }

        // Need to split interior node
        let mut page = guard.write();
        let split_result = self.split_and_insert_interior(&mut page, cell)?;

        Ok(Some(split_result))
    }

    /// Split an interior page and insert a cell
    fn split_and_insert_interior(
        &self,
        page: &mut SlottedPage,
        cell: Cell,
    ) -> Result<(Vec<u8>, PageId)> {
        let (mut new_page, separator) = page.split()?;

        // Determine which page gets the new cell
        if cell.key.as_slice() < separator.as_slice() {
            page.insert_cell(&cell)?;
        } else {
            new_page.insert_cell(&cell)?;
        }

        // Write new page to disk
        let (new_page_id, new_guard) = self.buffer_pool.new_page()?;
        {
            let mut new_page_ref = new_guard.write();
            // Need to make this an interior page
            let mut interior_page = SlottedPage::new_interior();
            interior_page.set_right_child(new_page.right_child());
            for i in 0..new_page.cell_count() {
                let c = new_page.get_cell(i)?;
                interior_page.insert_cell(&Cell::new_interior(c.key, c.left_child))?;
            }
            *new_page_ref = interior_page;
        }

        Ok((separator, new_page_id))
    }

    /// Split the root, creating a new root
    fn split_root(&mut self, separator: Vec<u8>, new_child_id: PageId) -> Result<()> {
        let old_root_id = self.root_page;

        // Create new root
        // After split: old_root has keys < separator, new_child has keys >= separator
        let (new_root_id, guard) = self.buffer_pool.new_page()?;
        {
            let mut new_root = guard.write();
            // Convert to interior page
            *new_root = SlottedPage::new_interior();
            
            // In our semantics:
            // - right_child stores keys < first separator (old_root)
            // - cell.left_child stores keys >= separator (new_child)
            new_root.set_right_child(old_root_id);
            let cell = Cell::new_interior(separator, new_child_id);
            new_root.insert_cell(&cell)?;
        }

        self.root_page = new_root_id;
        self.height += 1;

        // Persist the new root to the file header
        self.buffer_pool.set_root_page(new_root_id, self.height as u32)?;
        self.buffer_pool.flush_page(new_root_id)?;

        Ok(())
    }

    /// Recursive delete
    fn delete_recursive(&mut self, page_id: PageId, key: &[u8]) -> Result<bool> {
        let guard = self.buffer_pool.fetch_page_mut(page_id)?;
        let page = guard.read();

        if page.is_leaf() {
            drop(page);
            let mut page = guard.write();

            if let Some(idx) = page.search(key)? {
                page.delete_cell(idx)?;
                return Ok(true);
            }
            return Ok(false);
        }

        // Interior node - find child
        let child_id = page.find_child(key)?;
        drop(page);
        drop(guard);

        // Recursive delete
        self.delete_recursive(child_id, key)

        // Note: In v1, we don't rebalance after deletion.
        // A production implementation would merge underflowing nodes.
    }

    /// Recursive scan
    fn scan_recursive(
        &self,
        page_id: PageId,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        results: &mut Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<()> {
        let guard = self.buffer_pool.fetch_page(page_id)?;
        let page = guard.read();

        if page.is_leaf() {
            // Scan all cells in range
            for i in 0..page.cell_count() {
                let cell = page.get_cell(i)?;

                // Check start bound
                if let Some(s) = start {
                    if cell.key.as_slice() < s {
                        continue;
                    }
                }

                // Check end bound
                if let Some(e) = end {
                    if cell.key.as_slice() >= e {
                        break;
                    }
                }

                results.push((cell.key, cell.value));
            }
        } else {
            // Interior node traversal with new semantics:
            // - right_child contains keys < first separator
            // - cell[i].left_child contains keys >= cell[i].key and < cell[i+1].key
            // - last cell's left_child contains keys >= last separator
            
            let cell_count = page.cell_count();
            
            // Collect children in order: right_child first (smallest keys), then cells
            let mut children_to_scan: Vec<PageId> = Vec::new();
            
            // First: right_child (keys < first separator, or all keys if no cells)
            let right_child = page.right_child();
            let first_sep = if cell_count > 0 {
                Some(page.get_cell(0)?.key.clone())
            } else {
                None
            };
            
            // Check if right_child should be scanned
            let scan_right_child = match (start, &first_sep) {
                (None, _) => true, // No start bound
                (Some(_), None) => true, // No separator, scan everything
                (Some(s), Some(fs)) => s < fs.as_slice(), // Start < first sep
            };
            
            if scan_right_child {
                // Also check end bound
                let include_right = end.is_none() || first_sep.is_none() || 
                    end.unwrap() > [].as_slice(); // always true unless we want to optimize
                if include_right {
                    children_to_scan.push(right_child);
                }
            }
            
            // Then each cell's child
            for i in 0..cell_count {
                let cell = page.get_cell(i)?;
                let cell_lower = cell.key.as_slice();
                let cell_upper = if i + 1 < cell_count {
                    Some(page.get_cell(i + 1)?.key.clone())
                } else {
                    None // Last cell - no upper bound
                };
                
                // Check if this child's range overlaps with [start, end)
                let overlaps = match (start, end, &cell_upper) {
                    (None, None, _) => true,
                    (Some(s), None, _) => {
                        // Start bound only: overlaps if cell covers any keys >= s
                        cell_upper.is_none() || cell_upper.as_ref().unwrap().as_slice() > s
                    }
                    (None, Some(e), _) => {
                        // End bound only: overlaps if cell covers any keys < e
                        cell_lower < e
                    }
                    (Some(s), Some(e), _) => {
                        // Both bounds: check overlap
                        let range_start_ok = cell_upper.is_none() || cell_upper.as_ref().unwrap().as_slice() > s;
                        let range_end_ok = cell_lower < e;
                        range_start_ok && range_end_ok
                    }
                };
                
                if overlaps {
                    children_to_scan.push(cell.left_child);
                }
            }
            
            drop(page);
            drop(guard);
            
            // Scan children
            for child_id in children_to_scan {
                self.scan_recursive(child_id, start, end, results)?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::DiskManagerImpl;
    use tempfile::tempdir;

    fn create_test_btree() -> Result<(BTree, tempfile::TempDir)> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let dm = Arc::new(DiskManagerImpl::open(&path, false)?);
        let pool = Arc::new(BufferPoolImpl::new(dm, 100));
        let btree = BTree::new(pool)?;
        Ok((btree, dir))
    }

    #[test]
    fn test_btree_empty() -> Result<()> {
        let (btree, _dir) = create_test_btree()?;
        assert_eq!(btree.get(b"key")?, None);
        Ok(())
    }

    #[test]
    fn test_btree_single_insert() -> Result<()> {
        let (mut btree, _dir) = create_test_btree()?;

        btree.put(b"hello", b"world")?;
        assert_eq!(btree.get(b"hello")?, Some(b"world".to_vec()));
        assert_eq!(btree.get(b"other")?, None);

        Ok(())
    }

    #[test]
    fn test_btree_multiple_inserts() -> Result<()> {
        let (mut btree, _dir) = create_test_btree()?;

        btree.put(b"c", b"3")?;
        btree.put(b"a", b"1")?;
        btree.put(b"b", b"2")?;

        assert_eq!(btree.get(b"a")?, Some(b"1".to_vec()));
        assert_eq!(btree.get(b"b")?, Some(b"2".to_vec()));
        assert_eq!(btree.get(b"c")?, Some(b"3".to_vec()));

        Ok(())
    }

    #[test]
    fn test_btree_update() -> Result<()> {
        let (mut btree, _dir) = create_test_btree()?;

        btree.put(b"key", b"value1")?;
        assert_eq!(btree.get(b"key")?, Some(b"value1".to_vec()));

        btree.put(b"key", b"value2")?;
        assert_eq!(btree.get(b"key")?, Some(b"value2".to_vec()));

        Ok(())
    }

    #[test]
    fn test_btree_delete() -> Result<()> {
        let (mut btree, _dir) = create_test_btree()?;

        btree.put(b"key", b"value")?;
        assert!(btree.delete(b"key")?);
        assert_eq!(btree.get(b"key")?, None);
        assert!(!btree.delete(b"key")?); // Already deleted

        Ok(())
    }

    #[test]
    fn test_btree_scan() -> Result<()> {
        let (mut btree, _dir) = create_test_btree()?;

        btree.put(b"a", b"1")?;
        btree.put(b"b", b"2")?;
        btree.put(b"c", b"3")?;
        btree.put(b"d", b"4")?;

        // Full scan
        let all = btree.scan(None, None)?;
        assert_eq!(all.len(), 4);

        // Range scan
        let range = btree.scan(Some(b"b"), Some(b"d"))?;
        assert_eq!(range.len(), 2);
        assert_eq!(range[0].0, b"b".to_vec());
        assert_eq!(range[1].0, b"c".to_vec());

        Ok(())
    }

    #[test]
    fn test_btree_many_inserts() -> Result<()> {
        let (mut btree, _dir) = create_test_btree()?;

        // Insert enough keys to cause splits
        for i in 0..100 {
            let key = format!("key{:03}", i);
            let value = format!("value{}", i);
            btree.put(key.as_bytes(), value.as_bytes())?;
        }

        // Verify all keys
        for i in 0..100 {
            let key = format!("key{:03}", i);
            let expected = format!("value{}", i);
            let result = btree.get(key.as_bytes())?;
            assert_eq!(result, Some(expected.into_bytes()), "Failed for key {}", key);
        }

        Ok(())
    }
}
