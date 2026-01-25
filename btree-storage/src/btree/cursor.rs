//! B-tree cursor for iteration.
//!
//! The cursor provides a way to iterate over key-value pairs in the B-tree
//! in sorted order. It maintains a stack of (page_id, cell_index) pairs
//! representing the current position in the tree.

use crate::buffer::BufferPool;
use crate::error::Result;
use crate::types::PageId;
use std::sync::Arc;

/// A cursor for iterating over B-tree entries
pub struct Cursor<P: BufferPool> {
    /// The buffer pool for page access
    buffer_pool: Arc<P>,
    /// Stack of (page_id, cell_index) representing path to current position
    stack: Vec<(PageId, usize)>,
    /// Whether the cursor is positioned at a valid entry
    valid: bool,
}

impl<P: BufferPool> Cursor<P> {
    /// Create a new cursor starting at the first entry
    pub fn new(buffer_pool: Arc<P>, root_page: PageId) -> Result<Self> {
        let mut cursor = Self {
            buffer_pool,
            stack: Vec::new(),
            valid: false,
        };

        if root_page.value() != 0 {
            cursor.seek_to_first(root_page)?;
        }

        Ok(cursor)
    }

    /// Create a cursor positioned at a specific key (or the first key >= target)
    pub fn seek(buffer_pool: Arc<P>, root_page: PageId, key: &[u8]) -> Result<Self> {
        let mut cursor = Self {
            buffer_pool,
            stack: Vec::new(),
            valid: false,
        };

        if root_page.value() != 0 {
            cursor.seek_to_key(root_page, key)?;
        }

        Ok(cursor)
    }

    /// Check if the cursor is positioned at a valid entry
    pub fn is_valid(&self) -> bool {
        self.valid
    }

    /// Get the current key-value pair
    pub fn current(&self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        if !self.valid || self.stack.is_empty() {
            return Ok(None);
        }

        let (page_id, cell_idx) = self.stack.last().unwrap();
        let guard = self.buffer_pool.fetch_page(*page_id)?;
        let page = guard.read();

        if *cell_idx >= page.cell_count() {
            return Ok(None);
        }

        let cell = page.get_cell(*cell_idx)?;
        Ok(Some((cell.key, cell.value)))
    }

    /// Move to the next entry
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<bool> {
        if !self.valid || self.stack.is_empty() {
            return Ok(false);
        }

        let (page_id, cell_idx) = self.stack.last_mut().unwrap();
        let guard = self.buffer_pool.fetch_page(*page_id)?;
        let page = guard.read();

        // Move to next cell in current page
        *cell_idx += 1;

        if *cell_idx < page.cell_count() {
            // Still have cells in this page
            return Ok(true);
        }

        // Need to move to next leaf page
        // For now, we pop and move up the tree
        drop(page);
        drop(guard);
        self.stack.pop();

        while let Some((parent_page_id, parent_idx)) = self.stack.last_mut() {
            let guard = self.buffer_pool.fetch_page(*parent_page_id)?;
            let page = guard.read();

            if page.is_interior() {
                // Try to move to next child
                *parent_idx += 1;

                if *parent_idx < page.cell_count() {
                    // Go to left child of next separator
                    let cell = page.get_cell(*parent_idx)?;
                    drop(page);
                    drop(guard);
                    return self.descend_to_leftmost(cell.left_child);
                } else {
                    // Go to right child
                    let right_child = page.right_child();
                    drop(page);
                    drop(guard);
                    return self.descend_to_leftmost(right_child);
                }
            }

            drop(page);
            drop(guard);
            self.stack.pop();
        }

        self.valid = false;
        Ok(false)
    }

    /// Seek to the first entry in the tree
    fn seek_to_first(&mut self, root_page: PageId) -> Result<()> {
        self.descend_to_leftmost(root_page)?;
        Ok(())
    }

    /// Descend to the leftmost leaf entry starting from a page
    fn descend_to_leftmost(&mut self, page_id: PageId) -> Result<bool> {
        let mut current = page_id;

        loop {
            let guard = self.buffer_pool.fetch_page(current)?;
            let page = guard.read();

            if page.is_leaf() {
                if page.cell_count() > 0 {
                    self.stack.push((current, 0));
                    self.valid = true;
                    return Ok(true);
                } else {
                    self.valid = false;
                    return Ok(false);
                }
            }

            // Interior page - go to leftmost child
            if page.cell_count() > 0 {
                let cell = page.get_cell(0)?;
                self.stack.push((current, 0));
                current = cell.left_child;
            } else {
                // Only right child
                current = page.right_child();
            }
        }
    }

    /// Seek to a specific key (or first key >= target)
    fn seek_to_key(&mut self, root_page: PageId, key: &[u8]) -> Result<()> {
        let mut current = root_page;

        loop {
            let guard = self.buffer_pool.fetch_page(current)?;
            let page = guard.read();

            if page.is_leaf() {
                // Binary search for the key or first key >= target
                let cell_count = page.cell_count();
                for i in 0..cell_count {
                    let cell = page.get_cell(i)?;
                    if cell.key.as_slice() >= key {
                        self.stack.push((current, i));
                        self.valid = true;
                        return Ok(());
                    }
                }
                // No key >= target in this leaf
                self.valid = false;
                return Ok(());
            }

            // Interior page - find correct child
            let child = page.find_child(key)?;
            let cell_count = page.cell_count();

            // Find which separator we passed (for stack tracking)
            for i in 0..cell_count {
                let cell = page.get_cell(i)?;
                if key < cell.key.as_slice() {
                    self.stack.push((current, i));
                    break;
                }
                if i == cell_count - 1 {
                    self.stack.push((current, cell_count));
                }
            }

            current = child;
        }
    }
}
