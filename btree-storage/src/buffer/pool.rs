//! Buffer pool implementation.
//!
//! The buffer pool manages a fixed number of in-memory page frames,
//! caching pages read from disk and writing dirty pages back.

use crate::buffer::lru::LruCache;
use crate::error::{Result, StorageError};
use crate::page::SlottedPage;
use crate::storage::DiskManager;
use crate::types::PageId;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::collections::HashMap;
use std::sync::Arc;

/// Trait for buffer pool operations
pub trait BufferPool: Send + Sync {
    /// Fetch a page from the buffer pool
    fn fetch_page(&self, page_id: PageId) -> Result<PageGuard<'_>>;

    /// Fetch a page for writing
    fn fetch_page_mut(&self, page_id: PageId) -> Result<PageGuardMut<'_>>;

    /// Allocate a new page
    fn new_page(&self) -> Result<(PageId, PageGuardMut<'_>)>;

    /// Flush a specific page to disk
    fn flush_page(&self, page_id: PageId) -> Result<()>;

    /// Flush all dirty pages to disk
    fn flush_all(&self) -> Result<()>;

    /// Deallocate a page
    fn free_page(&self, page_id: PageId) -> Result<()>;

    /// Get the total number of pages in the database
    fn page_count(&self) -> usize;

    /// Get the buffer pool capacity
    fn capacity(&self) -> usize;

    /// Get the root page ID from the file header
    fn root_page(&self) -> PageId;

    /// Get the tree height from the file header
    fn tree_height(&self) -> u32;

    /// Set the root page and height in the file header
    fn set_root_page(&self, page_id: PageId, height: u32) -> Result<()>;
}

/// A frame in the buffer pool
struct BufferFrame {
    /// The page data
    page: SlottedPage,
    /// Whether the page has been modified
    dirty: bool,
    /// Pin count (number of active references)
    pin_count: u32,
}

impl BufferFrame {
    fn new(page: SlottedPage) -> Self {
        Self {
            page,
            dirty: false,
            pin_count: 0,
        }
    }
}

/// Buffer pool implementation
pub struct BufferPoolImpl {
    /// The disk manager for I/O
    disk_manager: Arc<dyn DiskManager>,
    /// Cached frames indexed by page ID
    frames: RwLock<HashMap<PageId, Arc<RwLock<BufferFrame>>>>,
    /// LRU cache for eviction
    lru: RwLock<LruCache>,
    /// Maximum number of frames
    capacity: usize,
}

impl BufferPoolImpl {
    /// Create a new buffer pool
    pub fn new(disk_manager: Arc<dyn DiskManager>, capacity: usize) -> Self {
        Self {
            disk_manager,
            frames: RwLock::new(HashMap::with_capacity(capacity)),
            lru: RwLock::new(LruCache::new(capacity)),
            capacity,
        }
    }

    /// Get or load a frame for a page
    fn get_frame(&self, page_id: PageId) -> Result<Arc<RwLock<BufferFrame>>> {
        // Check if already in buffer
        {
            let frames = self.frames.read();
            if let Some(frame) = frames.get(&page_id) {
                let mut lru = self.lru.write();
                lru.access(page_id.value());
                return Ok(Arc::clone(frame));
            }
        }

        // Need to load from disk
        self.load_page(page_id)
    }

    /// Load a page from disk into the buffer pool
    fn load_page(&self, page_id: PageId) -> Result<Arc<RwLock<BufferFrame>>> {
        // Read from disk
        let page_buf = self.disk_manager.read_page(page_id)?;
        let page = SlottedPage::from_bytes(page_buf.as_bytes())?;

        // Evict if necessary
        {
            let frames = self.frames.read();
            if frames.len() >= self.capacity {
                drop(frames);
                self.evict_one()?;
            }
        }

        // Insert into buffer
        let frame = Arc::new(RwLock::new(BufferFrame::new(page)));
        {
            let mut frames = self.frames.write();
            frames.insert(page_id, Arc::clone(&frame));
        }
        {
            let mut lru = self.lru.write();
            lru.access(page_id.value());
        }

        Ok(frame)
    }

    /// Evict one page from the buffer pool
    fn evict_one(&self) -> Result<()> {
        let mut lru = self.lru.write();

        // Find an unpinned page to evict
        loop {
            let page_id = match lru.pop_lru() {
                Some(id) => PageId::new(id),
                None => return Err(StorageError::BufferPoolExhausted),
            };

            let frames = self.frames.read();
            if let Some(frame) = frames.get(&page_id) {
                let frame_guard = frame.read();
                if frame_guard.pin_count == 0 {
                    drop(frame_guard);
                    drop(frames);

                    // Write back if dirty
                    self.flush_page(page_id)?;

                    // Remove from buffer
                    let mut frames = self.frames.write();
                    frames.remove(&page_id);
                    return Ok(());
                }
                // Page is pinned, try next
                lru.access(page_id.value()); // Put back in LRU
            }
        }
    }
}

impl BufferPool for BufferPoolImpl {
    fn fetch_page(&self, page_id: PageId) -> Result<PageGuard<'_>> {
        let frame = self.get_frame(page_id)?;
        {
            let mut f = frame.write();
            f.pin_count += 1;
        }
        Ok(PageGuard {
            page_id,
            frame,
            pool: self,
        })
    }

    fn fetch_page_mut(&self, page_id: PageId) -> Result<PageGuardMut<'_>> {
        let frame = self.get_frame(page_id)?;
        {
            let mut f = frame.write();
            f.pin_count += 1;
            f.dirty = true;
        }
        Ok(PageGuardMut {
            page_id,
            frame,
            pool: self,
        })
    }

    fn new_page(&self) -> Result<(PageId, PageGuardMut<'_>)> {
        // Allocate from disk manager
        let page_id = self.disk_manager.allocate_page()?;

        // Create a new leaf page by default
        let page = SlottedPage::new_leaf();
        let frame = Arc::new(RwLock::new(BufferFrame {
            page,
            dirty: true,
            pin_count: 1,
        }));

        {
            let mut frames = self.frames.write();
            frames.insert(page_id, Arc::clone(&frame));
        }
        {
            let mut lru = self.lru.write();
            lru.access(page_id.value());
        }

        Ok((
            page_id,
            PageGuardMut {
                page_id,
                frame,
                pool: self,
            },
        ))
    }

    fn flush_page(&self, page_id: PageId) -> Result<()> {
        let frames = self.frames.read();
        if let Some(frame) = frames.get(&page_id) {
            let mut frame_guard = frame.write();
            if frame_guard.dirty {
                let data = frame_guard.page.as_bytes();
                self.disk_manager.write_page(page_id, data)?;
                frame_guard.dirty = false;
            }
        }
        Ok(())
    }

    fn flush_all(&self) -> Result<()> {
        let frames = self.frames.read();
        for (&page_id, frame) in frames.iter() {
            let mut frame_guard = frame.write();
            if frame_guard.dirty {
                let data = frame_guard.page.as_bytes();
                self.disk_manager.write_page(page_id, data)?;
                frame_guard.dirty = false;
            }
        }
        self.disk_manager.sync()?;
        Ok(())
    }

    fn free_page(&self, page_id: PageId) -> Result<()> {
        // Remove from buffer
        {
            let mut frames = self.frames.write();
            frames.remove(&page_id);
        }
        {
            let mut lru = self.lru.write();
            lru.remove(page_id.value());
        }

        // Tell disk manager to add to free list
        self.disk_manager.deallocate_page(page_id)?;
        Ok(())
    }

    fn page_count(&self) -> usize {
        self.disk_manager.header().page_count as usize
    }

    fn capacity(&self) -> usize {
        self.capacity
    }

    fn root_page(&self) -> PageId {
        self.disk_manager.header().root_page
    }

    fn tree_height(&self) -> u32 {
        self.disk_manager.header().tree_height
    }

    fn set_root_page(&self, page_id: PageId, height: u32) -> Result<()> {
        self.disk_manager.set_root_page(page_id, height)
    }
}

/// RAII guard for read access to a page
pub struct PageGuard<'a> {
    page_id: PageId,
    frame: Arc<RwLock<BufferFrame>>,
    pool: &'a BufferPoolImpl,
}

impl<'a> PageGuard<'a> {
    /// Get the page ID
    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    /// Get a read lock on the page
    pub fn read(&self) -> PageRef<'_> {
        PageRef {
            guard: self.frame.read(),
        }
    }
}

impl<'a> Drop for PageGuard<'a> {
    fn drop(&mut self) {
        let mut frame = self.frame.write();
        frame.pin_count = frame.pin_count.saturating_sub(1);
        // Update LRU
        let mut lru = self.pool.lru.write();
        lru.access(self.page_id.value());
    }
}

/// Reference to a page (through a read lock)
pub struct PageRef<'a> {
    guard: RwLockReadGuard<'a, BufferFrame>,
}

impl<'a> std::ops::Deref for PageRef<'a> {
    type Target = SlottedPage;

    fn deref(&self) -> &Self::Target {
        &self.guard.page
    }
}

/// RAII guard for write access to a page
pub struct PageGuardMut<'a> {
    page_id: PageId,
    frame: Arc<RwLock<BufferFrame>>,
    pool: &'a BufferPoolImpl,
}

impl<'a> PageGuardMut<'a> {
    /// Get the page ID
    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    /// Get a write lock on the page
    pub fn write(&self) -> PageRefMut<'_> {
        let mut guard = self.frame.write();
        guard.dirty = true;
        PageRefMut { guard }
    }

    /// Get a read lock on the page
    pub fn read(&self) -> PageRef<'_> {
        PageRef {
            guard: self.frame.read(),
        }
    }
}

impl<'a> Drop for PageGuardMut<'a> {
    fn drop(&mut self) {
        let mut frame = self.frame.write();
        frame.pin_count = frame.pin_count.saturating_sub(1);
        // Update LRU
        let mut lru = self.pool.lru.write();
        lru.access(self.page_id.value());
    }
}

/// Mutable reference to a page (through a write lock)
pub struct PageRefMut<'a> {
    guard: RwLockWriteGuard<'a, BufferFrame>,
}

impl<'a> std::ops::Deref for PageRefMut<'a> {
    type Target = SlottedPage;

    fn deref(&self) -> &Self::Target {
        &self.guard.page
    }
}

impl<'a> std::ops::DerefMut for PageRefMut<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard.page
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::Cell;
    use crate::storage::DiskManagerImpl;
    use tempfile::tempdir;

    #[test]
    fn test_buffer_pool_new_page() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let dm = Arc::new(DiskManagerImpl::open(&path, false)?);
        let pool = BufferPoolImpl::new(dm, 10);

        let (page_id, guard) = pool.new_page()?;
        assert_eq!(page_id, PageId::new(1));

        {
            let mut page = guard.write();
            page.insert_cell(&Cell::new_leaf(b"key".to_vec(), b"value".to_vec()))?;
        }

        pool.flush_all()?;

        Ok(())
    }

    #[test]
    fn test_buffer_pool_fetch() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let dm = Arc::new(DiskManagerImpl::open(&path, false)?);
        let pool = BufferPoolImpl::new(dm, 10);

        // Create a page
        let page_id = {
            let (page_id, guard) = pool.new_page()?;
            {
                let mut page = guard.write();
                page.insert_cell(&Cell::new_leaf(b"hello".to_vec(), b"world".to_vec()))?;
            }
            page_id
        };

        pool.flush_all()?;

        // Fetch and verify
        let guard = pool.fetch_page(page_id)?;
        let page = guard.read();
        let cell = page.get_cell(0)?;
        assert_eq!(cell.key, b"hello");
        assert_eq!(cell.value, b"world");

        Ok(())
    }
}
