//! Disk manager implementation.
//!
//! The disk manager is responsible for reading and writing pages to the
//! database file. It abstracts the file I/O operations behind a trait
//! so that the rest of the system can be tested with mock implementations.

use crate::error::{Result, StorageError};
use crate::page::PageBuf;
use crate::storage::{FileHeader, FreeList};
use crate::types::{PageId, PAGE_SIZE};
use parking_lot::RwLock;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

/// Trait for disk I/O operations
///
/// This abstraction allows swapping the storage backend or mocking for tests.
pub trait DiskManager: Send + Sync {
    /// Read a page from disk
    fn read_page(&self, page_id: PageId) -> Result<PageBuf>;

    /// Write a page to disk
    fn write_page(&self, page_id: PageId, data: &[u8]) -> Result<()>;

    /// Allocate a new page
    fn allocate_page(&self) -> Result<PageId>;

    /// Deallocate a page (add to free list)
    fn deallocate_page(&self, page_id: PageId) -> Result<()>;

    /// Sync all data to disk
    fn sync(&self) -> Result<()>;

    /// Get the file header
    fn header(&self) -> FileHeader;

    /// Update the root page
    fn set_root_page(&self, page_id: PageId, height: u32) -> Result<()>;
}

/// File-based disk manager implementation
pub struct DiskManagerImpl {
    /// The database file
    file: RwLock<File>,
    /// The file header (cached)
    header: RwLock<FileHeader>,
    /// Free list for page reuse
    free_list: RwLock<FreeList>,
    /// Whether to sync on each write
    sync_on_write: bool,
}

impl DiskManagerImpl {
    /// Open or create a database file
    pub fn open(path: &Path, sync_on_write: bool) -> Result<Self> {
        let exists = path.exists();

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        let header = if exists && file.metadata()?.len() >= PAGE_SIZE as u64 {
            // Read existing header
            let mut file_ref = &file;
            let mut buf = vec![0u8; PAGE_SIZE];
            file_ref.read_exact(&mut buf)?;
            FileHeader::read(&buf)?
        } else {
            // Create new database
            let header = FileHeader::new();
            let mut buf = vec![0u8; PAGE_SIZE];
            header.write(&mut buf);

            let mut file_ref = &file;
            file_ref.seek(SeekFrom::Start(0))?;
            file_ref.write_all(&buf)?;
            file_ref.sync_all()?;

            header
        };

        Ok(Self {
            file: RwLock::new(file),
            header: RwLock::new(header),
            free_list: RwLock::new(FreeList::new()),
            sync_on_write,
        })
    }

    /// Flush the header to disk
    fn flush_header(&self) -> Result<()> {
        let header = self.header.read();
        let mut buf = vec![0u8; PAGE_SIZE];
        header.write(&mut buf);

        let mut file = self.file.write();
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&buf)?;

        if self.sync_on_write {
            file.sync_data()?;
        }

        Ok(())
    }
}

impl DiskManager for DiskManagerImpl {
    fn read_page(&self, page_id: PageId) -> Result<PageBuf> {
        if page_id.value() == 0 {
            return Err(StorageError::invalid_operation(
                "cannot read header page directly",
            ));
        }

        let header = self.header.read();
        if page_id.value() >= header.page_count {
            return Err(StorageError::PageNotFound(page_id));
        }
        drop(header);

        let offset = page_id.file_offset(PAGE_SIZE);
        let mut buf = vec![0u8; PAGE_SIZE];

        let mut file = self.file.write();
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut buf)?;

        Ok(PageBuf::from_bytes(&buf))
    }

    fn write_page(&self, page_id: PageId, data: &[u8]) -> Result<()> {
        if page_id.value() == 0 {
            return Err(StorageError::invalid_operation(
                "cannot write header page directly",
            ));
        }

        if data.len() != PAGE_SIZE {
            return Err(StorageError::invalid_operation(format!(
                "page data must be {} bytes, got {}",
                PAGE_SIZE,
                data.len()
            )));
        }

        let offset = page_id.file_offset(PAGE_SIZE);

        let mut file = self.file.write();
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(data)?;

        if self.sync_on_write {
            file.sync_data()?;
        }

        Ok(())
    }

    fn allocate_page(&self) -> Result<PageId> {
        // First try the free list
        {
            let mut free_list = self.free_list.write();
            if let Some(page_id) = free_list.pop() {
                return Ok(page_id);
            }
        }

        // Allocate a new page
        let page_id = {
            let mut header = self.header.write();
            header.allocate_page()
        };

        // Extend the file
        let offset = page_id.file_offset(PAGE_SIZE);
        let zeros = vec![0u8; PAGE_SIZE];

        let mut file = self.file.write();
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&zeros)?;

        // Update header on disk
        drop(file);
        self.flush_header()?;

        Ok(page_id)
    }

    fn deallocate_page(&self, page_id: PageId) -> Result<()> {
        if page_id.value() == 0 {
            return Err(StorageError::invalid_operation(
                "cannot deallocate header page",
            ));
        }

        let mut free_list = self.free_list.write();
        free_list.push(page_id);

        // Update header
        {
            let mut header = self.header.write();
            header.free_page_count = free_list.len() as u32;
            header.first_free_page = page_id;
        }

        self.flush_header()?;

        Ok(())
    }

    fn sync(&self) -> Result<()> {
        self.flush_header()?;
        let file = self.file.write();
        file.sync_all()?;
        Ok(())
    }

    fn header(&self) -> FileHeader {
        *self.header.read()
    }

    fn set_root_page(&self, page_id: PageId, height: u32) -> Result<()> {
        {
            let mut header = self.header.write();
            header.root_page = page_id;
            header.tree_height = height;
        }
        self.flush_header()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_create_new_database() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let dm = DiskManagerImpl::open(&path, false)?;
        let header = dm.header();

        assert_eq!(header.page_count, 1);
        assert_eq!(header.root_page, PageId::new(0));
        assert_eq!(header.page_size, PAGE_SIZE as u32);

        Ok(())
    }

    #[test]
    fn test_allocate_and_write_page() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let dm = DiskManagerImpl::open(&path, false)?;

        let page_id = dm.allocate_page()?;
        assert_eq!(page_id, PageId::new(1));

        let mut data = vec![0u8; PAGE_SIZE];
        data[0..5].copy_from_slice(b"hello");
        dm.write_page(page_id, &data)?;

        let read_data = dm.read_page(page_id)?;
        assert_eq!(&read_data[0..5], b"hello");

        Ok(())
    }

    #[test]
    fn test_reopen_database() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        // Create and write
        {
            let dm = DiskManagerImpl::open(&path, true)?;
            let page_id = dm.allocate_page()?;
            let mut data = vec![0u8; PAGE_SIZE];
            data[0..4].copy_from_slice(b"test");
            dm.write_page(page_id, &data)?;
            dm.set_root_page(page_id, 1)?;
        }

        // Reopen and verify
        {
            let dm = DiskManagerImpl::open(&path, false)?;
            let header = dm.header();
            assert_eq!(header.page_count, 2);
            assert_eq!(header.root_page, PageId::new(1));

            let read_data = dm.read_page(PageId::new(1))?;
            assert_eq!(&read_data[0..4], b"test");
        }

        Ok(())
    }

    #[test]
    fn test_free_list() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let dm = DiskManagerImpl::open(&path, false)?;

        let p1 = dm.allocate_page()?;
        let p2 = dm.allocate_page()?;
        let p3 = dm.allocate_page()?;

        assert_eq!(p1, PageId::new(1));
        assert_eq!(p2, PageId::new(2));
        assert_eq!(p3, PageId::new(3));

        // Deallocate p2
        dm.deallocate_page(p2)?;

        // Next allocation should reuse p2
        let p4 = dm.allocate_page()?;
        assert_eq!(p4, PageId::new(2));

        Ok(())
    }
}
