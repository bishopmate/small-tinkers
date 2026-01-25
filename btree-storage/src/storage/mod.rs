//! Storage layer: disk I/O and page management.
//!
//! This module provides abstractions for reading and writing pages to disk,
//! managing the database file format, and tracking free pages.

mod disk_manager;
mod file_header;
mod freelist;

pub use disk_manager::{DiskManager, DiskManagerImpl};
pub use file_header::FileHeader;
pub use freelist::FreeList;
