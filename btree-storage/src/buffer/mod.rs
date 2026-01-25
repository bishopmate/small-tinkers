//! Buffer pool: in-memory page cache with LRU eviction.
//!
//! The buffer pool caches pages in memory to reduce disk I/O.
//! It uses an LRU (Least Recently Used) eviction policy.

mod lru;
mod pool;

pub use pool::{BufferPool, BufferPoolImpl, PageGuard, PageGuardMut};
