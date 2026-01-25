//! B-tree implementation.
//!
//! This module provides a disk-based B-tree that supports:
//! - Point lookups (get)
//! - Insertions (put)
//! - Deletions (delete)
//! - Range scans

mod cursor;
mod tree;

pub use cursor::Cursor;
pub use tree::BTree;
