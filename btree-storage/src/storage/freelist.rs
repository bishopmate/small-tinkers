//! Free list management.
//!
//! Tracks deallocated pages that can be reused for new allocations.
//! In v1, we use a simple in-memory list. A production implementation
//! would persist free page information to disk.

use crate::types::PageId;
use std::collections::VecDeque;

/// Manages free pages for reuse
#[derive(Debug, Default)]
pub struct FreeList {
    /// Queue of free page IDs
    pages: VecDeque<PageId>,
}

impl FreeList {
    /// Create a new empty free list
    pub fn new() -> Self {
        Self {
            pages: VecDeque::new(),
        }
    }

    /// Add a page to the free list
    pub fn push(&mut self, page_id: PageId) {
        self.pages.push_back(page_id);
    }

    /// Get a free page, if available
    pub fn pop(&mut self) -> Option<PageId> {
        self.pages.pop_front()
    }

    /// Get the number of free pages
    pub fn len(&self) -> usize {
        self.pages.len()
    }

    /// Check if the free list is empty
    pub fn is_empty(&self) -> bool {
        self.pages.is_empty()
    }

    /// Get all free page IDs (for persistence)
    pub fn page_ids(&self) -> impl Iterator<Item = PageId> + '_ {
        self.pages.iter().copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_freelist_operations() {
        let mut fl = FreeList::new();
        assert!(fl.is_empty());
        assert_eq!(fl.pop(), None);

        fl.push(PageId::new(5));
        fl.push(PageId::new(10));
        assert_eq!(fl.len(), 2);

        assert_eq!(fl.pop(), Some(PageId::new(5)));
        assert_eq!(fl.pop(), Some(PageId::new(10)));
        assert_eq!(fl.pop(), None);
    }
}
