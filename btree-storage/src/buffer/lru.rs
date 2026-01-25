//! LRU (Least Recently Used) cache implementation.

use std::collections::HashMap;

/// A simple LRU cache that tracks page access order
pub struct LruCache {
    /// Maps page ID to its position in the access order
    positions: HashMap<u32, usize>,
    /// Doubly-linked list nodes for O(1) removal
    order: Vec<LruNode>,
    /// Head of the list (most recently used)
    head: Option<usize>,
    /// Tail of the list (least recently used)
    tail: Option<usize>,
    /// Free list of node indices
    free_slots: Vec<usize>,
}

#[derive(Clone, Copy)]
struct LruNode {
    page_id: u32,
    prev: Option<usize>,
    next: Option<usize>,
    active: bool,
}

impl LruCache {
    /// Create a new LRU cache with the given capacity
    pub fn new(capacity: usize) -> Self {
        Self {
            positions: HashMap::with_capacity(capacity),
            order: Vec::with_capacity(capacity),
            head: None,
            tail: None,
            free_slots: Vec::new(),
        }
    }

    /// Record access to a page (moves it to front)
    pub fn access(&mut self, page_id: u32) {
        if let Some(&pos) = self.positions.get(&page_id) {
            // Already in cache, move to front
            self.move_to_front(pos);
        } else {
            // New entry, add to front
            self.insert(page_id);
        }
    }

    /// Remove a page from the cache
    pub fn remove(&mut self, page_id: u32) {
        if let Some(pos) = self.positions.remove(&page_id) {
            self.unlink(pos);
            self.order[pos].active = false;
            self.free_slots.push(pos);
        }
    }

    /// Get the least recently used page ID
    pub fn lru(&self) -> Option<u32> {
        self.tail.map(|pos| self.order[pos].page_id)
    }

    /// Pop the least recently used page ID
    pub fn pop_lru(&mut self) -> Option<u32> {
        let page_id = self.lru()?;
        self.remove(page_id);
        Some(page_id)
    }

    /// Insert a new page at the front
    fn insert(&mut self, page_id: u32) {
        let pos = if let Some(pos) = self.free_slots.pop() {
            self.order[pos] = LruNode {
                page_id,
                prev: None,
                next: self.head,
                active: true,
            };
            pos
        } else {
            let pos = self.order.len();
            self.order.push(LruNode {
                page_id,
                prev: None,
                next: self.head,
                active: true,
            });
            pos
        };

        if let Some(old_head) = self.head {
            self.order[old_head].prev = Some(pos);
        }
        self.head = Some(pos);

        if self.tail.is_none() {
            self.tail = Some(pos);
        }

        self.positions.insert(page_id, pos);
    }

    /// Move a node to the front of the list
    fn move_to_front(&mut self, pos: usize) {
        if self.head == Some(pos) {
            return; // Already at front
        }

        self.unlink(pos);

        // Link at front
        self.order[pos].prev = None;
        self.order[pos].next = self.head;

        if let Some(old_head) = self.head {
            self.order[old_head].prev = Some(pos);
        }
        self.head = Some(pos);

        if self.tail.is_none() {
            self.tail = Some(pos);
        }
    }

    /// Unlink a node from the list
    fn unlink(&mut self, pos: usize) {
        let node = self.order[pos];

        if let Some(prev) = node.prev {
            self.order[prev].next = node.next;
        } else {
            self.head = node.next;
        }

        if let Some(next) = node.next {
            self.order[next].prev = node.prev;
        } else {
            self.tail = node.prev;
        }
    }

    /// Get the number of items in the cache (test only)
    #[cfg(test)]
    fn len(&self) -> usize {
        self.positions.len()
    }

    /// Check if the cache is empty (test only)
    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.positions.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_basic() {
        let mut cache = LruCache::new(3);

        cache.access(1);
        cache.access(2);
        cache.access(3);

        assert_eq!(cache.lru(), Some(1));

        // Access 1, making it most recent
        cache.access(1);
        assert_eq!(cache.lru(), Some(2));

        // Pop LRU
        assert_eq!(cache.pop_lru(), Some(2));
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_lru_remove() {
        let mut cache = LruCache::new(3);

        cache.access(1);
        cache.access(2);
        cache.access(3);

        cache.remove(2);
        assert_eq!(cache.len(), 2);

        // LRU should still be 1
        assert_eq!(cache.lru(), Some(1));

        // After popping 1, LRU should be 3
        cache.pop_lru();
        assert_eq!(cache.lru(), Some(3));
    }

    #[test]
    fn test_lru_empty() {
        let mut cache = LruCache::new(3);
        assert!(cache.is_empty());
        assert_eq!(cache.lru(), None);
        assert_eq!(cache.pop_lru(), None);
    }
}
