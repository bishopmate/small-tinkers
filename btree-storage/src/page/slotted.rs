//! Slotted page implementation.
//!
//! A slotted page uses the following layout:
//! ```text
//! ┌────────────────────────────────────────────────────┐
//! │                    Page Header                      │
//! ├────────────────────────────────────────────────────┤
//! │                 Cell Pointer Array                  │
//! │  [ptr0][ptr1][ptr2]...      →                      │
//! ├────────────────────────────────────────────────────┤
//! │                                                    │
//! │                   Free Space                        │
//! │                                                    │
//! ├────────────────────────────────────────────────────┤
//! │                 Cell Content Area                   │
//! │      ←  [cell2][cell1][cell0]                      │
//! └────────────────────────────────────────────────────┘
//! ```
//!
//! Cell pointers are sorted by key order for binary search.
//! Cell content grows from the end of the page toward the header.

use crate::error::{Result, StorageError};
use crate::page::{Cell, PageBuf, PageHeader};
use crate::types::{PageId, PageType};

/// A slotted page providing cell-based storage
pub struct SlottedPage {
    /// The raw page data
    data: PageBuf,
    /// Cached header (kept in sync with data)
    header: PageHeader,
}

impl SlottedPage {
    /// Create a new empty leaf page
    pub fn new_leaf() -> Self {
        let mut data = PageBuf::new();
        let header = PageHeader::new_leaf();
        header.write(&mut data);
        Self { data, header }
    }

    /// Create a new empty interior page
    pub fn new_interior() -> Self {
        let mut data = PageBuf::new();
        let header = PageHeader::new_interior();
        header.write(&mut data);
        Self { data, header }
    }

    /// Load a page from raw bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let data = PageBuf::from_bytes(bytes);
        let header = PageHeader::read(&data)
            .ok_or_else(|| StorageError::invalid_page("invalid page header"))?;
        Ok(Self { data, header })
    }

    /// Get the raw bytes of this page
    pub fn as_bytes(&self) -> &[u8] {
        self.data.as_bytes()
    }

    /// Get the page header
    pub fn header(&self) -> &PageHeader {
        &self.header
    }

    /// Get the number of cells in this page
    pub fn cell_count(&self) -> usize {
        self.header.cell_count as usize
    }

    /// Check if this is a leaf page
    pub fn is_leaf(&self) -> bool {
        self.header.page_type.is_leaf()
    }

    /// Check if this is an interior page
    pub fn is_interior(&self) -> bool {
        self.header.page_type.is_interior()
    }

    /// Get the page type
    pub fn page_type(&self) -> PageType {
        self.header.page_type
    }

    /// Get the right-most child pointer (for interior pages)
    pub fn right_child(&self) -> PageId {
        PageId::new(self.header.right_child)
    }

    /// Set the right-most child pointer (for interior pages)
    pub fn set_right_child(&mut self, page_id: PageId) {
        self.header.right_child = page_id.value();
        self.sync_header();
    }

    /// Get the cell pointer at the given index
    fn cell_pointer(&self, index: usize) -> u16 {
        let offset = self.header.cell_pointer_offset() + index * 2;
        u16::from_be_bytes([self.data[offset], self.data[offset + 1]])
    }

    /// Set the cell pointer at the given index
    fn set_cell_pointer(&mut self, index: usize, pointer: u16) {
        let offset = self.header.cell_pointer_offset() + index * 2;
        self.data[offset..offset + 2].copy_from_slice(&pointer.to_be_bytes());
    }

    /// Get the cell at the given index
    pub fn get_cell(&self, index: usize) -> Result<Cell> {
        if index >= self.cell_count() {
            return Err(StorageError::invalid_operation(format!(
                "cell index {} out of bounds (count: {})",
                index,
                self.cell_count()
            )));
        }

        let pointer = self.cell_pointer(index) as usize;
        let cell_bytes = &self.data[pointer..];

        let cell = if self.is_leaf() {
            Cell::decode_leaf(cell_bytes)
                .ok_or_else(|| StorageError::corruption("failed to decode leaf cell"))?
                .0
        } else {
            Cell::decode_interior(cell_bytes)
                .ok_or_else(|| StorageError::corruption("failed to decode interior cell"))?
                .0
        };

        Ok(cell)
    }

    /// Get all cells in this page (in sorted key order)
    pub fn get_all_cells(&self) -> Result<Vec<Cell>> {
        let mut cells = Vec::with_capacity(self.cell_count());
        for i in 0..self.cell_count() {
            cells.push(self.get_cell(i)?);
        }
        Ok(cells)
    }

    /// Calculate free space available for new cells
    pub fn free_space(&self) -> usize {
        let ptr_array_end = self.header.cell_pointer_array_end();
        let content_start = self.header.cell_content_start as usize;

        // Available space minus the 2 bytes needed for a new cell pointer
        content_start.saturating_sub(ptr_array_end).saturating_sub(2)
    }

    /// Check if a cell of the given size can fit
    pub fn can_fit(&self, cell_size: usize) -> bool {
        self.free_space() >= cell_size
    }

    /// Insert a cell at the correct sorted position
    ///
    /// Returns the index where the cell was inserted.
    pub fn insert_cell(&mut self, cell: &Cell) -> Result<usize> {
        let encoded = cell.encode();
        let cell_size = encoded.len();

        if !self.can_fit(cell_size) {
            return Err(StorageError::PageFull {
                page_id: PageId::INVALID,
                needed: cell_size + 2,
                available: self.free_space(),
            });
        }

        // Find insertion position using binary search
        let insert_pos = self.find_insert_position(&cell.key)?;

        // Allocate space for the cell content
        let new_content_start = self.header.cell_content_start as usize - cell_size;
        self.data[new_content_start..new_content_start + cell_size].copy_from_slice(&encoded);

        // Shift cell pointers to make room
        let cell_count = self.cell_count();
        for i in (insert_pos..cell_count).rev() {
            let ptr = self.cell_pointer(i);
            self.set_cell_pointer(i + 1, ptr);
        }

        // Insert the new cell pointer
        self.set_cell_pointer(insert_pos, new_content_start as u16);

        // Update header
        self.header.cell_count += 1;
        self.header.cell_content_start = new_content_start as u16;
        self.sync_header();

        Ok(insert_pos)
    }

    /// Find the position where a key should be inserted
    fn find_insert_position(&self, key: &[u8]) -> Result<usize> {
        let cell_count = self.cell_count();
        if cell_count == 0 {
            return Ok(0);
        }

        // Binary search for the insertion point
        let mut low = 0;
        let mut high = cell_count;

        while low < high {
            let mid = low + (high - low) / 2;
            let cell = self.get_cell(mid)?;

            match key.cmp(&cell.key) {
                std::cmp::Ordering::Less => high = mid,
                std::cmp::Ordering::Greater => low = mid + 1,
                std::cmp::Ordering::Equal => return Ok(mid), // Key exists
            }
        }

        Ok(low)
    }

    /// Search for a key and return its index, or None if not found
    pub fn search(&self, key: &[u8]) -> Result<Option<usize>> {
        let cell_count = self.cell_count();
        if cell_count == 0 {
            return Ok(None);
        }

        let mut low = 0;
        let mut high = cell_count;

        while low < high {
            let mid = low + (high - low) / 2;
            let cell = self.get_cell(mid)?;

            match key.cmp(&cell.key) {
                std::cmp::Ordering::Less => high = mid,
                std::cmp::Ordering::Greater => low = mid + 1,
                std::cmp::Ordering::Equal => return Ok(Some(mid)),
            }
        }

        Ok(None)
    }

    /// Find the child page for a given key (for interior pages)
    ///
    /// Returns the page ID of the child that should contain the key.
    /// 
    /// Interior node structure:
    /// - Cells are stored in key order, each with (key, child_ptr)
    /// - child_ptr points to keys >= cell.key
    /// - right_child points to keys < first cell's key
    ///
    /// Example with keys [10, 20, 30]:
    /// - right_child → keys < 10
    /// - cell[0].left_child → keys >= 10 and < 20
    /// - cell[1].left_child → keys >= 20 and < 30
    /// - cell[2].left_child → keys >= 30
    pub fn find_child(&self, key: &[u8]) -> Result<PageId> {
        if !self.is_interior() {
            return Err(StorageError::invalid_operation(
                "find_child called on leaf page",
            ));
        }

        let cell_count = self.cell_count();
        if cell_count == 0 {
            // No separators, use right_child (which acts as the only child)
            return Ok(self.right_child());
        }

        // Check if key is less than the first separator
        let first_cell = self.get_cell(0)?;
        if key < first_cell.key.as_slice() {
            // Keys < first separator go to right_child
            return Ok(self.right_child());
        }

        // Binary search for the largest separator <= key
        let mut low = 0;
        let mut high = cell_count;

        while low < high {
            let mid = low + (high - low) / 2;
            let cell = self.get_cell(mid)?;

            if key < cell.key.as_slice() {
                high = mid;
            } else {
                low = mid + 1;
            }
        }

        // low is now the index AFTER the last separator <= key
        // So the separator at low-1 is the largest one <= key
        // Return its child pointer
        Ok(self.get_cell(low - 1)?.left_child)
    }

    /// Update the value of an existing cell at the given index
    ///
    /// This is only valid for leaf pages.
    pub fn update_cell(&mut self, index: usize, new_value: &[u8]) -> Result<()> {
        if !self.is_leaf() {
            return Err(StorageError::invalid_operation(
                "update_cell called on interior page",
            ));
        }

        let cell = self.get_cell(index)?;
        let new_cell = Cell::new_leaf(cell.key.clone(), new_value.to_vec());

        // For simplicity, we delete and re-insert
        // A more efficient implementation would update in-place if the new cell fits
        self.delete_cell(index)?;

        // Re-insert at the correct position (should be same position)
        self.insert_cell(&new_cell)?;

        Ok(())
    }

    /// Delete the cell at the given index
    pub fn delete_cell(&mut self, index: usize) -> Result<Cell> {
        if index >= self.cell_count() {
            return Err(StorageError::invalid_operation(format!(
                "delete index {} out of bounds",
                index
            )));
        }

        let cell = self.get_cell(index)?;

        // Shift cell pointers down
        let cell_count = self.cell_count();
        for i in index..cell_count - 1 {
            let ptr = self.cell_pointer(i + 1);
            self.set_cell_pointer(i, ptr);
        }

        // Update header
        self.header.cell_count -= 1;
        // Note: We don't reclaim the cell content space immediately
        // A defragment operation would be needed to compact the page
        self.header.fragmented_bytes += cell.encoded_size() as u8;
        self.sync_header();

        Ok(cell)
    }

    /// Split this page, returning a new page with the upper half of keys
    ///
    /// Returns (new_page, separator_key) where separator_key is the first key
    /// of the new page (for insertion into parent).
    pub fn split(&mut self) -> Result<(SlottedPage, Vec<u8>)> {
        let cell_count = self.cell_count();
        let mid = cell_count / 2;

        // Create new page of same type
        let mut new_page = if self.is_leaf() {
            SlottedPage::new_leaf()
        } else {
            SlottedPage::new_interior()
        };

        // Move upper half of cells to new page
        let cells_to_move: Vec<Cell> = (mid..cell_count)
            .map(|i| self.get_cell(i))
            .collect::<Result<_>>()?;

        // Get separator key before moving cells
        let separator_key = cells_to_move[0].key.clone();

        // For interior pages, the separator key goes to parent and is not in either child
        // For leaf pages, the separator key stays in the new (right) page

        if self.is_interior() {
            // Interior page split with new semantics:
            // - right_child = keys < first separator
            // - cell.left_child = keys >= cell.key
            //
            // When splitting [k1, k2, k3, k4] at mid=2:
            // - Left page keeps [k1, k2], right_child unchanged
            // - Separator = k3 goes to parent
            // - Right page gets [k4, ...], with right_child = k3.left_child
            //
            // k3.left_child contains keys >= k3, which is now the "beginning" of right page
            // So right page's right_child should be k3.left_child (for keys < k4)
            
            let first_cell = &cells_to_move[0];
            
            // Right page's right_child = separator's child (keys >= k3 and < next key)
            new_page.set_right_child(first_cell.left_child);

            // Left page's right_child stays the same (keys < k1)
            // (no change needed)

            // Insert remaining cells (after separator) into new page
            for cell in cells_to_move.iter().skip(1) {
                new_page.insert_cell(cell)?;
            }
        } else {
            // For leaf pages, copy all cells to new page
            for cell in &cells_to_move {
                new_page.insert_cell(cell)?;
            }
        }

        // Remove moved cells from this page (in reverse order)
        for i in (mid..cell_count).rev() {
            self.delete_cell(i)?;
        }

        // Defragment this page to reclaim space
        self.defragment()?;

        Ok((new_page, separator_key))
    }

    /// Defragment the page to reclaim fragmented space
    pub fn defragment(&mut self) -> Result<()> {
        let cells = self.get_all_cells()?;

        // Reset page
        let mut new_page = if self.is_leaf() {
            SlottedPage::new_leaf()
        } else {
            let mut p = SlottedPage::new_interior();
            p.set_right_child(self.right_child());
            p
        };

        // Re-insert all cells
        for cell in cells {
            new_page.insert_cell(&cell)?;
        }

        // Copy new page data to self
        self.data = new_page.data;
        self.header = new_page.header;

        Ok(())
    }

    /// Sync the header to the raw page data
    fn sync_header(&mut self) {
        self.header.write(&mut self.data);
    }
}

impl Clone for SlottedPage {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            header: self.header,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_leaf_page() {
        let page = SlottedPage::new_leaf();
        assert!(page.is_leaf());
        assert!(!page.is_interior());
        assert_eq!(page.cell_count(), 0);
        assert!(page.free_space() > 0);
    }

    #[test]
    fn test_insert_and_get() {
        let mut page = SlottedPage::new_leaf();

        let cell1 = Cell::new_leaf(b"banana".to_vec(), b"yellow".to_vec());
        let cell2 = Cell::new_leaf(b"apple".to_vec(), b"red".to_vec());
        let cell3 = Cell::new_leaf(b"cherry".to_vec(), b"red".to_vec());

        page.insert_cell(&cell1).unwrap();
        page.insert_cell(&cell2).unwrap();
        page.insert_cell(&cell3).unwrap();

        assert_eq!(page.cell_count(), 3);

        // Cells should be in sorted order
        let c0 = page.get_cell(0).unwrap();
        let c1 = page.get_cell(1).unwrap();
        let c2 = page.get_cell(2).unwrap();

        assert_eq!(c0.key, b"apple");
        assert_eq!(c1.key, b"banana");
        assert_eq!(c2.key, b"cherry");
    }

    #[test]
    fn test_search() {
        let mut page = SlottedPage::new_leaf();

        page.insert_cell(&Cell::new_leaf(b"a".to_vec(), b"1".to_vec()))
            .unwrap();
        page.insert_cell(&Cell::new_leaf(b"c".to_vec(), b"3".to_vec()))
            .unwrap();
        page.insert_cell(&Cell::new_leaf(b"b".to_vec(), b"2".to_vec()))
            .unwrap();

        assert_eq!(page.search(b"a").unwrap(), Some(0));
        assert_eq!(page.search(b"b").unwrap(), Some(1));
        assert_eq!(page.search(b"c").unwrap(), Some(2));
        assert_eq!(page.search(b"d").unwrap(), None);
    }

    #[test]
    fn test_delete() {
        let mut page = SlottedPage::new_leaf();

        page.insert_cell(&Cell::new_leaf(b"a".to_vec(), b"1".to_vec()))
            .unwrap();
        page.insert_cell(&Cell::new_leaf(b"b".to_vec(), b"2".to_vec()))
            .unwrap();
        page.insert_cell(&Cell::new_leaf(b"c".to_vec(), b"3".to_vec()))
            .unwrap();

        let deleted = page.delete_cell(1).unwrap();
        assert_eq!(deleted.key, b"b");
        assert_eq!(page.cell_count(), 2);

        // Remaining cells
        assert_eq!(page.get_cell(0).unwrap().key, b"a".to_vec());
        assert_eq!(page.get_cell(1).unwrap().key, b"c".to_vec());
    }

    #[test]
    fn test_split() {
        let mut page = SlottedPage::new_leaf();

        // Insert several cells
        for i in 0..10 {
            let key = format!("key{:02}", i);
            let value = format!("value{}", i);
            page.insert_cell(&Cell::new_leaf(key.into_bytes(), value.into_bytes()))
                .unwrap();
        }

        assert_eq!(page.cell_count(), 10);

        let (new_page, separator) = page.split().unwrap();

        // Check that cells are distributed
        assert!(page.cell_count() > 0);
        assert!(new_page.cell_count() > 0);
        assert_eq!(page.cell_count() + new_page.cell_count(), 10);

        // Separator should be the first key of new page
        assert_eq!(separator, new_page.get_cell(0).unwrap().key);

        // All keys in old page should be less than separator
        for i in 0..page.cell_count() {
            assert!(page.get_cell(i).unwrap().key < separator);
        }
    }

    #[test]
    fn test_interior_page() {
        let mut page = SlottedPage::new_interior();
        page.set_right_child(PageId::new(100));

        // Insert separator keys with child pointers
        // With new semantics:
        // - right_child (100) = keys < first separator
        // - cell.left_child = keys >= cell.key
        page.insert_cell(&Cell::new_interior(b"m".to_vec(), PageId::new(10)))
            .unwrap();
        page.insert_cell(&Cell::new_interior(b"t".to_vec(), PageId::new(20)))
            .unwrap();

        // Keys < "m" should go to right_child (100)
        assert_eq!(page.find_child(b"a").unwrap(), PageId::new(100));
        // Keys >= "m" and < "t" should go to child 10 (cell "m"'s child)
        assert_eq!(page.find_child(b"m").unwrap(), PageId::new(10));
        assert_eq!(page.find_child(b"n").unwrap(), PageId::new(10));
        // Keys >= "t" should go to child 20 (cell "t"'s child)
        assert_eq!(page.find_child(b"t").unwrap(), PageId::new(20));
        assert_eq!(page.find_child(b"z").unwrap(), PageId::new(20));
    }

    #[test]
    fn test_from_bytes_roundtrip() {
        let mut page = SlottedPage::new_leaf();
        page.insert_cell(&Cell::new_leaf(b"test".to_vec(), b"data".to_vec()))
            .unwrap();

        let bytes = page.as_bytes();
        let restored = SlottedPage::from_bytes(bytes).unwrap();

        assert_eq!(restored.cell_count(), 1);
        let cell = restored.get_cell(0).unwrap();
        assert_eq!(cell.key, b"test");
        assert_eq!(cell.value, b"data");
    }
}
