# Architecture Deep Dive

This document provides an in-depth look at the B-Tree Storage Engine's internal architecture, design patterns, and implementation details.

## Table of Contents

1. [System Overview](#system-overview)
2. [Layer Details](#layer-details)
3. [Data Flow](#data-flow)
4. [Key Algorithms](#key-algorithms)
5. [Concurrency Model](#concurrency-model)
6. [Error Handling](#error-handling)
7. [Memory Management](#memory-management)

---

## System Overview

### Design Philosophy

The storage engine follows these core principles:

1. **Modularity**: Each layer has a well-defined interface (trait)
2. **Testability**: All components can be tested in isolation
3. **Safety**: Rust's ownership model prevents data races
4. **Performance**: Zero-copy where possible, minimal allocations

### Component Diagram

```
                    ┌─────────────────────────┐
                    │      Application        │
                    │    (uses Db struct)     │
                    └───────────┬─────────────┘
                                │
                    ┌───────────▼─────────────┐
                    │          Db             │
                    │   (Public API Layer)    │
                    │  • Thread-safe handle   │
                    │  • Wraps BTree in RwLock│
                    └───────────┬─────────────┘
                                │
                    ┌───────────▼─────────────┐
                    │        BTree            │
                    │   (Tree Operations)     │
                    │  • Search, Insert       │
                    │  • Split, Merge         │
                    │  • Scan, Cursor         │
                    └───────────┬─────────────┘
                                │
                    ┌───────────▼─────────────┐
                    │     BufferPoolImpl      │
                    │   (Page Caching)        │
                    │  • LRU eviction         │
                    │  • Dirty tracking       │
                    │  • Page pinning         │
                    └───────────┬─────────────┘
                                │
                    ┌───────────▼─────────────┐
                    │    DiskManagerImpl      │
                    │   (File I/O)            │
                    │  • Read/Write pages     │
                    │  • Free list mgmt       │
                    │  • File header          │
                    └───────────┬─────────────┘
                                │
                    ┌───────────▼─────────────┐
                    │      SlottedPage        │
                    │   (Page Format)         │
                    │  • Cell management      │
                    │  • Binary search        │
                    │  • Split/merge          │
                    └─────────────────────────┘
```

---

## Layer Details

### 1. Public API Layer (`lib.rs`)

The `Db` struct provides the public interface:

```rust
pub struct Db {
    btree: Arc<RwLock<BTree>>,      // Thread-safe tree access
    buffer_pool: Arc<BufferPoolImpl>, // Shared buffer pool
    disk_manager: Arc<DiskManagerImpl>, // Shared disk manager
}
```

**Key Responsibilities:**
- Convert user calls to internal operations
- Manage concurrency with `RwLock`
- Expose statistics and configuration

**Thread Safety:**
- `RwLock<BTree>` allows multiple concurrent readers
- Single writer has exclusive access
- `Arc` enables shared ownership across threads

### 2. B-Tree Layer (`btree/`)

#### BTree Structure

```rust
pub struct BTree {
    buffer_pool: Arc<BufferPoolImpl>,
    root_page: PageId,    // Root of the tree (0 = empty)
    height: usize,        // Current tree height
}
```

#### Core Operations

**Search (`get`)**
```
1. Start at root_page
2. If leaf: binary search for key
3. If interior: find_child() to get next page
4. Recurse until found or not found
```

**Insert (`put`)**
```
1. Search for insertion point
2. If key exists: update value
3. If room in leaf: insert cell
4. If leaf full: split and propagate
```

**Split Algorithm**
```
1. Find middle cell (n/2)
2. Create new page with upper half
3. Keep lower half in original
4. Return (separator, new_page_id) to parent
5. If parent full: recursively split
6. If root splits: create new root
```

#### Interior Node Semantics

The B-tree uses a specific pointer interpretation:

```
Interior Page Structure:
┌─────────────────────────────────────────────────────┐
│ right_child │ [sep₁, child₁] │ [sep₂, child₂] │ ...│
└─────────────────────────────────────────────────────┘

Semantics:
• right_child → keys < sep₁
• child₁ → keys ≥ sep₁ and < sep₂
• child₂ → keys ≥ sep₂ and < sep₃
• childₙ → keys ≥ sepₙ (last separator)
```

This design was chosen because:
1. Natural insertion: new child goes with its separator
2. Simpler split logic: separator naturally divides ranges
3. Efficient search: binary search finds correct child

### 3. Buffer Pool Layer (`buffer/`)

#### BufferPool Trait

```rust
pub trait BufferPool: Send + Sync {
    fn fetch_page(&self, page_id: PageId) -> Result<PageGuard>;
    fn fetch_page_mut(&self, page_id: PageId) -> Result<PageGuardMut>;
    fn new_page(&self) -> Result<(PageId, PageGuardMut)>;
    fn flush_page(&self, page_id: PageId) -> Result<()>;
    fn flush_all(&self) -> Result<()>;
    // ... more methods
}
```

#### BufferPoolImpl Internals

```rust
pub struct BufferPoolImpl {
    disk_manager: Arc<DiskManagerImpl>,
    
    // Page cache: PageId -> (Page, Metadata)
    pages: RwLock<HashMap<PageId, Arc<RwLock<SlottedPage>>>>,
    
    // Dirty page tracking
    dirty_pages: RwLock<HashSet<PageId>>,
    
    // LRU eviction
    lru: Mutex<LruCache>,
    
    // Configuration
    capacity: usize,
}
```

#### Page Lifecycle

```
┌─────────┐     fetch_page()     ┌─────────┐
│  Disk   │ ──────────────────► │  Cache  │
└─────────┘                      └─────────┘
                                      │
                              modify via guard
                                      │
                                      ▼
                                ┌─────────┐
                                │  Dirty  │
                                └─────────┘
                                      │
                              flush_page()
                                      │
                                      ▼
                                ┌─────────┐
                                │  Disk   │
                                └─────────┘
```

#### LRU Cache Implementation

```rust
pub struct LruCache {
    head: Option<PageId>,      // Most recently used
    tail: Option<PageId>,      // Least recently used
    nodes: HashMap<PageId, LruNode>,
}

struct LruNode {
    prev: Option<PageId>,
    next: Option<PageId>,
}
```

**Eviction Process:**
1. Check if cache is at capacity
2. Find LRU page (tail of list)
3. If dirty, flush to disk
4. Remove from cache and LRU
5. Load new page into freed slot

### 4. Storage Layer (`storage/`)

#### DiskManager Trait

```rust
pub trait DiskManager: Send + Sync {
    fn read_page(&self, page_id: PageId) -> Result<SlottedPage>;
    fn write_page(&self, page_id: PageId, page: &SlottedPage) -> Result<()>;
    fn allocate_page(&self) -> Result<PageId>;
    fn deallocate_page(&self, page_id: PageId) -> Result<()>;
    fn page_count(&self) -> usize;
    fn sync(&self) -> Result<()>;
}
```

#### File Header Management

The file header (page 0) stores critical metadata:

```rust
pub struct FileHeader {
    pub magic: u64,           // File identification
    pub version: u32,         // Format version
    pub page_size: u32,       // Page size (4096)
    pub page_count: u32,      // Total pages
    pub free_list_head: u32,  // First free page
    pub root_page: u32,       // B-tree root
    pub tree_height: u32,     // Tree height
    pub checksum: u32,        // CRC32 validation
}
```

**Header Update Protocol:**
1. Modify header in memory
2. Compute new checksum
3. Write to page 0
4. Sync if configured

#### Free List

Deleted pages are tracked in a free list:

```rust
pub struct FreeList {
    head: PageId,  // First free page
}
```

**Allocation:**
```
If free_list_head != 0:
    1. Pop head from free list
    2. Update header
    3. Return popped page
Else:
    1. Increment page_count
    2. Update header
    3. Return new page ID
```

### 5. Page Layer (`page/`)

#### SlottedPage Structure

```rust
pub struct SlottedPage {
    data: [u8; PAGE_SIZE],  // Raw page bytes
    header: PageHeader,      // Parsed header
}
```

#### Page Layout

```
Offset    Content
──────────────────────────────────────────────
0         Page Header (8-12 bytes)
8/12      Cell Pointer Array (2 bytes each)
          ↓ grows downward
          
          [Free Space]
          
          ↑ grows upward
?         Cell Content Area
4095      Last byte of page
──────────────────────────────────────────────
```

#### Cell Insertion Algorithm

```
insert_cell(cell):
    1. Encode cell to bytes
    2. cell_size = encoded.len()
    3. Find insertion point (binary search by key)
    4. Check if page has room
    5. Allocate space: content_start -= cell_size
    6. Write cell at content_start
    7. Shift cell pointers to make room
    8. Insert new pointer at correct position
    9. Increment cell_count
    10. Update header
```

#### Cell Encoding (Varint)

Variable-length integers minimize space:

| Value Range | Bytes Used |
|-------------|------------|
| 0-127 | 1 |
| 128-16383 | 2 |
| 16384-2097151 | 3 |
| ... | ... |

```rust
fn encode_varint(value: u64) -> Vec<u8> {
    let mut result = Vec::new();
    let mut v = value;
    while v >= 0x80 {
        result.push((v as u8) | 0x80);
        v >>= 7;
    }
    result.push(v as u8);
    result
}
```

---

## Data Flow

### Insert Operation Flow

```
db.put(key, value)
        │
        ▼
┌───────────────────┐
│ Acquire write lock│
│ on BTree          │
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐
│ btree.put(k, v)   │
│ • validate sizes  │
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐
│ insert_recursive  │
│ • fetch root page │
│ • traverse tree   │
└─────────┬─────────┘
          │
    ┌─────┴─────┐
    │           │
    ▼           ▼
┌───────┐   ┌───────┐
│ Leaf  │   │Interior│
│ page  │   │ page   │
└───┬───┘   └───┬───┘
    │           │
    ▼           ▼
┌───────────────────┐
│ BufferPool        │
│ • check cache     │
│ • load if needed  │
│ • return guard    │
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐
│ SlottedPage       │
│ • insert_cell     │
│ • or split        │
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐
│ Mark dirty        │
│ (write-back cache)│
└───────────────────┘
```

### Read Operation Flow

```
db.get(key)
        │
        ▼
┌───────────────────┐
│ Acquire read lock │
│ on BTree          │
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐
│ btree.get(key)    │
│ • start at root   │
└─────────┬─────────┘
          │
          ▼
    ┌─────────────┐
    │   search    │◄────────────────┐
    │  page_id    │                 │
    └──────┬──────┘                 │
           │                        │
           ▼                        │
    ┌─────────────┐                 │
    │ BufferPool  │                 │
    │ fetch_page  │                 │
    └──────┬──────┘                 │
           │                        │
           ▼                        │
    ┌─────────────┐                 │
    │ SlottedPage │                 │
    │  is_leaf?   │                 │
    └──────┬──────┘                 │
           │                        │
     ┌─────┴─────┐                  │
     │           │                  │
     ▼           ▼                  │
 ┌───────┐   ┌────────┐            │
 │ Leaf  │   │Interior│            │
 │search │   │find_   │────────────┘
 │ key   │   │child   │
 └───┬───┘   └────────┘
     │
     ▼
┌─────────────┐
│ Return      │
│ Some(value) │
│ or None     │
└─────────────┘
```

---

## Key Algorithms

### Binary Search in Slotted Page

```rust
pub fn search(&self, key: &[u8]) -> Result<Option<usize>> {
    let count = self.cell_count();
    if count == 0 {
        return Ok(None);
    }

    let mut low = 0;
    let mut high = count;

    while low < high {
        let mid = low + (high - low) / 2;
        let cell = self.get_cell(mid)?;

        match key.cmp(&cell.key) {
            Ordering::Less => high = mid,
            Ordering::Greater => low = mid + 1,
            Ordering::Equal => return Ok(Some(mid)),
        }
    }

    Ok(None)
}
```

**Complexity:** O(log n) where n = cells per page

### Page Split

```rust
pub fn split(&mut self) -> Result<(SlottedPage, Vec<u8>)> {
    let mid = self.cell_count() / 2;
    
    // Create new page of same type
    let mut new_page = if self.is_leaf() {
        SlottedPage::new_leaf()
    } else {
        SlottedPage::new_interior()
    };

    // Move upper half to new page
    let cells_to_move = (mid..self.cell_count())
        .map(|i| self.get_cell(i))
        .collect::<Result<Vec<_>>>()?;

    let separator = cells_to_move[0].key.clone();

    if self.is_interior() {
        // For interior: first cell's child becomes right_child
        new_page.set_right_child(cells_to_move[0].left_child);
        for cell in cells_to_move.iter().skip(1) {
            new_page.insert_cell(cell)?;
        }
    } else {
        // For leaf: copy all cells
        for cell in &cells_to_move {
            new_page.insert_cell(cell)?;
        }
    }

    // Remove from original
    for i in (mid..self.cell_count()).rev() {
        self.delete_cell(i)?;
    }

    Ok((new_page, separator))
}
```

### LRU Eviction

```rust
impl LruCache {
    pub fn evict(&mut self) -> Option<PageId> {
        // Remove from tail (least recently used)
        let victim = self.tail?;
        self.remove(victim);
        Some(victim)
    }

    pub fn access(&mut self, page_id: PageId) {
        if self.nodes.contains_key(&page_id) {
            // Move to head (most recently used)
            self.remove(page_id);
        }
        self.push_front(page_id);
    }
}
```

---

## Concurrency Model

### Lock Hierarchy

```
Level 1: Db.btree (RwLock)
    │
    ▼
Level 2: BufferPool.pages (RwLock)
    │
    ▼
Level 3: Individual Page (RwLock via PageGuard)
```

### Lock Acquisition Rules

1. **Never hold multiple page locks simultaneously** during tree traversal
2. **Release parent lock before acquiring child lock** (hand-over-hand not needed due to top-level lock)
3. **Dirty set lock is acquired briefly** during mark/flush

### PageGuard Pattern

```rust
pub struct PageGuard<'a> {
    page: RwLockReadGuard<'a, SlottedPage>,
}

pub struct PageGuardMut<'a> {
    page: RwLockWriteGuard<'a, SlottedPage>,
    buffer_pool: &'a BufferPoolImpl,
    page_id: PageId,
}

// Automatically marks dirty on drop if modified
impl<'a> Drop for PageGuardMut<'a> {
    fn drop(&mut self) {
        self.buffer_pool.mark_dirty(self.page_id);
    }
}
```

---

## Error Handling

### Error Propagation

All fallible operations return `Result<T, StorageError>`:

```rust
pub type Result<T> = std::result::Result<T, StorageError>;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Page {page_id} not found")]
    PageNotFound { page_id: u32 },

    #[error("Key too large: {size} bytes (max: {max})")]
    KeyTooLarge { size: usize, max: usize },

    #[error("Corruption detected: {0}")]
    Corruption(String),
    
    // ... other variants
}
```

### Recovery Strategy

On error detection:
1. **I/O errors**: Propagate to caller, don't corrupt state
2. **Checksum mismatch**: Return `Corruption` error
3. **Page not found**: Return specific error for debugging

---

## Memory Management

### Allocation Points

| Component | Allocation Type | Lifetime |
|-----------|-----------------|----------|
| Page data | `[u8; 4096]` | Per-page |
| Cell vectors | `Vec<Cell>` | Temporary |
| Key/Value | `Vec<u8>` | Per-cell |
| LRU nodes | `HashMap` entry | Per-cached-page |

### Memory Budget

With default settings:
- Buffer pool: 1000 pages × 4KB = **4 MB**
- LRU metadata: ~24 bytes/page = **24 KB**
- Page metadata: ~100 bytes/page = **100 KB**
- **Total**: ~4.2 MB baseline

### Zero-Copy Operations

Where possible, we avoid copying:
- Page reads return guards (no copy)
- Cell access returns references when possible
- Scan collects results but minimizes intermediate copies

---

## Future Architecture

### Planned Changes for v2

1. **WAL Integration**
   ```
   BufferPool ──► WAL ──► DiskManager
   ```

2. **MVCC Layer**
   ```
   Transaction Manager
         │
         ▼
   Version Chain per Key
   ```

3. **Compression**
   ```
   SlottedPage ──► LZ4 ──► Disk
   ```

### Extension Points

The trait-based design allows swapping implementations:

- `DiskManager`: Could use mmap, async I/O
- `BufferPool`: Could use clock, 2Q algorithm
- Page format: Could add compression, encryption
