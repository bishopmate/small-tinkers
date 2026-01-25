# B-Tree Storage Engine

A high-performance, disk-based B-tree storage engine written in Rust. Designed as the storage layer for relational databases, providing efficient key-value storage with support for point lookups, range scans, and ordered iteration.

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Quick Start](#quick-start)
- [API Reference](#api-reference)
- [CLI Tool](#cli-tool)
- [Configuration](#configuration)
- [File Format Specification](#file-format-specification)
- [Design Decisions](#design-decisions)
- [Performance](#performance)
- [Limitations](#limitations)
- [Contributing](#contributing)
- [License](#license)

---

## Features

- **Disk-Based Storage**: Persistent B-tree with efficient page-based I/O
- **High Performance**: ~450,000+ operations/second for sequential inserts
- **Buffer Pool**: LRU-based page cache with configurable size
- **Slotted Page Format**: Cell-based layout optimized for variable-length keys and values
- **Range Queries**: Efficient range scans with start/end bounds
- **Concurrent Access**: Single-writer/multi-reader model using `RwLock`
- **Crash Recovery Ready**: Page checksums and atomic header updates (WAL planned for v2)
- **Zero Dependencies Runtime**: Only uses standard library + lightweight crates

---

## Architecture

The storage engine is composed of four modular layers:

```
┌─────────────────────────────────────────────────────────────┐
│                       Public API (Db)                        │
│                  get, put, delete, range, scan               │
├─────────────────────────────────────────────────────────────┤
│                     B-Tree Layer (btree)                     │
│         Tree traversal, splits, merges, cursors              │
├─────────────────────────────────────────────────────────────┤
│                   Buffer Pool (buffer)                       │
│            LRU cache, dirty page tracking, eviction          │
├─────────────────────────────────────────────────────────────┤
│                   Storage Layer (storage)                    │
│        Disk I/O, page allocation, free list, file header     │
├─────────────────────────────────────────────────────────────┤
│                     Page Layer (page)                        │
│      Slotted pages, cell encoding, page headers              │
└─────────────────────────────────────────────────────────────┘
```

### Module Overview

| Module | Description |
|--------|-------------|
| `lib.rs` | Public API (`Db`, `Config`, `DbStats`) |
| `btree/` | B-tree operations and cursor iteration |
| `buffer/` | Buffer pool with LRU eviction |
| `storage/` | Disk manager, file header, free list |
| `page/` | Slotted page format, cells, headers |
| `types/` | Core types (`PageId`, varint encoding) |
| `error/` | Error types and result aliases |

---

## Getting Started

### Prerequisites

- **Rust**: Version 1.70 or later
- **Cargo**: Comes with Rust installation

```bash
# Install Rust (if not already installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

### Installation

#### As a Library Dependency

Add to your `Cargo.toml`:

```toml
[dependencies]
btree-storage = { path = "path/to/btree-storage" }
```

Or if published to crates.io:

```toml
[dependencies]
btree-storage = "0.1"
```

#### Building from Source

```bash
# Clone or navigate to the project
cd btree-storage

# Build in release mode (recommended for performance)
cargo build --release

# Run tests
cargo test --release

# Build documentation
cargo doc --open
```

### Quick Start

#### Using as a Library

```rust
use btree_storage::{Db, Config, Result};

fn main() -> Result<()> {
    // Open or create a database
    let config = Config::new("my_database.db")
        .buffer_pool_size(1000)  // Cache up to 1000 pages
        .sync_on_write(false);   // Async writes for performance
    
    let db = Db::open(config)?;

    // Insert key-value pairs
    db.put(b"user:1001", b"Alice")?;
    db.put(b"user:1002", b"Bob")?;
    db.put(b"user:1003", b"Charlie")?;

    // Point lookup
    if let Some(value) = db.get(b"user:1001")? {
        println!("Found: {}", String::from_utf8_lossy(&value));
    }

    // Range scan
    let users = db.range(Some(b"user:1001"), Some(b"user:1003"))?;
    for (key, value) in users {
        println!("{} -> {}", 
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&value));
    }

    // Delete
    db.delete(b"user:1002")?;

    // Ensure data is persisted
    db.flush()?;

    Ok(())
}
```

#### Using the CLI Tool

```bash
# Build the CLI
cargo build --release

# Create a database and insert data
./target/release/btree_cli mydb.db put greeting "Hello, World!"

# Retrieve data
./target/release/btree_cli mydb.db get greeting
# Output: Hello, World!

# Bulk insert for testing
./target/release/btree_cli mydb.db bulk_insert 10000

# View statistics
./target/release/btree_cli mydb.db stats
# Output:
# page_count: 129
# buffer_pool_size: 1000
# tree_height: 2

# Scan all keys
./target/release/btree_cli mydb.db scan

# Range scan
./target/release/btree_cli mydb.db scan key_00001000 key_00002000
```

---

## API Reference

### `Db` - Main Database Handle

```rust
impl Db {
    /// Open or create a database at the configured path
    pub fn open(config: Config) -> Result<Self>;
    
    /// Get a value by key. Returns None if not found.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    
    /// Insert or update a key-value pair
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;
    
    /// Delete a key. Returns true if the key existed.
    pub fn delete(&self, key: &[u8]) -> Result<bool>;
    
    /// Check if a key exists
    pub fn contains(&self, key: &[u8]) -> Result<bool>;
    
    /// Iterate over all key-value pairs in sorted order
    pub fn iter(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
    
    /// Range scan with optional bounds [start, end)
    pub fn range(
        &self, 
        start: Option<&[u8]>, 
        end: Option<&[u8]>
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
    
    /// Flush all dirty pages to disk
    pub fn flush(&self) -> Result<()>;
    
    /// Get database statistics
    pub fn stats(&self) -> DbStats;
}
```

### `Config` - Database Configuration

```rust
impl Config {
    /// Create configuration with default settings
    pub fn new<P: Into<PathBuf>>(path: P) -> Self;
    
    /// Set buffer pool size (number of pages to cache)
    /// Default: 1000 pages (~4MB with 4KB pages)
    pub fn buffer_pool_size(self, size: usize) -> Self;
    
    /// Enable synchronous writes for durability
    /// Default: false (async for performance)
    pub fn sync_on_write(self, enabled: bool) -> Self;
}
```

### `DbStats` - Database Statistics

```rust
pub struct DbStats {
    /// Total number of pages in the database file
    pub page_count: usize,
    
    /// Buffer pool capacity (pages)
    pub buffer_pool_size: usize,
    
    /// Current height of the B-tree
    pub tree_height: usize,
}
```

### Error Handling

All operations return `Result<T, StorageError>`. Common error variants:

```rust
pub enum StorageError {
    /// I/O error from the filesystem
    Io(std::io::Error),
    
    /// Page not found in buffer pool or on disk
    PageNotFound { page_id: u32 },
    
    /// Key exceeds maximum allowed size (1KB)
    KeyTooLarge { size: usize, max: usize },
    
    /// Value exceeds maximum allowed size (1MB)
    ValueTooLarge { size: usize, max: usize },
    
    /// Page is full, cannot insert more cells
    PageFull,
    
    /// Data corruption detected (checksum mismatch)
    Corruption(String),
    
    // ... other variants
}
```

---

## CLI Tool

The `btree_cli` binary provides a command-line interface for testing and debugging.

### Commands

| Command | Usage | Description |
|---------|-------|-------------|
| `put` | `btree_cli <db> put <key> <value>` | Insert or update a key-value pair |
| `get` | `btree_cli <db> get <key>` | Retrieve a value by key |
| `delete` | `btree_cli <db> delete <key>` | Delete a key |
| `scan` | `btree_cli <db> scan [start] [end]` | Scan keys in range |
| `stats` | `btree_cli <db> stats` | Show database statistics |
| `bulk_insert` | `btree_cli <db> bulk_insert <count>` | Insert test records |
| `debug` | `btree_cli <db> debug <key>` | Trace search path for a key |

### Examples

```bash
# Basic operations
btree_cli test.db put mykey "my value"
btree_cli test.db get mykey

# Bulk operations for benchmarking
btree_cli bench.db bulk_insert 100000
btree_cli bench.db stats

# Debug tree traversal
btree_cli bench.db debug key_00050000
```

---

## Configuration

### Compile-Time Constants

Located in `src/types/mod.rs`:

| Constant | Default | Description |
|----------|---------|-------------|
| `PAGE_SIZE` | 4096 | Page size in bytes |
| `MAX_KEY_SIZE` | 1024 | Maximum key size (1KB) |
| `MAX_VALUE_SIZE` | 1048576 | Maximum value size (1MB) |

### Runtime Configuration

```rust
let config = Config::new("database.db")
    .buffer_pool_size(10000)  // 10,000 pages = ~40MB cache
    .sync_on_write(true);     // Sync after each write
```

### Recommended Settings

| Use Case | Buffer Pool Size | Sync on Write |
|----------|------------------|---------------|
| Development | 100-1000 | false |
| Production (Performance) | 10000+ | false |
| Production (Durability) | 10000+ | true |
| Memory-Constrained | 50-100 | false |

---

## File Format Specification

### Database File Structure

```
┌──────────────────────────────────────────┐
│              File Header (4KB)           │  Page 0
│  Magic, version, page count, root, etc.  │
├──────────────────────────────────────────┤
│              B-Tree Pages                │  Pages 1..N
│         (Leaf and Interior nodes)        │
└──────────────────────────────────────────┘
```

### File Header (128 bytes)

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 8 | Magic | `0x42545245_53544F52` ("BTRESTOR") |
| 8 | 4 | Version | Format version (currently 1) |
| 12 | 4 | Page Size | Page size in bytes (4096) |
| 16 | 4 | Page Count | Total pages in file |
| 20 | 4 | Free List Head | First free page (0 if none) |
| 24 | 4 | Root Page | Root B-tree page ID |
| 28 | 4 | Tree Height | Current tree height |
| 32 | 92 | Reserved | Future use |
| 124 | 4 | Checksum | CRC32 of header |

### Page Header

**Leaf Page (8 bytes):**

| Offset | Size | Field |
|--------|------|-------|
| 0 | 1 | Page Type (0x0D = leaf) |
| 1 | 2 | First Freeblock Offset |
| 3 | 2 | Cell Count |
| 5 | 2 | Cell Content Start |
| 7 | 1 | Fragmented Bytes |

**Interior Page (12 bytes):**

| Offset | Size | Field |
|--------|------|-------|
| 0-7 | 8 | Same as leaf |
| 8 | 4 | Right Child Page ID |

### Slotted Page Layout

```
┌────────────────────────────────────────────────────────┐
│ Header │ Cell Pointers │ ... Free Space ... │ Cells    │
│ (8-12) │ (2 bytes each)│                    │ (var)    │
└────────────────────────────────────────────────────────┘
         ↑               ↑                    ↑
         Header End      Pointers grow →      ← Content grows
```

### Cell Format

**Leaf Cell:**
```
┌──────────────┬──────────────┬─────────┬───────────┐
│ Key Size     │ Value Size   │ Key     │ Value     │
│ (varint)     │ (varint)     │ (bytes) │ (bytes)   │
└──────────────┴──────────────┴─────────┴───────────┘
```

**Interior Cell:**
```
┌──────────────┬──────────────┬─────────┐
│ Key Size     │ Child PageId │ Key     │
│ (varint)     │ (4 bytes)    │ (bytes) │
└──────────────┴──────────────┴─────────┘
```

### Interior Node Semantics

The B-tree uses the following pointer semantics for interior nodes:

- `right_child`: Points to child containing keys **< first separator**
- `cell[i].left_child`: Points to child containing keys **≥ cell[i].key**

Example with separators [10, 20, 30]:
```
right_child → keys < 10
cell[0].child → keys ≥ 10 and < 20
cell[1].child → keys ≥ 20 and < 30
cell[2].child → keys ≥ 30
```

---

## Design Decisions

### Why Slotted Pages?

Slotted pages allow variable-length records while maintaining sorted order:

1. **Efficient Insertion**: Only cell pointers need to shift, not actual data
2. **Variable Length**: Keys and values can be any size up to limits
3. **Space Reclamation**: Deleted space can be compacted via defragmentation
4. **Binary Search**: Cells are logically sorted via pointer array

### Why LRU Buffer Pool?

The buffer pool provides:

1. **Reduced I/O**: Hot pages stay in memory
2. **Write Coalescing**: Multiple updates to same page before flush
3. **Predictable Memory**: Fixed-size cache prevents unbounded growth
4. **Simple Eviction**: LRU is effective for most workloads

### Concurrency Model

Single-writer/multi-reader using `parking_lot::RwLock`:

- **Readers**: Can execute concurrently (get, scan, range)
- **Writers**: Exclusive access (put, delete)
- **Deadlock-Free**: Single lock on entire tree

### No WAL (v1)

Version 1 prioritizes simplicity:

- Dirty pages are flushed on `flush()` call
- Power loss may lose uncommitted changes
- v2 will add write-ahead logging for durability

---

## Performance

### Benchmarks

Tested on MacBook Pro M1, 16GB RAM:

| Operation | Throughput | Notes |
|-----------|------------|-------|
| Sequential Insert | ~450,000 ops/sec | Bulk insert |
| Random Insert | ~150,000 ops/sec | Scattered keys |
| Point Lookup | ~1,000,000 ops/sec | Cached pages |
| Range Scan | ~500,000 keys/sec | Sequential read |

### Scaling Characteristics

| Keys | Pages | Tree Height | Lookup I/Os |
|------|-------|-------------|-------------|
| 100 | 2 | 1 | 1 |
| 1,000 | 14 | 2 | 2 |
| 10,000 | 129 | 2 | 2 |
| 100,000 | 1,341 | 3 | 3 |
| 1,000,000 | ~13,000 | 3-4 | 3-4 |

### Optimization Tips

1. **Increase Buffer Pool**: More cache = fewer disk reads
2. **Sequential Keys**: Better page utilization
3. **Batch Operations**: Group inserts, then flush
4. **Disable Sync**: Use `sync_on_write(false)` for bulk loads

---

## Limitations

### Current Limitations (v1)

| Limitation | Description | Planned Fix |
|------------|-------------|-------------|
| No WAL | Crash may lose data | v2 |
| No MVCC | Single-version only | v2 |
| No Compression | Pages stored uncompressed | v2 |
| Single File | No sharding support | Future |
| Memory-Mapped I/O | Uses traditional read/write | Future |

### Size Limits

| Limit | Value |
|-------|-------|
| Maximum Key Size | 1 KB |
| Maximum Value Size | 1 MB |
| Maximum Pages | 4 billion (4GB page IDs) |
| Maximum File Size | ~16 TB (4B × 4KB pages) |

---

## Project Structure

```
btree-storage/
├── Cargo.toml              # Project manifest
├── README.md               # This documentation
├── src/
│   ├── lib.rs              # Public API (Db, Config)
│   ├── error.rs            # Error types
│   ├── types/
│   │   ├── mod.rs          # Constants, PageType
│   │   ├── page_id.rs      # PageId type
│   │   └── varint.rs       # Variable-length integers
│   ├── page/
│   │   ├── mod.rs          # Page module exports
│   │   ├── header.rs       # Page header structure
│   │   ├── cell.rs         # Cell encoding/decoding
│   │   └── slotted.rs      # Slotted page implementation
│   ├── storage/
│   │   ├── mod.rs          # Storage module exports
│   │   ├── disk_manager.rs # File I/O operations
│   │   ├── file_header.rs  # Database file header
│   │   └── freelist.rs     # Free page tracking
│   ├── buffer/
│   │   ├── mod.rs          # Buffer pool exports
│   │   ├── pool.rs         # Buffer pool implementation
│   │   └── lru.rs          # LRU eviction cache
│   ├── btree/
│   │   ├── mod.rs          # B-tree module exports
│   │   ├── tree.rs         # B-tree operations
│   │   └── cursor.rs       # Iterator/cursor
│   └── bin/
│       └── btree_cli.rs    # CLI tool
└── tests/
    └── test_btree.py       # Python integration tests
```

---

## Testing

### Run Rust Tests

```bash
# Run all tests
cargo test

# Run with output
cargo test -- --nocapture

# Run specific test
cargo test test_btree_many_inserts

# Run in release mode (faster)
cargo test --release
```

### Run Python Integration Tests

```bash
# Requires Python 3
python3 tests/test_btree.py
```

### Test Coverage

The test suite includes:

- **Unit Tests**: 44 tests covering all modules
- **Integration Tests**: End-to-end operations
- **Stress Tests**: Large dataset handling
- **Edge Cases**: Empty trees, single elements, boundaries

---

## Contributing

1. Fork the repository
2. Create a feature branch
3. Write tests for new functionality
4. Ensure all tests pass: `cargo test --release`
5. Run clippy: `cargo clippy`
6. Format code: `cargo fmt`
7. Submit a pull request

---

## License

This project is licensed under the MIT License.

---

## Acknowledgments

- Inspired by SQLite's B-tree implementation
- Page format influenced by PostgreSQL's slotted pages
- Buffer pool design from database systems literature

---

## Roadmap

### v1.0 (Current)
- [x] Basic B-tree operations
- [x] Buffer pool with LRU
- [x] Slotted page format
- [x] Range queries
- [x] CLI tool

### v2.0 (Planned)
- [ ] Write-Ahead Logging (WAL)
- [ ] MVCC for concurrent reads
- [ ] Page compression (LZ4/Snappy)
- [ ] Bloom filters for negative lookups
- [ ] Bulk loading optimization

### Future
- [ ] Memory-mapped I/O option
- [ ] Secondary indexes
- [ ] Distributed sharding
- [ ] Replication support
