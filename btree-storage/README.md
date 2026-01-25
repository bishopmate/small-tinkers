# B-Tree Storage Engine

A high-performance, disk-based B-tree storage engine in Rust with an interactive web visualizer.

## Features

- 📦 Persistent B-tree with LRU buffer pool (~450k ops/sec)
- 🔧 Configurable node limits for learning/production
- 🌐 HTTP REST API for remote access
- 🎨 React-based tree visualizer

## Prerequisites

- **Rust 1.70+**: [Install Rust](https://rustup.rs/) — `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
- **Node.js 18+** (for Web UI): [Install Node.js](https://nodejs.org/)

## Quick Start

### As a Library

```rust
use btree_storage::{Db, Config};

let db = Db::open(Config::new("my.db"))?;
db.put(b"hello", b"world")?;
let value = db.get(b"hello")?;  // Some(b"world")
db.delete(b"hello")?;
```

### CLI Tool

```bash
cargo build --release
./target/release/btree_cli mydb.db put key "value"
./target/release/btree_cli mydb.db get key
./target/release/btree_cli mydb.db bulk_insert 10000
./target/release/btree_cli mydb.db stats
```

### HTTP Server + Web UI

```bash
# Terminal 1: Start the server
cargo run --release --features server --bin btree_server

# Terminal 2: Start the UI
cd ui && npm install && npm run dev

# Open http://localhost:3000
```

**Web UI lets you:**
- Configure max keys per node (leaf/interior)
- Insert/delete keys and watch the tree restructure
- Quick-insert A-Z or 1-26 to see splits in action

## API Reference

```rust
db.put(key, value)?;           // Insert/update
db.get(key)?;                  // Point lookup → Option<Vec<u8>>
db.delete(key)?;               // Delete → bool
db.range(start, end)?;         // Range scan
db.contains(key)?;             // Existence check
db.flush()?;                   // Persist to disk
db.stats();                    // Page count, height, etc.
```

## REST API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/db` | POST | Create database |
| `/api/kv` | POST | Insert `{"key": "...", "value": "..."}` |
| `/api/kv/:key` | GET/DELETE | Get or delete key |
| `/api/tree` | GET | Tree structure (for visualization) |
| `/api/stats` | GET | Database statistics |
| `/api/bulk` | POST | Bulk insert |
| `/api/clear` | POST | Clear all data |

## Configuration

```rust
let config = Config::new("db.db")
    .buffer_pool_size(1000)   // Pages to cache
    .sync_on_write(false)     // Async for speed
    .btree_config(BTreeConfig {
        max_leaf_keys: 4,     // Keys before leaf splits
        max_interior_keys: 3, // Keys before interior splits
    });
```

## Project Structure

```
btree-storage/
├── src/
│   ├── lib.rs          # Public API
│   ├── btree/          # B-tree operations
│   ├── buffer/         # LRU buffer pool
│   ├── storage/        # Disk I/O
│   ├── page/           # Slotted page format
│   └── bin/
│       ├── btree_cli.rs    # CLI
│       └── btree_server.rs # HTTP server
├── ui/                 # React visualizer
├── docs/
│   ├── ARCHITECTURE.md # Deep-dive internals
│   └── EXAMPLES.md     # More usage examples
└── tests/
```

## Performance

| Operation | Throughput |
|-----------|------------|
| Sequential Insert | ~450k ops/sec |
| Point Lookup (cached) | ~1M ops/sec |
| Range Scan | ~500k keys/sec |

## Limitations

- No WAL (crash may lose uncommitted data)
- Single-writer model
- Max key: 1KB, Max value: 1MB

## Testing

```bash
cargo test --release
python3 tests/test_btree.py  # Integration tests
```

## Roadmap

**v1.0 ✅** — B-tree, buffer pool, CLI, HTTP API, Web UI  
**v2.0** — WAL, MVCC, compression

## License

MIT
