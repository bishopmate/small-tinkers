# Usage Examples

## Basic Operations

```rust
use btree_storage::{Db, Config, Result};

fn main() -> Result<()> {
    let db = Db::open(Config::new("my.db"))?;

    // CRUD
    db.put(b"user:1", b"Alice")?;
    let val = db.get(b"user:1")?;        // Some(b"Alice")
    let exists = db.contains(b"user:1")?; // true
    db.delete(b"user:1")?;

    // Range scan
    for (k, v) in db.range(Some(b"user:1"), Some(b"user:9"))? {
        println!("{} = {}", String::from_utf8_lossy(&k), String::from_utf8_lossy(&v));
    }

    db.flush()?;
    Ok(())
}
```

## Configuration

```rust
use btree_storage::{Config, BTreeConfig};

// High performance
let config = Config::new("fast.db")
    .buffer_pool_size(10000)
    .sync_on_write(false);

// Custom node limits (for visualization)
let config = Config::new("visual.db")
    .btree_config(BTreeConfig {
        max_leaf_keys: 3,
        max_interior_keys: 2,
    });
```

## HTTP API

```bash
# Start server
cargo run --release --features server --bin btree_server

# Create database
curl -X POST http://localhost:3001/api/db \
  -H "Content-Type: application/json" \
  -d '{"path": "demo.db", "maxLeafKeys": 4}'

# Insert
curl -X POST http://localhost:3001/api/kv \
  -H "Content-Type: application/json" \
  -d '{"key": "hello", "value": "world"}'

# Get
curl http://localhost:3001/api/kv/hello

# Get tree structure
curl http://localhost:3001/api/tree

# Delete
curl -X DELETE http://localhost:3001/api/kv/hello
```

## CLI

```bash
./target/release/btree_cli mydb.db put key "value"
./target/release/btree_cli mydb.db get key
./target/release/btree_cli mydb.db scan
./target/release/btree_cli mydb.db bulk_insert 10000
./target/release/btree_cli mydb.db stats
```
