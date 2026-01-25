# Usage Examples

This document provides practical examples for using the B-Tree Storage Engine in various scenarios.

## Table of Contents

- [Basic Operations](#basic-operations)
- [Batch Processing](#batch-processing)
- [Range Queries](#range-queries)
- [Key Design Patterns](#key-design-patterns)
- [Error Handling](#error-handling)
- [Configuration Examples](#configuration-examples)
- [Integration Examples](#integration-examples)

---

## Basic Operations

### Simple Key-Value Store

```rust
use btree_storage::{Db, Config, Result};

fn main() -> Result<()> {
    // Open database (creates if doesn't exist)
    let db = Db::open(Config::new("simple.db"))?;

    // Insert
    db.put(b"name", b"Alice")?;
    db.put(b"age", b"30")?;
    db.put(b"city", b"New York")?;

    // Retrieve
    if let Some(name) = db.get(b"name")? {
        println!("Name: {}", String::from_utf8_lossy(&name));
    }

    // Check existence
    if db.contains(b"email")? {
        println!("Email exists");
    } else {
        println!("Email not found");
    }

    // Update (same as insert)
    db.put(b"age", b"31")?;

    // Delete
    if db.delete(b"city")? {
        println!("City deleted");
    }

    // Ensure persistence
    db.flush()?;

    Ok(())
}
```

### Working with Binary Data

```rust
use btree_storage::{Db, Config, Result};

fn main() -> Result<()> {
    let db = Db::open(Config::new("binary.db"))?;

    // Store binary data
    let image_data: Vec<u8> = vec![0xFF, 0xD8, 0xFF, 0xE0]; // JPEG header
    db.put(b"image:001", &image_data)?;

    // Store serialized structs (using serde + bincode)
    #[derive(serde::Serialize, serde::Deserialize)]
    struct User {
        id: u64,
        name: String,
        active: bool,
    }

    let user = User {
        id: 1001,
        name: "Alice".to_string(),
        active: true,
    };

    let encoded = bincode::serialize(&user).unwrap();
    db.put(b"user:1001", &encoded)?;

    // Retrieve and deserialize
    if let Some(data) = db.get(b"user:1001")? {
        let user: User = bincode::deserialize(&data).unwrap();
        println!("User: {} (ID: {})", user.name, user.id);
    }

    Ok(())
}
```

---

## Batch Processing

### Bulk Insert

```rust
use btree_storage::{Db, Config, Result};
use std::time::Instant;

fn main() -> Result<()> {
    let db = Db::open(
        Config::new("bulk.db")
            .buffer_pool_size(5000)  // Larger cache for bulk ops
    )?;

    let count = 100_000;
    let start = Instant::now();

    for i in 0..count {
        let key = format!("key:{:08}", i);
        let value = format!("value:{}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
    }

    db.flush()?;
    let elapsed = start.elapsed();

    println!("Inserted {} records in {:?}", count, elapsed);
    println!("Rate: {:.0} ops/sec", count as f64 / elapsed.as_secs_f64());

    let stats = db.stats();
    println!("Pages: {}, Height: {}", stats.page_count, stats.tree_height);

    Ok(())
}
```

### Import from CSV

```rust
use btree_storage::{Db, Config, Result};
use std::fs::File;
use std::io::{BufRead, BufReader};

fn import_csv(db: &Db, path: &str) -> Result<usize> {
    let file = File::open(path).map_err(|e| {
        btree_storage::StorageError::Io(e)
    })?;
    let reader = BufReader::new(file);
    let mut count = 0;

    for line in reader.lines() {
        let line = line.map_err(btree_storage::StorageError::Io)?;
        let parts: Vec<&str> = line.splitn(2, ',').collect();
        
        if parts.len() == 2 {
            db.put(parts[0].as_bytes(), parts[1].as_bytes())?;
            count += 1;
        }
    }

    db.flush()?;
    Ok(count)
}

fn main() -> Result<()> {
    let db = Db::open(Config::new("imported.db"))?;
    let count = import_csv(&db, "data.csv")?;
    println!("Imported {} records", count);
    Ok(())
}
```

### Export to JSON

```rust
use btree_storage::{Db, Config, Result};
use std::fs::File;
use std::io::Write;

fn export_json(db: &Db, path: &str) -> Result<usize> {
    let mut file = File::create(path).map_err(btree_storage::StorageError::Io)?;
    let records = db.iter()?;

    writeln!(file, "[").map_err(btree_storage::StorageError::Io)?;

    for (i, (key, value)) in records.iter().enumerate() {
        let key_str = String::from_utf8_lossy(key);
        let value_str = String::from_utf8_lossy(value);
        
        let comma = if i < records.len() - 1 { "," } else { "" };
        writeln!(file, r#"  {{"key": "{}", "value": "{}"}}{}"#, 
            key_str, value_str, comma
        ).map_err(btree_storage::StorageError::Io)?;
    }

    writeln!(file, "]").map_err(btree_storage::StorageError::Io)?;
    Ok(records.len())
}

fn main() -> Result<()> {
    let db = Db::open(Config::new("data.db"))?;
    let count = export_json(&db, "export.json")?;
    println!("Exported {} records", count);
    Ok(())
}
```

---

## Range Queries

### Basic Range Scan

```rust
use btree_storage::{Db, Config, Result};

fn main() -> Result<()> {
    let db = Db::open(Config::new("range.db"))?;

    // Insert test data
    for i in 0..100 {
        let key = format!("item:{:03}", i);
        let value = format!("Item number {}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
    }

    // Scan range [item:020, item:030)
    println!("Range [item:020, item:030):");
    let results = db.range(Some(b"item:020"), Some(b"item:030"))?;
    for (key, value) in results {
        println!("  {} -> {}", 
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&value));
    }

    // Scan from beginning to item:005
    println!("\nFirst 5 items:");
    let results = db.range(None, Some(b"item:005"))?;
    for (key, _) in results {
        println!("  {}", String::from_utf8_lossy(&key));
    }

    // Scan from item:095 to end
    println!("\nLast 5 items:");
    let results = db.range(Some(b"item:095"), None)?;
    for (key, _) in results {
        println!("  {}", String::from_utf8_lossy(&key));
    }

    Ok(())
}
```

### Prefix Scan

```rust
use btree_storage::{Db, Config, Result};

/// Scan all keys with a given prefix
fn scan_prefix(db: &Db, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    // Create end key by incrementing the last byte of prefix
    let mut end_prefix = prefix.to_vec();
    
    // Find the end of the prefix range
    // e.g., "user:" -> "user;" (next ASCII char after ':')
    if let Some(last) = end_prefix.last_mut() {
        *last += 1;
    }

    db.range(Some(prefix), Some(&end_prefix))
}

fn main() -> Result<()> {
    let db = Db::open(Config::new("prefix.db"))?;

    // Insert users and orders
    db.put(b"user:001", b"Alice")?;
    db.put(b"user:002", b"Bob")?;
    db.put(b"user:003", b"Charlie")?;
    db.put(b"order:001", b"Order 1")?;
    db.put(b"order:002", b"Order 2")?;

    // Scan only users
    println!("Users:");
    for (key, value) in scan_prefix(&db, b"user:")? {
        println!("  {} -> {}", 
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&value));
    }

    // Scan only orders
    println!("\nOrders:");
    for (key, value) in scan_prefix(&db, b"order:")? {
        println!("  {} -> {}", 
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&value));
    }

    Ok(())
}
```

### Pagination

```rust
use btree_storage::{Db, Config, Result};

struct Page {
    items: Vec<(String, String)>,
    next_cursor: Option<String>,
}

fn paginate(db: &Db, cursor: Option<&str>, limit: usize) -> Result<Page> {
    let start = cursor.map(|s| s.as_bytes());
    let results = db.range(start, None)?;

    let mut items = Vec::new();
    let mut next_cursor = None;

    for (i, (key, value)) in results.into_iter().enumerate() {
        if i >= limit {
            next_cursor = Some(String::from_utf8_lossy(&key).to_string());
            break;
        }
        items.push((
            String::from_utf8_lossy(&key).to_string(),
            String::from_utf8_lossy(&value).to_string(),
        ));
    }

    Ok(Page { items, next_cursor })
}

fn main() -> Result<()> {
    let db = Db::open(Config::new("pagination.db"))?;

    // Insert 25 items
    for i in 0..25 {
        db.put(format!("item:{:03}", i).as_bytes(), b"value")?;
    }

    // Paginate with limit of 10
    println!("Page 1:");
    let page1 = paginate(&db, None, 10)?;
    for (key, _) in &page1.items {
        println!("  {}", key);
    }

    if let Some(cursor) = &page1.next_cursor {
        println!("\nPage 2 (cursor: {}):", cursor);
        let page2 = paginate(&db, Some(cursor), 10)?;
        for (key, _) in &page2.items {
            println!("  {}", key);
        }
    }

    Ok(())
}
```

---

## Key Design Patterns

### Composite Keys

```rust
use btree_storage::{Db, Config, Result};

/// Create a composite key from multiple parts
fn make_key(parts: &[&str]) -> Vec<u8> {
    parts.join("\x00").into_bytes()
}

/// Parse a composite key
fn parse_key(key: &[u8]) -> Vec<String> {
    String::from_utf8_lossy(key)
        .split('\x00')
        .map(|s| s.to_string())
        .collect()
}

fn main() -> Result<()> {
    let db = Db::open(Config::new("composite.db"))?;

    // Store order items: order_id + item_id -> quantity
    db.put(&make_key(&["order", "1001", "item", "A"]), b"5")?;
    db.put(&make_key(&["order", "1001", "item", "B"]), b"3")?;
    db.put(&make_key(&["order", "1002", "item", "A"]), b"2")?;

    // Query all items in order 1001
    let prefix = make_key(&["order", "1001", "item", ""]);
    let mut end = prefix.clone();
    if let Some(last) = end.last_mut() {
        *last = 0xFF;  // Max byte value
    }

    println!("Items in order 1001:");
    for (key, value) in db.range(Some(&prefix), Some(&end))? {
        let parts = parse_key(&key);
        let qty = String::from_utf8_lossy(&value);
        println!("  {} = {}", parts.join("/"), qty);
    }

    Ok(())
}
```

### Time-Series Data

```rust
use btree_storage::{Db, Config, Result};
use std::time::{SystemTime, UNIX_EPOCH};

/// Create a time-series key
fn time_key(metric: &str, timestamp: u64) -> Vec<u8> {
    format!("{}:{:016x}", metric, timestamp).into_bytes()
}

/// Get current timestamp in milliseconds
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn main() -> Result<()> {
    let db = Db::open(Config::new("timeseries.db"))?;

    // Record metrics
    let base_time = now_ms();
    for i in 0..100 {
        let ts = base_time + i * 1000;  // 1 second intervals
        let value = format!("{:.2}", 20.0 + (i as f64 * 0.1));
        
        db.put(&time_key("temperature", ts), value.as_bytes())?;
        db.put(&time_key("humidity", ts), b"65")?;
    }

    // Query temperature for last 10 seconds
    let end_time = base_time + 100_000;
    let start_time = end_time - 10_000;
    
    let start_key = time_key("temperature", start_time);
    let end_key = time_key("temperature", end_time);

    println!("Temperature readings (last 10 seconds):");
    for (key, value) in db.range(Some(&start_key), Some(&end_key))? {
        let key_str = String::from_utf8_lossy(&key);
        let value_str = String::from_utf8_lossy(&value);
        println!("  {} = {}", key_str, value_str);
    }

    Ok(())
}
```

### Secondary Index Pattern

```rust
use btree_storage::{Db, Config, Result};

struct IndexedStore {
    db: Db,
}

impl IndexedStore {
    fn new(path: &str) -> Result<Self> {
        Ok(Self {
            db: Db::open(Config::new(path))?,
        })
    }

    /// Insert a user with secondary index on email
    fn insert_user(&self, id: &str, name: &str, email: &str) -> Result<()> {
        // Primary: user:{id} -> {name}|{email}
        let primary_key = format!("user:{}", id);
        let value = format!("{}|{}", name, email);
        self.db.put(primary_key.as_bytes(), value.as_bytes())?;

        // Secondary: email:{email} -> {id}
        let index_key = format!("email:{}", email);
        self.db.put(index_key.as_bytes(), id.as_bytes())?;

        Ok(())
    }

    /// Lookup user by ID
    fn get_by_id(&self, id: &str) -> Result<Option<(String, String)>> {
        let key = format!("user:{}", id);
        if let Some(value) = self.db.get(key.as_bytes())? {
            let value_str = String::from_utf8_lossy(&value);
            let parts: Vec<&str> = value_str.splitn(2, '|').collect();
            if parts.len() == 2 {
                return Ok(Some((parts[0].to_string(), parts[1].to_string())));
            }
        }
        Ok(None)
    }

    /// Lookup user by email (using secondary index)
    fn get_by_email(&self, email: &str) -> Result<Option<(String, String, String)>> {
        let index_key = format!("email:{}", email);
        if let Some(id) = self.db.get(index_key.as_bytes())? {
            let id_str = String::from_utf8_lossy(&id);
            if let Some((name, email)) = self.get_by_id(&id_str)? {
                return Ok(Some((id_str.to_string(), name, email)));
            }
        }
        Ok(None)
    }

    /// Delete user and index
    fn delete_user(&self, id: &str) -> Result<bool> {
        // Get email for index cleanup
        if let Some((_, email)) = self.get_by_id(id)? {
            let index_key = format!("email:{}", email);
            self.db.delete(index_key.as_bytes())?;
        }
        
        let key = format!("user:{}", id);
        self.db.delete(key.as_bytes())
    }
}

fn main() -> Result<()> {
    let store = IndexedStore::new("indexed.db")?;

    store.insert_user("1", "Alice", "alice@example.com")?;
    store.insert_user("2", "Bob", "bob@example.com")?;

    // Lookup by ID
    if let Some((name, email)) = store.get_by_id("1")? {
        println!("By ID: {} <{}>", name, email);
    }

    // Lookup by email
    if let Some((id, name, email)) = store.get_by_email("bob@example.com")? {
        println!("By email: [{}] {} <{}>", id, name, email);
    }

    Ok(())
}
```

---

## Error Handling

### Comprehensive Error Handling

```rust
use btree_storage::{Db, Config, Result, StorageError};

fn robust_insert(db: &Db, key: &[u8], value: &[u8]) -> Result<()> {
    match db.put(key, value) {
        Ok(()) => {
            println!("Insert successful");
            Ok(())
        }
        Err(StorageError::KeyTooLarge { size, max }) => {
            eprintln!("Key too large: {} bytes (max: {})", size, max);
            Err(StorageError::KeyTooLarge { size, max })
        }
        Err(StorageError::ValueTooLarge { size, max }) => {
            eprintln!("Value too large: {} bytes (max: {})", size, max);
            Err(StorageError::ValueTooLarge { size, max })
        }
        Err(StorageError::Io(e)) => {
            eprintln!("I/O error: {}", e);
            // Could retry or handle gracefully
            Err(StorageError::Io(e))
        }
        Err(e) => {
            eprintln!("Unexpected error: {}", e);
            Err(e)
        }
    }
}

fn main() {
    let db = match Db::open(Config::new("errors.db")) {
        Ok(db) => db,
        Err(StorageError::Io(e)) if e.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("Permission denied. Check file permissions.");
            return;
        }
        Err(e) => {
            eprintln!("Failed to open database: {}", e);
            return;
        }
    };

    // Test with oversized key
    let large_key = vec![0u8; 2000];
    let _ = robust_insert(&db, &large_key, b"value");
}
```

### Using anyhow for Applications

```rust
use btree_storage::{Db, Config};
use anyhow::{Context, Result};

fn main() -> Result<()> {
    let db = Db::open(Config::new("app.db"))
        .context("Failed to open database")?;

    db.put(b"key", b"value")
        .context("Failed to insert record")?;

    let value = db.get(b"key")
        .context("Failed to read record")?
        .context("Key not found")?;

    println!("Value: {}", String::from_utf8_lossy(&value));
    Ok(())
}
```

---

## Configuration Examples

### Development Configuration

```rust
let config = Config::new("dev.db")
    .buffer_pool_size(100)    // Small cache
    .sync_on_write(false);    // Fast but not durable
```

### Production Configuration

```rust
let config = Config::new("/data/production.db")
    .buffer_pool_size(10_000)  // ~40MB cache
    .sync_on_write(true);      // Durable writes
```

### Memory-Constrained Environment

```rust
let config = Config::new("minimal.db")
    .buffer_pool_size(25);     // ~100KB cache
```

### High-Throughput Batch Processing

```rust
let config = Config::new("batch.db")
    .buffer_pool_size(50_000)  // ~200MB cache
    .sync_on_write(false);     // Flush manually after batch
```

---

## Integration Examples

### Web Server Integration (Actix-web)

```rust
use actix_web::{web, App, HttpResponse, HttpServer};
use btree_storage::{Db, Config};
use std::sync::Arc;

struct AppState {
    db: Arc<Db>,
}

async fn get_value(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> HttpResponse {
    let key = path.into_inner();
    
    match state.db.get(key.as_bytes()) {
        Ok(Some(value)) => HttpResponse::Ok()
            .body(String::from_utf8_lossy(&value).to_string()),
        Ok(None) => HttpResponse::NotFound().body("Key not found"),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

async fn set_value(
    state: web::Data<AppState>,
    path: web::Path<String>,
    body: web::Bytes,
) -> HttpResponse {
    let key = path.into_inner();
    
    match state.db.put(key.as_bytes(), &body) {
        Ok(()) => HttpResponse::Ok().body("OK"),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let db = Db::open(Config::new("web.db")).expect("Failed to open DB");
    let state = web::Data::new(AppState { db: Arc::new(db) });

    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .route("/kv/{key}", web::get().to(get_value))
            .route("/kv/{key}", web::put().to(set_value))
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}
```

### CLI Application (clap)

```rust
use btree_storage::{Db, Config};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "kvstore")]
#[command(about = "Simple key-value store CLI")]
struct Cli {
    /// Database file path
    #[arg(short, long, default_value = "store.db")]
    database: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Get a value by key
    Get { key: String },
    /// Set a key-value pair
    Set { key: String, value: String },
    /// Delete a key
    Del { key: String },
    /// List all keys
    List,
}

fn main() -> btree_storage::Result<()> {
    let cli = Cli::parse();
    let db = Db::open(Config::new(&cli.database))?;

    match cli.command {
        Commands::Get { key } => {
            match db.get(key.as_bytes())? {
                Some(value) => println!("{}", String::from_utf8_lossy(&value)),
                None => println!("(nil)"),
            }
        }
        Commands::Set { key, value } => {
            db.put(key.as_bytes(), value.as_bytes())?;
            println!("OK");
        }
        Commands::Del { key } => {
            if db.delete(key.as_bytes())? {
                println!("(deleted)");
            } else {
                println!("(not found)");
            }
        }
        Commands::List => {
            for (key, _) in db.iter()? {
                println!("{}", String::from_utf8_lossy(&key));
            }
        }
    }

    db.flush()?;
    Ok(())
}
```

---

## Best Practices Summary

1. **Choose appropriate buffer pool size** based on available memory and access patterns
2. **Use flush() explicitly** when durability is needed
3. **Design keys for efficient range queries** using prefixes
4. **Handle errors gracefully** - don't unwrap in production
5. **Use composite keys** for multi-attribute lookups
6. **Batch operations** when possible for better performance
7. **Monitor stats()** to understand database growth
