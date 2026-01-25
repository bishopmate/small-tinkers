//! Simple CLI for testing the B-tree storage engine.
//!
//! Usage:
//!   btree_cli <db_path> put <key> <value>
//!   btree_cli <db_path> get <key>
//!   btree_cli <db_path> delete <key>
//!   btree_cli <db_path> scan [start] [end]
//!   btree_cli <db_path> stats
//!   btree_cli <db_path> bulk_insert <count>
//!   btree_cli <db_path> debug <key>

use btree_storage::{Config, Db};
use std::env;
use std::process::exit;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 3 {
        eprintln!("Usage: btree_cli <db_path> <command> [args...]");
        eprintln!("Commands:");
        eprintln!("  put <key> <value>   - Insert or update a key-value pair");
        eprintln!("  get <key>           - Get value for a key");
        eprintln!("  delete <key>        - Delete a key");
        eprintln!("  scan [start] [end]  - Scan keys in range");
        eprintln!("  stats               - Show database statistics");
        eprintln!("  bulk_insert <count> - Insert count test records");
        exit(1);
    }

    let db_path = &args[1];
    let command = &args[2];

    let config = Config::new(db_path);
    let db = match Db::open(config) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("ERROR: Failed to open database: {}", e);
            exit(1);
        }
    };

    match command.as_str() {
        "put" => {
            if args.len() < 5 {
                eprintln!("Usage: btree_cli <db_path> put <key> <value>");
                exit(1);
            }
            let key = &args[3];
            let value = &args[4];

            match db.put(key.as_bytes(), value.as_bytes()) {
                Ok(()) => println!("OK"),
                Err(e) => {
                    eprintln!("ERROR: {}", e);
                    exit(1);
                }
            }
        }

        "get" => {
            if args.len() < 4 {
                eprintln!("Usage: btree_cli <db_path> get <key>");
                exit(1);
            }
            let key = &args[3];

            match db.get(key.as_bytes()) {
                Ok(Some(value)) => {
                    match String::from_utf8(value) {
                        Ok(s) => println!("{}", s),
                        Err(_) => println!("<binary data>"),
                    }
                }
                Ok(None) => {
                    println!("NOT_FOUND");
                }
                Err(e) => {
                    eprintln!("ERROR: {}", e);
                    exit(1);
                }
            }
        }

        "delete" => {
            if args.len() < 4 {
                eprintln!("Usage: btree_cli <db_path> delete <key>");
                exit(1);
            }
            let key = &args[3];

            match db.delete(key.as_bytes()) {
                Ok(true) => println!("DELETED"),
                Ok(false) => println!("NOT_FOUND"),
                Err(e) => {
                    eprintln!("ERROR: {}", e);
                    exit(1);
                }
            }
        }

        "scan" => {
            let start = args.get(3).map(|s| s.as_bytes());
            let end = args.get(4).map(|s| s.as_bytes());

            match db.range(start, end) {
                Ok(results) => {
                    println!("COUNT: {}", results.len());
                    for (key, value) in results {
                        let key_str = String::from_utf8_lossy(&key);
                        let value_str = String::from_utf8_lossy(&value);
                        println!("{} -> {}", key_str, value_str);
                    }
                }
                Err(e) => {
                    eprintln!("ERROR: {}", e);
                    exit(1);
                }
            }
        }

        "stats" => {
            let stats = db.stats();
            println!("page_count: {}", stats.page_count);
            println!("buffer_pool_size: {}", stats.buffer_pool_size);
            println!("tree_height: {}", stats.tree_height);
        }

        "bulk_insert" => {
            if args.len() < 4 {
                eprintln!("Usage: btree_cli <db_path> bulk_insert <count>");
                exit(1);
            }
            let count: usize = match args[3].parse() {
                Ok(n) => n,
                Err(_) => {
                    eprintln!("ERROR: Invalid count");
                    exit(1);
                }
            };

            let start = std::time::Instant::now();
            for i in 0..count {
                let key = format!("key_{:08}", i);
                let value = format!("value_{}", i);
                if let Err(e) = db.put(key.as_bytes(), value.as_bytes()) {
                    eprintln!("ERROR at {}: {}", i, e);
                    exit(1);
                }
            }
            let elapsed = start.elapsed();

            if let Err(e) = db.flush() {
                eprintln!("ERROR flushing: {}", e);
                exit(1);
            }

            let ops_per_sec = count as f64 / elapsed.as_secs_f64();
            println!("INSERTED: {}", count);
            println!("TIME_MS: {}", elapsed.as_millis());
            println!("OPS_PER_SEC: {:.0}", ops_per_sec);
        }

        "debug" => {
            if args.len() < 4 {
                eprintln!("Usage: btree_cli <db_path> debug <key>");
                exit(1);
            }
            let key = &args[3];

            match db.debug_get(key.as_bytes()) {
                Ok(trace) => {
                    for line in trace {
                        println!("{}", line);
                    }
                }
                Err(e) => {
                    eprintln!("ERROR: {}", e);
                    exit(1);
                }
            }
        }

        _ => {
            eprintln!("Unknown command: {}", command);
            exit(1);
        }
    }

    // Ensure data is persisted
    if let Err(e) = db.flush() {
        eprintln!("Warning: Failed to flush: {}", e);
    }
}
