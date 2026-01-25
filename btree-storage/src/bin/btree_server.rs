//! HTTP server for the B-tree storage engine.
//!
//! Provides REST API endpoints for:
//! - CRUD operations on key-value pairs
//! - Tree visualization export
//! - Configuration management

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Json,
    routing::{delete, get, post},
    Router,
};
use btree_storage::{BTreeConfig, Config, Db, DbStats, TreeNode};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};

/// Application state shared across handlers
struct AppState {
    db: Option<RwLock<Db>>,
    db_path: RwLock<Option<String>>,
    btree_config: RwLock<BTreeConfig>,
}

impl AppState {
    fn new() -> Self {
        Self {
            db: None,
            db_path: RwLock::new(None),
            btree_config: RwLock::new(BTreeConfig::default()),
        }
    }
}

/// Request to create/open a database
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateDbRequest {
    path: Option<String>,
    max_leaf_keys: Option<usize>,
    max_interior_keys: Option<usize>,
}

/// Request for key-value operations
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PutRequest {
    key: String,
    value: String,
}

/// Response for get operations
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GetResponse {
    key: String,
    value: Option<String>,
    found: bool,
}

/// Response for operations that return success/failure
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OperationResponse {
    success: bool,
    message: String,
}

/// Stats response
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StatsResponse {
    page_count: usize,
    buffer_pool_size: usize,
    tree_height: usize,
    btree_config: BTreeConfig,
}

/// Config response
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ConfigResponse {
    max_leaf_keys: usize,
    max_interior_keys: usize,
}

/// Tree visualization response
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TreeResponse {
    tree: Option<TreeNode>,
    stats: Option<StatsResponse>,
}

/// Mutable app state for database management
struct MutableAppState {
    db: RwLock<Option<Db>>,
    btree_config: RwLock<BTreeConfig>,
}

impl MutableAppState {
    fn new() -> Self {
        Self {
            db: RwLock::new(None),
            btree_config: RwLock::new(BTreeConfig::default()),
        }
    }
}

type SharedState = Arc<MutableAppState>;

#[tokio::main]
async fn main() {
    let state = Arc::new(MutableAppState::new());

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .route("/api/db", post(create_db))
        .route("/api/db", delete(close_db))
        .route("/api/config", get(get_config))
        .route("/api/config", post(set_config))
        .route("/api/kv/{key}", get(get_value))
        .route("/api/kv", post(put_value))
        .route("/api/kv/{key}", delete(delete_value))
        .route("/api/keys", get(list_keys))
        .route("/api/tree", get(get_tree))
        .route("/api/stats", get(get_stats))
        .route("/api/clear", post(clear_db))
        .route("/api/bulk", post(bulk_insert))
        .layer(cors)
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3001").await.unwrap();
    println!("🚀 B-tree server running on http://localhost:3001");
    println!("API Endpoints:");
    println!("  POST   /api/db       - Create/open database");
    println!("  DELETE /api/db       - Close database");
    println!("  GET    /api/config   - Get B-tree config");
    println!("  POST   /api/config   - Set B-tree config");
    println!("  GET    /api/kv/:key  - Get value by key");
    println!("  POST   /api/kv       - Put key-value pair");
    println!("  DELETE /api/kv/:key  - Delete key");
    println!("  GET    /api/keys     - List all keys");
    println!("  GET    /api/tree     - Get tree structure for visualization");
    println!("  GET    /api/stats    - Get database stats");
    println!("  POST   /api/clear    - Clear all data");
    println!("  POST   /api/bulk     - Bulk insert key-value pairs");
    axum::serve(listener, app).await.unwrap();
}

async fn create_db(
    State(state): State<SharedState>,
    Json(req): Json<CreateDbRequest>,
) -> Result<Json<OperationResponse>, (StatusCode, Json<OperationResponse>)> {
    let path = req.path.unwrap_or_else(|| "/tmp/btree_viz.db".to_string());

    // Update config if provided
    if req.max_leaf_keys.is_some() || req.max_interior_keys.is_some() {
        let mut config = state.btree_config.write();
        if let Some(max_leaf) = req.max_leaf_keys {
            config.max_leaf_keys = max_leaf.max(2);
        }
        if let Some(max_interior) = req.max_interior_keys {
            config.max_interior_keys = max_interior.max(2);
        }
    }

    let btree_config = state.btree_config.read().clone();

    // Delete existing file to start fresh
    let _ = std::fs::remove_file(&path);

    let config = Config::new(&path).btree_config(btree_config);

    match Db::open(config) {
        Ok(db) => {
            let mut db_lock = state.db.write();
            *db_lock = Some(db);
            Ok(Json(OperationResponse {
                success: true,
                message: format!("Database opened at {}", path),
            }))
        }
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(OperationResponse {
                success: false,
                message: format!("Failed to open database: {}", e),
            }),
        )),
    }
}

async fn close_db(
    State(state): State<SharedState>,
) -> Json<OperationResponse> {
    let mut db_lock = state.db.write();
    if db_lock.is_some() {
        if let Some(ref db) = *db_lock {
            let _ = db.flush();
        }
        *db_lock = None;
        Json(OperationResponse {
            success: true,
            message: "Database closed".to_string(),
        })
    } else {
        Json(OperationResponse {
            success: false,
            message: "No database open".to_string(),
        })
    }
}

async fn get_config(
    State(state): State<SharedState>,
) -> Json<ConfigResponse> {
    let config = state.btree_config.read();
    Json(ConfigResponse {
        max_leaf_keys: config.max_leaf_keys,
        max_interior_keys: config.max_interior_keys,
    })
}

async fn set_config(
    State(state): State<SharedState>,
    Json(req): Json<CreateDbRequest>,
) -> Json<OperationResponse> {
    let mut config = state.btree_config.write();
    if let Some(max_leaf) = req.max_leaf_keys {
        config.max_leaf_keys = max_leaf.max(2);
    }
    if let Some(max_interior) = req.max_interior_keys {
        config.max_interior_keys = max_interior.max(2);
    }
    Json(OperationResponse {
        success: true,
        message: format!(
            "Config updated: max_leaf_keys={}, max_interior_keys={}",
            config.max_leaf_keys, config.max_interior_keys
        ),
    })
}

async fn get_value(
    State(state): State<SharedState>,
    Path(key): Path<String>,
) -> Result<Json<GetResponse>, (StatusCode, Json<OperationResponse>)> {
    let db_lock = state.db.read();
    match &*db_lock {
        Some(db) => match db.get(key.as_bytes()) {
            Ok(value) => {
                let found = value.is_some();
                Ok(Json(GetResponse {
                    key: key.clone(),
                    value: value.map(|v| String::from_utf8_lossy(&v).to_string()),
                    found,
                }))
            }
            Err(e) => Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(OperationResponse {
                    success: false,
                    message: format!("Get failed: {}", e),
                }),
            )),
        },
        None => Err((
            StatusCode::BAD_REQUEST,
            Json(OperationResponse {
                success: false,
                message: "No database open".to_string(),
            }),
        )),
    }
}

async fn put_value(
    State(state): State<SharedState>,
    Json(req): Json<PutRequest>,
) -> Result<Json<OperationResponse>, (StatusCode, Json<OperationResponse>)> {
    let db_lock = state.db.read();
    match &*db_lock {
        Some(db) => match db.put(req.key.as_bytes(), req.value.as_bytes()) {
            Ok(()) => Ok(Json(OperationResponse {
                success: true,
                message: format!("Inserted key '{}'", req.key),
            })),
            Err(e) => Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(OperationResponse {
                    success: false,
                    message: format!("Put failed: {}", e),
                }),
            )),
        },
        None => Err((
            StatusCode::BAD_REQUEST,
            Json(OperationResponse {
                success: false,
                message: "No database open".to_string(),
            }),
        )),
    }
}

async fn delete_value(
    State(state): State<SharedState>,
    Path(key): Path<String>,
) -> Result<Json<OperationResponse>, (StatusCode, Json<OperationResponse>)> {
    let db_lock = state.db.read();
    match &*db_lock {
        Some(db) => match db.delete(key.as_bytes()) {
            Ok(deleted) => Ok(Json(OperationResponse {
                success: true,
                message: if deleted {
                    format!("Deleted key '{}'", key)
                } else {
                    format!("Key '{}' not found", key)
                },
            })),
            Err(e) => Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(OperationResponse {
                    success: false,
                    message: format!("Delete failed: {}", e),
                }),
            )),
        },
        None => Err((
            StatusCode::BAD_REQUEST,
            Json(OperationResponse {
                success: false,
                message: "No database open".to_string(),
            }),
        )),
    }
}

async fn list_keys(
    State(state): State<SharedState>,
) -> Result<Json<Vec<String>>, (StatusCode, Json<OperationResponse>)> {
    let db_lock = state.db.read();
    match &*db_lock {
        Some(db) => match db.iter() {
            Ok(pairs) => {
                let keys: Vec<String> = pairs
                    .into_iter()
                    .map(|(k, _)| String::from_utf8_lossy(&k).to_string())
                    .collect();
                Ok(Json(keys))
            }
            Err(e) => Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(OperationResponse {
                    success: false,
                    message: format!("List keys failed: {}", e),
                }),
            )),
        },
        None => Err((
            StatusCode::BAD_REQUEST,
            Json(OperationResponse {
                success: false,
                message: "No database open".to_string(),
            }),
        )),
    }
}

async fn get_tree(
    State(state): State<SharedState>,
) -> Result<Json<TreeResponse>, (StatusCode, Json<OperationResponse>)> {
    let db_lock = state.db.read();
    match &*db_lock {
        Some(db) => {
            let tree = db.export_tree().ok().flatten();
            let stats_data = db.stats();
            let btree_config = db.btree_config();
            let stats = Some(StatsResponse {
                page_count: stats_data.page_count,
                buffer_pool_size: stats_data.buffer_pool_size,
                tree_height: stats_data.tree_height,
                btree_config,
            });
            Ok(Json(TreeResponse { tree, stats }))
        }
        None => Err((
            StatusCode::BAD_REQUEST,
            Json(OperationResponse {
                success: false,
                message: "No database open".to_string(),
            }),
        )),
    }
}

async fn get_stats(
    State(state): State<SharedState>,
) -> Result<Json<StatsResponse>, (StatusCode, Json<OperationResponse>)> {
    let db_lock = state.db.read();
    match &*db_lock {
        Some(db) => {
            let stats = db.stats();
            let btree_config = db.btree_config();
            Ok(Json(StatsResponse {
                page_count: stats.page_count,
                buffer_pool_size: stats.buffer_pool_size,
                tree_height: stats.tree_height,
                btree_config,
            }))
        }
        None => Err((
            StatusCode::BAD_REQUEST,
            Json(OperationResponse {
                success: false,
                message: "No database open".to_string(),
            }),
        )),
    }
}

async fn clear_db(
    State(state): State<SharedState>,
) -> Result<Json<OperationResponse>, (StatusCode, Json<OperationResponse>)> {
    // Close the database and reopen it fresh
    let btree_config = state.btree_config.read().clone();
    let mut db_lock = state.db.write();

    if db_lock.is_some() {
        *db_lock = None;
    }

    let path = "/tmp/btree_viz.db";
    let _ = std::fs::remove_file(path);

    let config = Config::new(path).btree_config(btree_config);
    match Db::open(config) {
        Ok(db) => {
            *db_lock = Some(db);
            Ok(Json(OperationResponse {
                success: true,
                message: "Database cleared".to_string(),
            }))
        }
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(OperationResponse {
                success: false,
                message: format!("Failed to clear database: {}", e),
            }),
        )),
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BulkInsertRequest {
    pairs: Vec<PutRequest>,
}

async fn bulk_insert(
    State(state): State<SharedState>,
    Json(req): Json<BulkInsertRequest>,
) -> Result<Json<OperationResponse>, (StatusCode, Json<OperationResponse>)> {
    let db_lock = state.db.read();
    match &*db_lock {
        Some(db) => {
            let mut count = 0;
            for pair in req.pairs {
                if let Err(e) = db.put(pair.key.as_bytes(), pair.value.as_bytes()) {
                    return Err((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(OperationResponse {
                            success: false,
                            message: format!("Bulk insert failed at key '{}': {}", pair.key, e),
                        }),
                    ));
                }
                count += 1;
            }
            Ok(Json(OperationResponse {
                success: true,
                message: format!("Inserted {} key-value pairs", count),
            }))
        }
        None => Err((
            StatusCode::BAD_REQUEST,
            Json(OperationResponse {
                success: false,
                message: "No database open".to_string(),
            }),
        )),
    }
}
