# B-Tree Visualizer

Interactive React app to visualize and manipulate the B-tree storage engine.

## Quick Start

```bash
# 1. Start the B-tree server (from project root)
cargo run --release --features server --bin btree_server

# 2. Start the UI
cd ui
npm install
npm run dev

# 3. Open http://localhost:3000
```

## Usage

1. **Set limits** — Configure max keys per leaf/interior node
2. **Create Database** — Initialize the tree
3. **Insert keys** — Type manually or use Quick Insert (A-Z, 1-26)
4. **Watch splits** — See nodes split when they exceed limits
5. **Delete keys** — Remove and watch restructuring

## Node Colors

- 🟢 **Green** — Leaf nodes (contain key-value data)
- 🔵 **Blue** — Interior nodes (contain separators + child pointers)

## Commands

```bash
npm run dev      # Dev server with hot reload
npm run build    # Production build
npm run preview  # Preview prod build
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/db` | POST | Create database |
| `/api/kv` | POST | Insert key-value |
| `/api/kv/:key` | GET/DELETE | Get or delete |
| `/api/tree` | GET | Tree structure |
| `/api/clear` | POST | Clear all data |
