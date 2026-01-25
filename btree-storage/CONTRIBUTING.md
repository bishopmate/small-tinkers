# Contributing

## Setup

```bash
# Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup component add clippy rustfmt

# Node.js (for UI)
# Install from https://nodejs.org/
```

## Development

```bash
# Build & test
cargo build --release
cargo test --release

# Lint & format
cargo clippy -- -D warnings
cargo fmt

# Run server + UI
cargo run --release --features server --bin btree_server
cd ui && npm install && npm run dev
```

## Code Style

- `rustfmt` for formatting
- `clippy` for lints
- Doc comments on public APIs
- Tests for new features

## Pull Requests

1. Fork & create feature branch
2. Write tests
3. Run `cargo test && cargo clippy && cargo fmt`
4. Submit PR
