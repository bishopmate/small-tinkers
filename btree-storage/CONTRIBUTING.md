# Contributing to B-Tree Storage Engine

Thank you for your interest in contributing! This document provides guidelines and instructions for contributing to the project.

## Table of Contents

- [Development Setup](#development-setup)
- [Code Style](#code-style)
- [Testing Guidelines](#testing-guidelines)
- [Pull Request Process](#pull-request-process)
- [Architecture Guidelines](#architecture-guidelines)

---

## Development Setup

### Prerequisites

```bash
# Install Rust (stable channel)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Install useful tools
rustup component add clippy rustfmt

# Optional: Install cargo-watch for auto-rebuild
cargo install cargo-watch
```

### Clone and Build

```bash
git clone <repository-url>
cd btree-storage

# Build in debug mode
cargo build

# Build in release mode (for benchmarks)
cargo build --release

# Run tests
cargo test

# Run with verbose output
cargo test -- --nocapture
```

### Development Workflow

```bash
# Watch for changes and auto-test
cargo watch -x test

# Check for issues without building
cargo check

# Run linter
cargo clippy -- -D warnings

# Format code
cargo fmt

# Generate and view documentation
cargo doc --open
```

---

## Code Style

### Rust Formatting

We use `rustfmt` with default settings:

```bash
# Format all files
cargo fmt

# Check formatting without modifying
cargo fmt --check
```

### Naming Conventions

| Item | Convention | Example |
|------|------------|---------|
| Types | PascalCase | `SlottedPage`, `PageId` |
| Functions | snake_case | `fetch_page`, `insert_cell` |
| Constants | SCREAMING_CASE | `PAGE_SIZE`, `MAX_KEY_SIZE` |
| Modules | snake_case | `disk_manager`, `file_header` |
| Type parameters | Single uppercase | `T`, `P` |

### Documentation

All public items must have documentation:

```rust
/// Brief one-line description.
///
/// More detailed explanation if needed. Can span
/// multiple lines.
///
/// # Arguments
///
/// * `key` - The key to search for
/// * `value` - The value to insert
///
/// # Returns
///
/// Returns `Ok(())` on success, or an error if:
/// - The key is too large
/// - The page is full
///
/// # Examples
///
/// ```rust
/// let db = Db::open(config)?;
/// db.put(b"key", b"value")?;
/// ```
pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
    // ...
}
```

### Error Handling

- Use `Result<T>` (our type alias) for fallible operations
- Create specific error variants for different failure modes
- Include context in error messages

```rust
// Good
return Err(StorageError::KeyTooLarge {
    size: key.len(),
    max: MAX_KEY_SIZE,
});

// Avoid
return Err(StorageError::InvalidData("key too large".into()));
```

### Comments

```rust
// Single-line comments for brief explanations

// Multi-line comments for more complex explanations
// that need to span multiple lines. Keep lines
// under 80 characters when possible.

/* Block comments for temporarily disabling code */

/// Doc comments for public API
/// Use these for anything users will see

//! Module-level documentation
//! Placed at the top of the file
```

---

## Testing Guidelines

### Test Organization

```
src/
├── module.rs          # Implementation
│   └── tests          # Unit tests in same file
│       mod tests {
│           #[test]
│           fn test_something() {}
│       }
tests/
└── integration.rs     # Integration tests
```

### Writing Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    // Helper function for test setup
    fn create_test_db() -> Result<(Db, tempfile::TempDir)> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let config = Config::new(&path);
        let db = Db::open(config)?;
        Ok((db, dir))
    }

    #[test]
    fn test_descriptive_name() -> Result<()> {
        // Arrange
        let (db, _dir) = create_test_db()?;

        // Act
        db.put(b"key", b"value")?;

        // Assert
        assert_eq!(db.get(b"key")?, Some(b"value".to_vec()));
        Ok(())
    }

    #[test]
    fn test_edge_case_empty_key() -> Result<()> {
        let (db, _dir) = create_test_db()?;
        
        // Empty key should work
        db.put(b"", b"value")?;
        assert_eq!(db.get(b"")?, Some(b"value".to_vec()));
        Ok(())
    }

    #[test]
    fn test_error_key_too_large() {
        let (db, _dir) = create_test_db().unwrap();
        let large_key = vec![0u8; MAX_KEY_SIZE + 1];
        
        let result = db.put(&large_key, b"value");
        assert!(matches!(result, Err(StorageError::KeyTooLarge { .. })));
    }
}
```

### Test Categories

1. **Unit Tests**: Test individual functions/methods
2. **Integration Tests**: Test component interactions
3. **Property Tests**: Test invariants (consider `proptest`)
4. **Stress Tests**: Test with large datasets

### Running Tests

```bash
# All tests
cargo test

# Specific test
cargo test test_btree_insert

# Tests in specific module
cargo test btree::

# With output
cargo test -- --nocapture

# Release mode (faster for large tests)
cargo test --release

# Single-threaded (for debugging)
cargo test -- --test-threads=1
```

---

## Pull Request Process

### Before Submitting

1. **Create an issue** describing the change (for non-trivial changes)
2. **Fork the repository** and create a feature branch
3. **Write tests** for new functionality
4. **Update documentation** if needed
5. **Run the full test suite**

### Checklist

```markdown
- [ ] Code compiles without warnings (`cargo build`)
- [ ] All tests pass (`cargo test`)
- [ ] No clippy warnings (`cargo clippy -- -D warnings`)
- [ ] Code is formatted (`cargo fmt --check`)
- [ ] Documentation is updated
- [ ] Commit messages are clear
```

### Commit Messages

Follow conventional commits:

```
type(scope): description

[optional body]

[optional footer]
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation only
- `refactor`: Code change that doesn't fix bug or add feature
- `test`: Adding or updating tests
- `perf`: Performance improvement
- `chore`: Build, CI, or tooling changes

Examples:
```
feat(btree): add range scan with custom comparator

fix(buffer): prevent eviction of pinned pages

docs(readme): add performance benchmarks

refactor(page): extract cell encoding to separate module

test(storage): add tests for free list edge cases
```

### Review Process

1. Submit PR with clear description
2. CI checks must pass
3. At least one maintainer approval required
4. Address feedback promptly
5. Squash commits if requested

---

## Architecture Guidelines

### Adding New Features

1. **Start with the interface** (trait or public API)
2. **Write tests first** (TDD encouraged)
3. **Implement incrementally**
4. **Document as you go**

### Modifying Existing Code

1. **Understand the current design** (read ARCHITECTURE.md)
2. **Maintain backwards compatibility** when possible
3. **Update tests** to cover changes
4. **Consider performance implications**

### Layer Responsibilities

| Layer | Allowed Dependencies | Responsibilities |
|-------|---------------------|------------------|
| `lib.rs` | All internal modules | Public API, config |
| `btree/` | buffer, page, types, error | Tree operations |
| `buffer/` | storage, page, types, error | Page caching |
| `storage/` | page, types, error | Disk I/O |
| `page/` | types, error | Page format |
| `types/` | error | Core types |
| `error/` | None | Error definitions |

### Performance Considerations

- **Avoid allocations** in hot paths
- **Use references** instead of cloning when possible
- **Profile before optimizing** (`cargo flamegraph`)
- **Document complexity** of algorithms

### Safety Guidelines

- **No unsafe code** without justification and review
- **Handle all errors** explicitly
- **Validate inputs** at API boundaries
- **Test edge cases** thoroughly

---

## Getting Help

- **Issues**: Report bugs or request features
- **Discussions**: Ask questions or share ideas
- **Code Review**: Request review on your PR

Thank you for contributing! 🎉
