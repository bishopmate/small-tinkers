#!/usr/bin/env python3
"""
Test script for the B-tree storage engine.

This script builds the Rust CLI tool and runs various tests against it
to verify the B-tree storage engine works correctly.
"""

import os
import subprocess
import tempfile
import shutil
import time
from pathlib import Path
from typing import Optional, Tuple, List

# Path to the project root
PROJECT_ROOT = Path(__file__).parent.parent
CLI_BINARY = PROJECT_ROOT / "target" / "release" / "btree_cli"


class BTreeTestClient:
    """Client wrapper for testing the B-tree CLI."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.cli_path = str(CLI_BINARY)

    def _run(self, *args) -> Tuple[int, str, str]:
        """Run CLI command and return (exit_code, stdout, stderr)."""
        cmd = [self.cli_path, self.db_path] + list(args)
        result = subprocess.run(cmd, capture_output=True, text=True)
        return result.returncode, result.stdout.strip(), result.stderr.strip()

    def put(self, key: str, value: str) -> bool:
        """Insert or update a key-value pair."""
        code, stdout, stderr = self._run("put", key, value)
        if code != 0:
            print(f"PUT ERROR: {stderr}")
            return False
        return stdout == "OK"

    def get(self, key: str) -> Optional[str]:
        """Get value for a key, returns None if not found."""
        code, stdout, stderr = self._run("get", key)
        if code != 0:
            print(f"GET ERROR: {stderr}")
            return None
        if stdout == "NOT_FOUND":
            return None
        return stdout

    def delete(self, key: str) -> bool:
        """Delete a key, returns True if deleted."""
        code, stdout, stderr = self._run("delete", key)
        if code != 0:
            print(f"DELETE ERROR: {stderr}")
            return False
        return stdout == "DELETED"

    def scan(self, start: Optional[str] = None, end: Optional[str] = None) -> List[Tuple[str, str]]:
        """Scan keys in range, returns list of (key, value) tuples."""
        args = ["scan"]
        if start is not None:
            args.append(start)
        if end is not None:
            args.append(end)

        code, stdout, stderr = self._run(*args)
        if code != 0:
            print(f"SCAN ERROR: {stderr}")
            return []

        results = []
        lines = stdout.split("\n")
        for line in lines[1:]:  # Skip COUNT line
            if " -> " in line:
                key, value = line.split(" -> ", 1)
                results.append((key, value))
        return results

    def stats(self) -> dict:
        """Get database statistics."""
        code, stdout, stderr = self._run("stats")
        if code != 0:
            print(f"STATS ERROR: {stderr}")
            return {}

        stats = {}
        for line in stdout.split("\n"):
            if ": " in line:
                key, value = line.split(": ", 1)
                stats[key] = int(value)
        return stats

    def bulk_insert(self, count: int) -> dict:
        """Bulk insert test records."""
        code, stdout, stderr = self._run("bulk_insert", str(count))
        if code != 0:
            print(f"BULK_INSERT ERROR: {stderr}")
            return {}

        result = {}
        for line in stdout.split("\n"):
            if ": " in line:
                key, value = line.split(": ", 1)
                result[key] = float(value) if "." in value else int(value)
        return result


def build_cli():
    """Build the CLI binary in release mode."""
    print("Building btree_cli in release mode...")
    result = subprocess.run(
        ["cargo", "build", "--release", "--bin", "btree_cli"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        print("Build failed:")
        print(result.stderr)
        return False
    print("Build successful!")
    return True


def test_basic_operations(client: BTreeTestClient) -> bool:
    """Test basic put/get/delete operations."""
    print("\n=== Test: Basic Operations ===")

    # Test put and get
    assert client.put("hello", "world"), "Put failed"
    assert client.get("hello") == "world", "Get failed"
    print("✓ Put/Get works")

    # Test update
    assert client.put("hello", "updated"), "Update put failed"
    assert client.get("hello") == "updated", "Update get failed"
    print("✓ Update works")

    # Test non-existent key
    assert client.get("nonexistent") is None, "Expected None for missing key"
    print("✓ Missing key returns None")

    # Test delete
    assert client.delete("hello"), "Delete failed"
    assert client.get("hello") is None, "Key should be deleted"
    print("✓ Delete works")

    # Test delete non-existent
    assert not client.delete("nonexistent"), "Delete of missing key should return False"
    print("✓ Delete of missing key handled")

    return True


def test_multiple_keys(client: BTreeTestClient) -> bool:
    """Test with multiple keys."""
    print("\n=== Test: Multiple Keys ===")

    keys = ["apple", "banana", "cherry", "date", "elderberry"]
    for i, key in enumerate(keys):
        assert client.put(key, f"value_{i}"), f"Put {key} failed"

    # Verify all keys
    for i, key in enumerate(keys):
        value = client.get(key)
        assert value == f"value_{i}", f"Get {key} returned wrong value: {value}"

    print(f"✓ Inserted and retrieved {len(keys)} keys")

    # Test scan all
    results = client.scan()
    assert len(results) == len(keys), f"Scan returned {len(results)}, expected {len(keys)}"
    # Results should be sorted
    result_keys = [r[0] for r in results]
    assert result_keys == sorted(result_keys), "Scan results not sorted"
    print("✓ Full scan returns sorted results")

    return True


def test_range_scan(client: BTreeTestClient) -> bool:
    """Test range scan operations."""
    print("\n=== Test: Range Scan ===")

    # Insert ordered data
    for i in range(10):
        key = f"key_{i:02d}"
        value = f"val_{i}"
        assert client.put(key, value), f"Put {key} failed"

    # Scan range
    results = client.scan("key_03", "key_07")
    result_keys = [r[0] for r in results]

    # Should include key_03, key_04, key_05, key_06 (not key_07)
    expected = ["key_03", "key_04", "key_05", "key_06"]
    assert result_keys == expected, f"Range scan returned {result_keys}, expected {expected}"
    print(f"✓ Range scan [key_03, key_07) returned correct {len(results)} keys")

    return True


def test_persistence(temp_dir: str) -> bool:
    """Test that data persists across database reopens."""
    print("\n=== Test: Persistence ===")

    db_path = os.path.join(temp_dir, "persist_test.db")

    # Write some data
    client1 = BTreeTestClient(db_path)
    assert client1.put("persist_key", "persist_value"), "Initial put failed"
    print("✓ Written data to database")

    # Open database again and verify
    client2 = BTreeTestClient(db_path)
    value = client2.get("persist_key")
    assert value == "persist_value", f"Persistence check failed: got {value}"
    print("✓ Data persisted across database reopen")

    return True


def test_large_dataset(client: BTreeTestClient) -> bool:
    """Test with a larger dataset to trigger page splits."""
    print("\n=== Test: Large Dataset (1000 keys) ===")

    result = client.bulk_insert(1000)
    assert result.get("INSERTED") == 1000, f"Bulk insert failed: {result}"

    ops_per_sec = result.get("OPS_PER_SEC", 0)
    time_ms = result.get("TIME_MS", 0)
    print(f"✓ Inserted 1000 keys in {time_ms}ms ({ops_per_sec:.0f} ops/sec)")

    # Verify some random keys
    for i in [0, 499, 999]:
        key = f"key_{i:08d}"
        value = client.get(key)
        expected = f"value_{i}"
        assert value == expected, f"Key {key}: expected {expected}, got {value}"
    print("✓ Verified sample keys from large dataset")

    # Check stats
    stats = client.stats()
    print(f"✓ Stats: {stats}")
    assert stats.get("page_count", 0) > 1, "Expected multiple pages for large dataset"

    return True


def test_special_characters(client: BTreeTestClient) -> bool:
    """Test keys and values with special characters."""
    print("\n=== Test: Special Characters ===")

    test_cases = [
        ("key_with_underscore", "value_with_underscore"),
        ("key-with-dash", "value-with-dash"),
        ("key.with.dots", "value.with.dots"),
        ("key123", "value456"),
        ("UPPERCASE", "lowercase"),
    ]

    for key, value in test_cases:
        assert client.put(key, value), f"Put {key} failed"
        result = client.get(key)
        assert result == value, f"Get {key}: expected {value}, got {result}"

    print(f"✓ {len(test_cases)} special character cases passed")
    return True


def run_all_tests():
    """Run all tests."""
    print("=" * 60)
    print("B-Tree Storage Engine Test Suite")
    print("=" * 60)

    # Build the CLI
    if not build_cli():
        print("\n❌ Build failed!")
        return False

    # Create temp directory for test databases
    temp_dir = tempfile.mkdtemp(prefix="btree_test_")
    print(f"\nUsing temp directory: {temp_dir}")

    try:
        # Run tests
        all_passed = True

        # Test 1: Basic operations
        db1 = os.path.join(temp_dir, "test1.db")
        client1 = BTreeTestClient(db1)
        if not test_basic_operations(client1):
            all_passed = False

        # Test 2: Multiple keys
        db2 = os.path.join(temp_dir, "test2.db")
        client2 = BTreeTestClient(db2)
        if not test_multiple_keys(client2):
            all_passed = False

        # Test 3: Range scan
        db3 = os.path.join(temp_dir, "test3.db")
        client3 = BTreeTestClient(db3)
        if not test_range_scan(client3):
            all_passed = False

        # Test 4: Persistence
        if not test_persistence(temp_dir):
            all_passed = False

        # Test 5: Large dataset
        db5 = os.path.join(temp_dir, "test5.db")
        client5 = BTreeTestClient(db5)
        if not test_large_dataset(client5):
            all_passed = False

        # Test 6: Special characters
        db6 = os.path.join(temp_dir, "test6.db")
        client6 = BTreeTestClient(db6)
        if not test_special_characters(client6):
            all_passed = False

        # Summary
        print("\n" + "=" * 60)
        if all_passed:
            print("✅ All tests passed!")
        else:
            print("❌ Some tests failed!")
        print("=" * 60)

        return all_passed

    except AssertionError as e:
        print(f"\n❌ Test assertion failed: {e}")
        return False
    except Exception as e:
        print(f"\n❌ Test error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        # Cleanup
        print(f"\nCleaning up temp directory: {temp_dir}")
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    import sys
    success = run_all_tests()
    sys.exit(0 if success else 1)
