-- ============================================================================
-- PostgreSQL MVCC Demonstration - Part 3: Version Copying Overhead
-- ============================================================================
-- PostgreSQL uses "append-only" version storage. When you UPDATE a row,
-- even if you change just ONE column, PostgreSQL copies the ENTIRE tuple
-- to a new location.
--
-- This is different from Oracle/MySQL which store compact "delta records"
-- containing only the changed values (like a git diff).
--
-- Problem: A table with 100 columns where you update 1 column still
-- creates a full copy of all 100 column values.
-- ============================================================================

\timing on
SET search_path TO mvcc_demo, public;

-- ============================================================================
-- STEP 1: Baseline Measurements
-- ============================================================================
-- Let's first understand our table's current size and structure.

-- Refresh statistics
ANALYZE mvcc_demo.movies;
VACUUM mvcc_demo.movies;

-- Capture baseline sizes
SELECT 
    'BASELINE' AS measurement,
    pg_relation_size('mvcc_demo.movies') AS table_bytes,
    pg_size_pretty(pg_relation_size('mvcc_demo.movies')) AS table_size,
    (SELECT count(*) FROM mvcc_demo.movies) AS row_count,
    pg_relation_size('mvcc_demo.movies') / (SELECT count(*) FROM mvcc_demo.movies) AS bytes_per_row;

-- Calculate average tuple size using pageinspect (if available) or estimate
-- Each row in our movies table is approximately:
--   - 23 bytes: tuple header (HeapTupleHeaderData)
--   - ~8 bytes: alignment padding
--   - Variable: actual column data (id, title, description, etc.)
-- Our rows are roughly 200-300 bytes each due to the TEXT description field.

-- ============================================================================
-- STEP 2: Update a SINGLE Tiny Column on Many Rows
-- ============================================================================
-- We'll update just `updated_at` (8 bytes) on 100,000 rows.
-- Despite changing only 8 bytes, PostgreSQL will copy ~250 bytes × 100K rows.
--
-- Expected overhead: ~25 MB of new data for a 0.8 MB logical change!

-- Get size before
SELECT pg_relation_size('mvcc_demo.movies') AS size_before_update;

-- Update 100,000 rows - changing only the timestamp column
UPDATE mvcc_demo.movies 
SET updated_at = NOW()
WHERE id <= 100000;

-- Get size after (table grew!)
SELECT 
    'AFTER 100K UPDATES' AS measurement,
    pg_relation_size('mvcc_demo.movies') AS size_after,
    pg_size_pretty(pg_relation_size('mvcc_demo.movies')) AS table_size;

-- Calculate the overhead
WITH sizes AS (
    SELECT 
        100000::bigint AS rows_updated,
        8::bigint AS bytes_changed_per_row,  -- Just the timestamp
        pg_relation_size('mvcc_demo.movies') AS current_size
)
SELECT 
    rows_updated,
    pg_size_pretty(rows_updated * bytes_changed_per_row) AS logical_change,
    pg_size_pretty(current_size) AS actual_table_size,
    -- Note: Table grew by roughly 250-300 bytes per row (full tuple copies)
    'PostgreSQL copies ENTIRE tuples, not just changed columns!' AS explanation
FROM sizes;

-- ============================================================================
-- STEP 3: Examine Dead Tuples Created
-- ============================================================================
-- Those 100K old versions are now "dead tuples" taking up space.

SELECT 
    relname,
    n_live_tup AS live_tuples,
    n_dead_tup AS dead_tuples,
    pg_size_pretty(pg_relation_size('mvcc_demo.movies')) AS table_size,
    round(100.0 * n_dead_tup / NULLIF(n_live_tup + n_dead_tup, 0), 2) AS dead_pct
FROM pg_stat_user_tables
WHERE schemaname = 'mvcc_demo' AND relname = 'movies';

-- ============================================================================
-- STEP 4: Compare with a Wide-Column Update
-- ============================================================================
-- Even updating a tiny column in a row with large columns copies everything.
-- Let's update a row and see both old and new versions' sizes.

-- Find a specific row's physical location and size
SELECT 
    ctid,
    octet_length(title) + octet_length(description) + 
    octet_length(genre) + octet_length(director) AS data_bytes,
    id, title
FROM mvcc_demo.movies
WHERE id = 1;

-- Update just the rating (4 bytes of actual change)
UPDATE mvcc_demo.movies SET rating = 1.0 WHERE id = 1;

-- The new version is a complete copy - same size as before
SELECT 
    ctid,
    octet_length(title) + octet_length(description) + 
    octet_length(genre) + octet_length(director) AS data_bytes,
    id, rating
FROM mvcc_demo.movies
WHERE id = 1;

-- ============================================================================
-- STEP 5: The Impact - Write Amplification
-- ============================================================================
-- "Write Amplification" = how much physical I/O vs logical change
--
-- PostgreSQL's approach:
--   Logical change: 8 bytes (timestamp)
--   Physical write: ~300 bytes (full tuple) + index updates
--   Write amplification: 37x just for the heap, more with indexes!
--
-- Oracle/MySQL delta approach:
--   Logical change: 8 bytes
--   Physical write: ~30-50 bytes (header + delta)
--   Write amplification: 4-6x
--
-- This matters for:
--   - SSD wear (limited write cycles)
--   - IOPS-based billing (AWS Aurora charges per I/O)
--   - Replication bandwidth
--   - Backup sizes

-- Demonstrate: Update another batch and watch size grow
SELECT pg_relation_size('mvcc_demo.movies') AS before_second_batch;

UPDATE mvcc_demo.movies 
SET updated_at = NOW()
WHERE id BETWEEN 100001 AND 200000;

SELECT 
    pg_relation_size('mvcc_demo.movies') AS after_second_batch,
    pg_size_pretty(pg_relation_size('mvcc_demo.movies')) AS size_pretty;

-- ============================================================================
-- STEP 6: The zheap Project (Context)
-- ============================================================================
-- PostgreSQL developers started the "zheap" project in 2013 to add
-- in-place updates and undo logging (similar to Oracle).
--
-- Status: The project stalled around 2021 and is not in production PostgreSQL.
-- This means append-only storage is here to stay for the foreseeable future.
--
-- See: https://github.com/EnterpriseDB/zheap

-- ============================================================================
-- STEP 7: Cleanup for Next Demo (Optional)
-- ============================================================================
-- Uncomment to reclaim space before the next demo

VACUUM (VERBOSE) mvcc_demo.movies;

-- Or for full space reclamation (acquires exclusive lock!):
VACUUM FULL mvcc_demo.movies;

-- ============================================================================
-- Summary: The Cost of Version Copying
-- ============================================================================
-- 1. PostgreSQL copies the ENTIRE tuple on every update, regardless of
--    how small the actual change is.
--
-- 2. A 1000-column row updated on 1 column = full copy of 999 unchanged columns.
--
-- 3. This creates significant write amplification:
--    - More I/O than necessary
--    - More WAL (write-ahead log) traffic
--    - Faster table bloat
--
-- 4. The zheap project aimed to fix this but has stalled.
--
-- Next: 04_table_bloat.sql - See how dead tuples accumulate and cause problems
-- ============================================================================
