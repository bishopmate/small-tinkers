-- ============================================================================
-- PostgreSQL MVCC Demonstration - Part 4: Table Bloat
-- ============================================================================
-- "Table bloat" occurs when dead tuples (old row versions) accumulate
-- faster than VACUUM can remove them. Dead tuples:
--
--   1. Consume disk space
--   2. Slow down sequential scans (must check visibility on every tuple)
--   3. Pollute the buffer pool with useless pages
--   4. Cause inaccurate statistics → poor query plans
--
-- This is one of PostgreSQL's most criticized aspects.
-- ============================================================================

\timing on
SET search_path TO mvcc_demo, public;

-- ============================================================================
-- STEP 1: Establish a Clean Baseline
-- ============================================================================
-- First, let's vacuum the table to start fresh.

VACUUM (VERBOSE) mvcc_demo.movies;
ANALYZE mvcc_demo.movies;

-- Record baseline state
SELECT 
    'BASELINE (after vacuum)' AS state,
    n_live_tup AS live,
    n_dead_tup AS dead,
    pg_size_pretty(pg_relation_size('mvcc_demo.movies')) AS table_size
FROM pg_stat_user_tables
WHERE schemaname = 'mvcc_demo' AND relname = 'movies';

-- ============================================================================
-- STEP 2: Generate Massive Dead Tuple Accumulation
-- ============================================================================
-- We'll update all 500K rows multiple times to create dead tuples.
-- Each update pass creates 500K new dead tuples.
--
-- Warning: This takes 1-2 minutes per pass.

-- Pass 1: Update all rows
SELECT 'Starting Pass 1 at ' || clock_timestamp();
UPDATE mvcc_demo.movies SET updated_at = NOW();
SELECT 'Pass 1 complete. Dead tuples:', n_dead_tup 
FROM pg_stat_user_tables WHERE relname = 'movies';

-- Pass 2
SELECT 'Starting Pass 2 at ' || clock_timestamp();
UPDATE mvcc_demo.movies SET updated_at = NOW();

-- Pass 3
SELECT 'Starting Pass 3 at ' || clock_timestamp();
UPDATE mvcc_demo.movies SET updated_at = NOW();

-- Check dead tuple accumulation
SELECT 
    'AFTER 3 UPDATE PASSES' AS state,
    n_live_tup AS live,
    n_dead_tup AS dead,
    pg_size_pretty(pg_relation_size('mvcc_demo.movies')) AS table_size,
    round(100.0 * n_dead_tup / NULLIF(n_live_tup, 0), 1) AS dead_to_live_ratio_pct
FROM pg_stat_user_tables
WHERE schemaname = 'mvcc_demo' AND relname = 'movies';

-- ============================================================================
-- STEP 3: Observe the Bloat
-- ============================================================================
-- The table now contains 500K live rows + ~1.5M dead rows = massive bloat!

-- Method 1: Simple dead tuple ratio
SELECT 
    schemaname,
    relname,
    n_live_tup,
    n_dead_tup,
    round(100.0 * n_dead_tup / NULLIF(n_live_tup + n_dead_tup, 0), 2) AS bloat_pct,
    pg_size_pretty(pg_relation_size(schemaname || '.' || relname)) AS size
FROM pg_stat_user_tables
WHERE schemaname = 'mvcc_demo'
ORDER BY n_dead_tup DESC;

-- Method 2: Using pgstattuple for accurate measurement
SELECT 
    table_len AS total_bytes,
    pg_size_pretty(table_len) AS total_size,
    tuple_count AS live_tuples,
    dead_tuple_count AS dead_tuples,
    pg_size_pretty(dead_tuple_len) AS dead_tuple_size,
    round(100.0 * dead_tuple_len / NULLIF(table_len, 0), 2) AS dead_space_pct,
    pg_size_pretty(free_space) AS free_space
FROM pgstattuple('mvcc_demo.movies');

-- ============================================================================
-- STEP 4: Impact on Query Performance
-- ============================================================================
-- A sequential scan must check EVERY tuple (including dead ones) for visibility.
-- More dead tuples = slower scans.

-- Let's time a count(*) which does a sequential scan
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT count(*) FROM mvcc_demo.movies;

-- Notice:
-- - "Rows Removed by Filter" might show dead tuples filtered
-- - "Buffers: shared hit/read" shows pages accessed
-- - Many pages contain mostly dead tuples = wasted I/O

-- ============================================================================
-- STEP 5: VACUUM - Reclaim Space for Reuse (Not Disk!)
-- ============================================================================
-- Regular VACUUM marks dead tuple space as "reusable" but does NOT shrink
-- the file or return space to the OS.

SELECT 'BEFORE VACUUM' AS state,
    pg_relation_size('mvcc_demo.movies') AS bytes,
    pg_size_pretty(pg_relation_size('mvcc_demo.movies')) AS size;

-- Run regular vacuum
VACUUM (VERBOSE) mvcc_demo.movies;

-- Check results
SELECT 'AFTER VACUUM' AS state,
    pg_relation_size('mvcc_demo.movies') AS bytes,
    pg_size_pretty(pg_relation_size('mvcc_demo.movies')) AS size;

-- Dead tuples are gone, but file size is THE SAME!
SELECT 
    'Post-VACUUM stats' AS state,
    n_live_tup AS live,
    n_dead_tup AS dead,
    pg_size_pretty(pg_relation_size('mvcc_demo.movies')) AS table_size
FROM pg_stat_user_tables
WHERE schemaname = 'mvcc_demo' AND relname = 'movies';

-- The space is now "free" for PostgreSQL to reuse, but the file hasn't shrunk.
-- You can see free space with pgstattuple:
SELECT 
    pg_size_pretty(table_len) AS total_size,
    pg_size_pretty(tuple_len) AS live_data,
    pg_size_pretty(free_space) AS reusable_free_space,
    round(100.0 * free_space / NULLIF(table_len, 0), 2) AS free_pct
FROM pgstattuple('mvcc_demo.movies');

-- ============================================================================
-- STEP 6: VACUUM FULL - Actually Reclaim Disk Space
-- ============================================================================
-- VACUUM FULL rewrites the entire table, reclaiming space to the OS.
--
-- ⚠️  WARNING: VACUUM FULL requires an EXCLUSIVE LOCK on the table!
--     - No reads or writes allowed during operation
--     - Temporarily doubles storage (writes new copy, then deletes old)
--     - Can take a very long time on large tables
--     - In production, use pg_repack instead (online rebuild)

SELECT 'BEFORE VACUUM FULL' AS state,
    pg_size_pretty(pg_relation_size('mvcc_demo.movies')) AS size;

-- This will lock the table exclusively
VACUUM FULL mvcc_demo.movies;

SELECT 'AFTER VACUUM FULL' AS state,
    pg_size_pretty(pg_relation_size('mvcc_demo.movies')) AS size;

-- Now the file has actually shrunk!

-- ============================================================================
-- STEP 7: Visualizing the Problem
-- ============================================================================
-- Let's create bloat again and see the numbers clearly.

-- Generate more dead tuples
UPDATE mvcc_demo.movies SET updated_at = NOW() WHERE id <= 250000;
UPDATE mvcc_demo.movies SET updated_at = NOW() WHERE id <= 250000;

-- The bloat scenario:
SELECT 
    '--- BLOAT ANALYSIS ---' AS header;

SELECT 
    'Live tuples' AS metric,
    n_live_tup::text AS value
FROM pg_stat_user_tables WHERE relname = 'movies'
UNION ALL
SELECT 
    'Dead tuples',
    n_dead_tup::text
FROM pg_stat_user_tables WHERE relname = 'movies'
UNION ALL
SELECT 
    'Table size on disk',
    pg_size_pretty(pg_relation_size('mvcc_demo.movies'))
UNION ALL
SELECT 
    'Estimated live data size',
    pg_size_pretty(
        (SELECT avg_tuple_len * n_live_tup 
         FROM (SELECT (sum(pg_column_size(movies.*)))::float / count(*) as avg_tuple_len 
               FROM mvcc_demo.movies LIMIT 1000) x,
              pg_stat_user_tables 
         WHERE relname = 'movies')::bigint
    );

-- ============================================================================
-- STEP 8: Production Reality Check
-- ============================================================================
-- In real production scenarios:
--
-- 1. A table with 100M rows might have 400M dead tuples (80% bloat)
-- 2. Sequential scans read 500GB instead of 100GB
-- 3. Buffer pool is polluted with dead data
-- 4. VACUUM FULL isn't viable (exclusive lock for hours)
-- 5. pg_repack requires disk space for a copy
--
-- Query to estimate real-world bloat:

SELECT 
    schemaname || '.' || relname AS table_name,
    pg_size_pretty(pg_total_relation_size(schemaname || '.' || relname)) AS total_size,
    n_live_tup AS live_tuples,
    n_dead_tup AS dead_tuples,
    CASE 
        WHEN n_live_tup + n_dead_tup = 0 THEN 0
        ELSE round(100.0 * n_dead_tup / (n_live_tup + n_dead_tup), 2)
    END AS dead_pct,
    last_vacuum,
    last_autovacuum
FROM pg_stat_user_tables
WHERE schemaname = 'mvcc_demo'
ORDER BY n_dead_tup DESC;

-- ============================================================================
-- Summary: Table Bloat
-- ============================================================================
-- 1. Every UPDATE creates a new tuple version; old versions are "dead"
-- 2. Dead tuples consume space and slow down scans
-- 3. Regular VACUUM marks space as reusable but doesn't shrink files
-- 4. Only VACUUM FULL or pg_repack can reclaim disk space
-- 5. VACUUM FULL requires exclusive lock - not viable in production
-- 6. High-update workloads need aggressive vacuum tuning
--
-- Next: 05_index_bloat.sql - Indexes have their own bloat problem
-- ============================================================================
