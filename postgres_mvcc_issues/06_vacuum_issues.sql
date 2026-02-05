-- ============================================================================
-- PostgreSQL MVCC Demonstration - Part 6: Vacuum Management Issues
-- ============================================================================
-- PostgreSQL relies on VACUUM to clean up dead tuples. But VACUUM has
-- significant limitations:
--
--   1. Autovacuum default thresholds are too conservative for large tables
--   2. Long-running transactions BLOCK vacuum from cleaning tuples
--   3. Creating a vicious cycle: bloat → slow queries → longer transactions
--   4. These problems affect ALL PostgreSQL variants (RDS, Aurora, etc.)
-- ============================================================================

\timing on
SET search_path TO mvcc_demo, public;

-- ============================================================================
-- STEP 1: Examine Current Autovacuum Settings
-- ============================================================================
-- Autovacuum is PostgreSQL's background process that automatically runs VACUUM.
-- But the default settings are problematic for large tables.

SELECT name, setting, unit, short_desc
FROM pg_settings
WHERE name LIKE 'autovacuum%'
ORDER BY name;

-- Key settings explained:
--
-- autovacuum_vacuum_threshold = 50 (default)
--   Minimum dead tuples before considering vacuum
--
-- autovacuum_vacuum_scale_factor = 0.2 (default = 20%)
--   Table must have 20% dead tuples before autovacuum triggers!
--
-- Formula: vacuum when dead_tuples > threshold + scale_factor × table_rows
--
-- Problem: A 100M row table won't vacuum until 20M+ tuples are dead!

-- Calculate when autovacuum would trigger for our table:
SELECT 
    relname,
    n_live_tup AS live_rows,
    50 + (0.2 * n_live_tup)::bigint AS vacuum_trigger_threshold,
    pg_size_pretty((50 + (0.2 * n_live_tup))::bigint * 300) AS approx_dead_data_before_vacuum
FROM pg_stat_user_tables
WHERE schemaname = 'mvcc_demo' AND relname = 'movies';

-- For 500K rows: vacuum triggers after ~100K dead tuples accumulate!

-- ============================================================================
-- STEP 2: Monitor Autovacuum Activity
-- ============================================================================
-- Let's see when vacuum last ran on our table.

SELECT 
    relname,
    last_vacuum,
    last_autovacuum,
    vacuum_count,
    autovacuum_count,
    n_dead_tup AS current_dead_tuples
FROM pg_stat_user_tables
WHERE schemaname = 'mvcc_demo' AND relname = 'movies';

-- Is autovacuum currently running?
SELECT 
    pid,
    age(clock_timestamp(), query_start) AS duration,
    query
FROM pg_stat_activity
WHERE query LIKE 'autovacuum:%';

-- ============================================================================
-- STEP 3: The Long-Running Transaction Problem
-- ============================================================================
-- VACUUM cannot remove tuples that might still be visible to ANY transaction.
-- A single long-running transaction can block vacuum for the entire database!
--
-- *** THIS REQUIRES TWO SEPARATE DATABASE SESSIONS ***

-- First, let's generate some dead tuples
UPDATE mvcc_demo.movies SET updated_at = NOW() WHERE id <= 50000;

-- Check dead tuples
SELECT 
    'Before long-running txn test' AS state,
    n_live_tup, n_dead_tup,
    round(100.0 * n_dead_tup / NULLIF(n_live_tup, 0), 2) AS dead_pct
FROM pg_stat_user_tables
WHERE schemaname = 'mvcc_demo' AND relname = 'movies';

-- ----------------------------------------------------------------------------
-- SESSION 1: Start a long-running transaction that holds a snapshot
-- ----------------------------------------------------------------------------
-- Run these commands in your FIRST terminal:

-- /*
SET search_path TO mvcc_demo, public;

BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;

-- This SELECT establishes a snapshot
-- The transaction now "sees" the database as of this moment
SELECT count(*) FROM movies;

-- Get our transaction ID for reference
SELECT txid_current(), now() AS snapshot_taken_at;

-- !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
-- DO NOT COMMIT/ROLLBACK YET! Leave this transaction open.
-- Go to Session 2.
-- !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
*/

-- ----------------------------------------------------------------------------
-- SESSION 2: Generate dead tuples and try to vacuum
-- ----------------------------------------------------------------------------
-- Run these commands in your SECOND terminal:

-- /*
SET search_path TO mvcc_demo, public;

-- Generate more dead tuples
UPDATE mvcc_demo.movies SET updated_at = NOW() WHERE id <= 100000;

-- Check dead tuple count
SELECT 
    n_dead_tup, 
    pg_size_pretty(pg_relation_size('mvcc_demo.movies')) AS size
FROM pg_stat_user_tables
WHERE relname = 'movies';

-- Try to vacuum
VACUUM VERBOSE mvcc_demo.movies;

-- Check again - dead tuples should STILL BE THERE!
-- VACUUM couldn't remove them because Session 1's snapshot might need them
SELECT 
    'AFTER VACUUM (with open transaction)' AS state,
    n_dead_tup, 
    pg_size_pretty(pg_relation_size('mvcc_demo.movies')) AS size
FROM pg_stat_user_tables
WHERE relname = 'movies';
*/

-- ----------------------------------------------------------------------------
-- SESSION 2: Identify the blocking transaction
-- ----------------------------------------------------------------------------

-- /*
-- Find the oldest transaction that's blocking vacuum
SELECT 
    pid,
    usename,
    state,
    age(clock_timestamp(), xact_start) AS transaction_age,
    age(clock_timestamp(), query_start) AS query_age,
    query,
    backend_xmin
FROM pg_stat_activity
WHERE backend_xmin IS NOT NULL
ORDER BY age(backend_xmin) DESC
LIMIT 5;

-- See the oldest xmin that vacuum must preserve
SELECT 
    datname,
    age(datfrozenxid) AS database_age
FROM pg_database
WHERE datname = current_database();
*/

-- ----------------------------------------------------------------------------
-- SESSION 1: Now close the transaction
-- ----------------------------------------------------------------------------
-- Go back to Session 1 and run:

-- /*
ROLLBACK;  -- or COMMIT, doesn't matter
*/

-- ----------------------------------------------------------------------------
-- SESSION 2: Vacuum should work now
-- ----------------------------------------------------------------------------

/*
-- Run vacuum again
VACUUM VERBOSE mvcc_demo.movies;

-- Dead tuples should be gone now!
SELECT 
    'AFTER VACUUM (transaction closed)' AS state,
    n_dead_tup, 
    pg_size_pretty(pg_relation_size('mvcc_demo.movies')) AS size
FROM pg_stat_user_tables
WHERE relname = 'movies';
*/

-- ============================================================================
-- STEP 4: The Vicious Cycle
-- ============================================================================
-- Here's how the problem compounds:
--
-- 1. Long-running transaction blocks vacuum
-- 2. Dead tuples accumulate → table bloat grows
-- 3. Queries become slower (more pages to scan)
-- 4. Slow queries run longer → become long-running transactions
-- 5. These block more vacuuming → GOTO step 2
--
-- Eventually the system can spiral into unusability.

-- Check for transactions that have been running too long
SELECT 
    pid,
    usename,
    application_name,
    client_addr,
    state,
    age(clock_timestamp(), xact_start) AS transaction_duration,
    age(clock_timestamp(), query_start) AS current_query_duration,
    left(query, 50) AS query_preview,
    wait_event_type,
    wait_event
FROM pg_stat_activity
WHERE state != 'idle'
  AND xact_start IS NOT NULL
  AND age(clock_timestamp(), xact_start) > interval '1 minute'
ORDER BY xact_start ASC;

-- ============================================================================
-- STEP 5: Better Autovacuum Settings
-- ============================================================================
-- For high-update workloads, consider more aggressive settings:

-- View current settings
SHOW autovacuum_vacuum_scale_factor;
SHOW autovacuum_vacuum_threshold;

-- Recommended settings for busy tables (per-table or global):
-- These are examples - DO NOT RUN these on a production system without testing!

/*
-- Global changes (postgresql.conf):
-- autovacuum_vacuum_scale_factor = 0.05   -- 5% instead of 20%
-- autovacuum_vacuum_threshold = 1000      -- After 1000 dead tuples
-- autovacuum_naptime = 10                 -- Check every 10 seconds

-- Per-table overrides (useful for hot tables):
ALTER TABLE mvcc_demo.movies SET (
    autovacuum_vacuum_scale_factor = 0.01,  -- 1% of table
    autovacuum_vacuum_threshold = 500
);
*/

-- Check table-specific settings
SELECT 
    relname,
    reloptions
FROM pg_class
WHERE relname = 'movies' AND relnamespace = 'mvcc_demo'::regnamespace;

-- ============================================================================
-- STEP 6: Transaction ID Wraparound - The Ultimate Problem
-- ============================================================================
-- PostgreSQL transaction IDs are 32-bit integers that wrap around at ~2 billion.
-- If a table's oldest xmin gets too old, PostgreSQL will STOP accepting writes
-- to prevent data loss.
--
-- Check current age of our database:

SELECT 
    datname,
    age(datfrozenxid) AS txid_age,
    2^31 - age(datfrozenxid) AS remaining_until_wraparound,
    datfrozenxid
FROM pg_database
WHERE datname = current_database();

-- age() approaching 2 billion = DANGER!
-- PostgreSQL will freeze writes at 2^31 - 1M as a safety measure.

-- Check per-table ages:
SELECT 
    schemaname,
    relname,
    age(relfrozenxid) AS table_age,
    pg_size_pretty(pg_relation_size(schemaname || '.' || relname)) AS size
FROM pg_stat_user_tables
WHERE schemaname = 'mvcc_demo'
ORDER BY age(relfrozenxid) DESC;

-- ============================================================================
-- STEP 7: Vacuum Progress Monitoring
-- ============================================================================
-- PostgreSQL 9.6+ provides vacuum progress monitoring.

-- Run this in one terminal while vacuum runs in another:
/*
SELECT 
    p.pid,
    p.datname,
    p.relid::regclass AS table_name,
    p.phase,
    p.heap_blks_total,
    p.heap_blks_scanned,
    round(100.0 * p.heap_blks_scanned / NULLIF(p.heap_blks_total, 0), 2) AS pct_complete,
    p.index_vacuum_count,
    p.max_dead_tuples,
    p.num_dead_tuples
FROM pg_stat_progress_vacuum p;
*/

-- ============================================================================
-- STEP 8: Simulate Production Issue
-- ============================================================================
-- Let's see how quickly dead tuples can accumulate.

-- Record start time
SELECT now() AS test_start, n_dead_tup AS starting_dead_tuples
FROM pg_stat_user_tables WHERE relname = 'movies';

-- Rapid updates (simulating a busy application)
UPDATE mvcc_demo.movies SET updated_at = NOW() WHERE id <= 100000;
UPDATE mvcc_demo.movies SET updated_at = NOW() WHERE id BETWEEN 100001 AND 200000;
UPDATE mvcc_demo.movies SET updated_at = NOW() WHERE id BETWEEN 200001 AND 300000;

-- Check dead tuple accumulation
SELECT 
    now() AS current_time,
    n_live_tup,
    n_dead_tup,
    pg_size_pretty(pg_relation_size('mvcc_demo.movies')) AS table_size,
    last_autovacuum
FROM pg_stat_user_tables WHERE relname = 'movies';

-- In a real system with constant updates, autovacuum often can't keep up!

-- ============================================================================
-- Summary: Vacuum Management Issues
-- ============================================================================
-- 1. Default autovacuum settings are too conservative (20% threshold)
-- 2. Long-running transactions BLOCK vacuum from cleaning ANY tuples
-- 3. This creates a vicious cycle of bloat → slow queries → more bloat
-- 4. Transaction ID wraparound can freeze your database entirely
-- 5. All PostgreSQL variants (RDS, Aurora) share these same problems
-- 6. Manual intervention (killing queries, aggressive settings) is often needed
--
-- Recommended monitoring:
--   - Track n_dead_tup trends
--   - Alert on transactions running > 10 minutes
--   - Monitor vacuum completion frequency
--   - Watch for transaction ID age approaching limits
--
-- Next: 07_diagnostics.sql - Production-ready monitoring queries
-- ============================================================================
