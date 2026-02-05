-- ============================================================================
-- PostgreSQL MVCC Demonstration - Part 2: MVCC Basics
-- ============================================================================
-- This script demonstrates the fundamental concepts of Multi-Version
-- Concurrency Control (MVCC) in PostgreSQL.
--
-- MVCC allows readers and writers to not block each other by maintaining
-- multiple versions of each row. Each transaction sees a consistent
-- snapshot of the database.
-- ============================================================================

\timing on
SET search_path TO mvcc_demo, public;

-- ============================================================================
-- SECTION A: System Columns - The Hidden Tuple Metadata
-- ============================================================================
-- Every PostgreSQL row has hidden system columns that track versioning:
--
--   xmin: Transaction ID that created this row version (INSERT or UPDATE)
--   xmax: Transaction ID that deleted/updated this row (0 if still valid)
--   ctid: Physical location of the tuple (page_number, tuple_index)
--
-- These columns are the backbone of MVCC visibility checks.

-- Let's look at a few rows with their system columns
SELECT 
    ctid,           -- Physical location: (page, offset)
    xmin,           -- Transaction that created this version
    xmax,           -- Transaction that deleted this version (0 = active)
    id, 
    title,
    rating
FROM mvcc_demo.movies
-- WHERE id <= 5
-- ORDER BY id;
WHERE (ctid::text::point)[0]::bigint IN (0,1);

-- Interpretation:
-- - ctid like (0,1) means page 0, tuple slot 1
-- - xmin shows the transaction ID of the INSERT
-- - xmax = 0 means the row hasn't been deleted or updated yet

-- ============================================================================
-- SECTION B: Snapshot Isolation - Seeing a Consistent Point-in-Time View
-- ============================================================================
-- In MVCC, each transaction gets a "snapshot" of which transactions are
-- visible. This allows concurrent reads and writes without blocking.
--
-- *** THIS REQUIRES TWO SEPARATE DATABASE SESSIONS ***
-- Open two psql terminals connected to the same database.

-- Let's first see the current state of a row we'll modify:
SELECT id, title, rating, xmin, xmax 
FROM mvcc_demo.movies 
WHERE id = 1;

-- ----------------------------------------------------------------------------
-- SESSION 1: Start a transaction and hold it open
-- ----------------------------------------------------------------------------
-- Run these commands in your FIRST terminal:

-- /*
BEGIN;

-- Check current transaction ID
SELECT txid_current();

-- Read the row - note the xmin value
SELECT id, title, rating, xmin, xmax 
FROM mvcc_demo.movies 
WHERE id = 1;

-- Don't commit yet! Leave this transaction open and switch to Session 2.
*/

-- ----------------------------------------------------------------------------
-- SESSION 2: Modify the row while Session 1 is still open
-- ----------------------------------------------------------------------------
-- Run these commands in your SECOND terminal:

-- /*
SET search_path TO mvcc_demo, public;

BEGIN;
SELECT txid_current();  -- Note: This will be higher than Session 1's txid

-- Update the rating
UPDATE mvcc_demo.movies 
SET rating = 9.9, updated_at = NOW()
WHERE id = 1;

-- The update created a NEW version of the row
-- The old version still exists for Session 1 to see!

COMMIT;

-- Verify the update is visible in Session 2
SELECT id, title, rating, xmin, xmax 
FROM mvcc_demo.movies 
WHERE id = 1;
*/

-- ----------------------------------------------------------------------------
-- SESSION 1: See snapshot isolation in action
-- ----------------------------------------------------------------------------
-- Go back to your FIRST terminal (still in the BEGIN transaction):

-- /*
-- Even though Session 2 committed, Session 1 still sees the OLD value!
-- This is because Session 1's snapshot was taken before Session 2 committed.
SELECT id, title, rating, xmin, xmax 
FROM mvcc_demo.movies 
WHERE id = 1;

-- Now commit Session 1
COMMIT;

-- After commit, a NEW query will see the updated value
SELECT id, title, rating, xmin, xmax 
FROM mvcc_demo.movies 
WHERE id = 1;
*/

-- Key Insight:
-- Session 1's snapshot was "frozen" at BEGIN time. It can only see
-- transactions that committed BEFORE its snapshot was taken.

-- ============================================================================
-- SECTION C: Version Chains via ctid
-- ============================================================================
-- When a row is updated, PostgreSQL creates a NEW tuple and links the old
-- one to the new one via ctid. This forms a "version chain."
--
-- PostgreSQL uses O2N (Oldest-to-Newest) ordering:
-- Old Version -> points to -> New Version
--
-- Most other databases (Oracle, MySQL) use N2O ordering where the newest
-- version is found first and points backward to older versions.

-- Let's trace a version chain by updating a row multiple times

-- First, record the current state
SELECT 'Before updates' AS stage, ctid, xmin, xmax, id, rating
FROM mvcc_demo.movies WHERE id = 2;

-- Update #1
UPDATE mvcc_demo.movies SET rating = 5.0 WHERE id = 2;

-- Check - the ctid may have changed (new physical location)
SELECT 'After update 1' AS stage, ctid, xmin, xmax, id, rating
FROM mvcc_demo.movies WHERE id = 2;

-- Update #2
UPDATE mvcc_demo.movies SET rating = 6.0 WHERE id = 2;

SELECT 'After update 2' AS stage, ctid, xmin, xmax, id, rating
FROM mvcc_demo.movies WHERE id = 2;

-- Update #3
UPDATE mvcc_demo.movies SET rating = 7.0 WHERE id = 2;

SELECT 'After update 3' AS stage, ctid, xmin, xmax, id, rating
FROM mvcc_demo.movies WHERE id = 2;

-- The old versions (rating 5.0, 6.0, original) still exist as "dead tuples"
-- They'll be cleaned up by VACUUM later.

-- Check dead tuple count after our updates
SELECT 
    relname,
    n_live_tup,
    n_dead_tup,
    n_tup_upd AS total_updates
FROM pg_stat_user_tables
WHERE schemaname = 'mvcc_demo' AND relname = 'movies';

-- ============================================================================
-- SECTION D: HOT Updates (Heap-Only Tuples)
-- ============================================================================
-- HOT is an optimization that avoids index updates when:
--   1. The updated column is NOT in any index
--   2. The new tuple fits on the SAME PAGE as the old tuple
--
-- When HOT applies, the new tuple is placed on the same page and the
-- indexes don't need to be modified - they still point to the old ctid,
-- which chains to the new version.

-- Reset statistics to see HOT updates clearly
SELECT pg_stat_reset();

-- Perform updates on a NON-indexed column (updated_at is not indexed)
UPDATE mvcc_demo.movies 
SET updated_at = NOW() 
WHERE id BETWEEN 100 AND 199;  -- 100 rows

-- Check HOT update statistics
SELECT 
    relname,
    n_tup_upd AS total_updates,
    n_tup_hot_upd AS hot_updates,
    round(100.0 * n_tup_hot_upd / NULLIF(n_tup_upd, 0), 2) AS hot_pct
FROM pg_stat_user_tables
WHERE schemaname = 'mvcc_demo' AND relname = 'movies';

-- Now update an INDEXED column (rating has an index)
UPDATE mvcc_demo.movies 
SET rating = rating + 0.1 
WHERE id BETWEEN 200 AND 299;  -- 100 rows

-- Check again - HOT percentage should be lower
SELECT 
    relname,
    n_tup_upd AS total_updates,
    n_tup_hot_upd AS hot_updates,
    round(100.0 * n_tup_hot_upd / NULLIF(n_tup_upd, 0), 2) AS hot_pct
FROM pg_stat_user_tables
WHERE schemaname = 'mvcc_demo' AND relname = 'movies';

-- Key Insight:
-- When you update an indexed column, PostgreSQL must update ALL indexes
-- on that table because the physical tuple location changed and the old
-- index entries are now stale.

-- ============================================================================
-- SECTION E: How Visibility Checks Work
-- ============================================================================
-- When PostgreSQL reads a tuple, it performs a visibility check:
--
-- 1. If xmin is from an aborted transaction -> tuple is invisible
-- 2. If xmin is from a transaction not yet committed (from viewer's snapshot)
--    -> tuple is invisible
-- 3. If xmax is 0 -> tuple is visible (not deleted)
-- 4. If xmax is from a committed transaction in viewer's snapshot
--    -> tuple is invisible (it was deleted/updated)
--
-- This check happens for EVERY tuple during a sequential scan, which is
-- one reason table bloat hurts performance - you're checking dead tuples too.

-- We can see committed transactions via clog (commit log)
-- PostgreSQL tracks transaction status in pg_xact (commit log)

-- Show the current transaction ID (useful for understanding xmin/xmax)
SELECT txid_current();

-- Show the oldest transaction ID that might still be running
-- This is the "horizon" - tuples older than this with xmax set can be vacuumed
SELECT 
    datname,
    age(datfrozenxid) AS frozen_xid_age,
    datfrozenxid
FROM pg_database
WHERE datname = current_database();

-- ============================================================================
-- Summary: What We Learned
-- ============================================================================
-- 1. Every tuple has hidden xmin, xmax, ctid columns for version tracking
-- 2. xmin = transaction that created the tuple, xmax = transaction that deleted it
-- 3. Transactions see a "snapshot" - they can't see uncommitted or newer changes
-- 4. Updates create NEW tuples, old ones become "dead" until vacuumed
-- 5. HOT updates are an optimization that avoids index updates
-- 6. PostgreSQL uses O2N (oldest-to-newest) version chains
--
-- Next: 03_version_copying.sql - See the cost of full-tuple copying
-- ============================================================================
