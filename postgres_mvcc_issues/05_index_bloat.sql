-- ============================================================================
-- PostgreSQL MVCC Demonstration - Part 5: Index Bloat & Write Amplification
-- ============================================================================
-- PostgreSQL stores PHYSICAL tuple addresses (ctid) in indexes.
-- This means:
--
--   1. When a tuple moves (non-HOT update), ALL indexes must be updated
--   2. Each index entry points to a specific (page, offset) location
--   3. Dead index entries accumulate just like dead heap tuples
--
-- This is different from Oracle/MySQL where secondary indexes store
-- logical identifiers (primary key or rowid) that don't change on updates.
-- ============================================================================

\timing on
SET search_path TO mvcc_demo, public;

-- ============================================================================
-- STEP 1: Baseline - Current Index Sizes
-- ============================================================================
-- First, let's vacuum everything and see clean index sizes.


-- VACUUM FULL rewrites the table with new fillfactor, leaving 20% free per page
VACUUM FULL mvcc_demo.movies;
ANALYZE mvcc_demo.movies;

-- Show all indexes and their sizes
SELECT 
    indexrelname AS index_name,
    pg_size_pretty(pg_relation_size(indexrelid)) AS size,
    pg_relation_size(indexrelid) AS size_bytes,
    idx_scan AS times_used,
    idx_tup_read AS tuples_read
FROM pg_stat_user_indexes
WHERE schemaname = 'mvcc_demo' AND relname = 'movies'
ORDER BY pg_relation_size(indexrelid) DESC;

-- Save baseline sizes
CREATE TEMP TABLE index_baseline AS
SELECT 
    indexrelname AS index_name,
    pg_relation_size(indexrelid) AS baseline_bytes
FROM pg_stat_user_indexes
WHERE schemaname = 'mvcc_demo' AND relname = 'movies';

SELECT * FROM index_baseline;

-- ============================================================================
-- STEP 2: HOT Updates - Index-Friendly Path
-- ============================================================================
-- HOT (Heap-Only Tuple) updates avoid index updates when:
--   1. No indexed columns are modified
--   2. New tuple fits on the same heap page
--
-- Let's update a non-indexed column (updated_at) and verify indexes don't grow.

-- Reset statistics
SELECT pg_stat_reset();

-- Get current sizes
SELECT 'BEFORE non-indexed update' AS state,
    indexrelname, pg_relation_size(indexrelid) AS bytes
FROM pg_stat_user_indexes
WHERE schemaname = 'mvcc_demo' AND relname = 'movies';

-- Update NON-indexed column on 100K rows
UPDATE mvcc_demo.movies 
SET updated_at = NOW()
WHERE id <= 100000;

-- Check if indexes grew
SELECT 'AFTER non-indexed update' AS state,
    indexrelname, pg_relation_size(indexrelid) AS bytes
FROM pg_stat_user_indexes
WHERE schemaname = 'mvcc_demo' AND relname = 'movies';

-- Check HOT update ratio (should be high!)
SELECT 
    relname,
    n_tup_upd AS total_updates,
    n_tup_hot_upd AS hot_updates,
    round(100.0 * n_tup_hot_upd / NULLIF(n_tup_upd, 0), 2) AS hot_ratio_pct
FROM pg_stat_user_tables
WHERE schemaname = 'mvcc_demo' AND relname = 'movies';

-- Result: Indexes should NOT have grown significantly because HOT was used!

-- ============================================================================
-- STEP 3: Non-HOT Updates - Index Explosion
-- ============================================================================
-- Now let's update an INDEXED column. Every index must be updated!
--
-- Our table has indexes on: genre, release_year, rating
-- Updating `rating` will:
--   1. Create a new heap tuple (full copy)
--   2. Insert a new entry in idx_movies_rating
--   3. Insert new entries in idx_movies_genre (still points to new ctid!)
--   4. Insert new entries in idx_movies_year (still points to new ctid!)
--   5. Mark old entries as dead

-- Record sizes before
CREATE TEMP TABLE before_indexed_update AS
SELECT 
    indexrelname,
    pg_relation_size(indexrelid) AS bytes
FROM pg_stat_user_indexes
WHERE schemaname = 'mvcc_demo' AND relname = 'movies';

-- Update INDEXED column on 100K rows
UPDATE mvcc_demo.movies 
SET rating = rating + 0.1
WHERE id <= 100000;

-- Compare sizes after
SELECT 
    b.indexrelname,
    pg_size_pretty(b.bytes) AS before_size,
    pg_size_pretty(pg_relation_size(i.indexrelid)) AS after_size,
    pg_size_pretty(pg_relation_size(i.indexrelid) - b.bytes) AS growth,
    round(100.0 * (pg_relation_size(i.indexrelid) - b.bytes) / NULLIF(b.bytes, 0), 2) AS growth_pct
FROM before_indexed_update b
JOIN pg_stat_user_indexes i ON i.indexrelname = b.indexrelname
WHERE i.schemaname = 'mvcc_demo' AND i.relname = 'movies'
ORDER BY pg_relation_size(i.indexrelid) - b.bytes DESC;

-- ALL indexes grew, not just the one on 'rating'!

-- Check HOT ratio - should be LOW now
SELECT 
    relname,
    n_tup_upd AS total_updates,
    n_tup_hot_upd AS hot_updates,
    n_tup_upd - n_tup_hot_upd AS non_hot_updates,
    round(100.0 * n_tup_hot_upd / NULLIF(n_tup_upd, 0), 2) AS hot_ratio_pct
FROM pg_stat_user_tables
WHERE schemaname = 'mvcc_demo' AND relname = 'movies';

-- ============================================================================
-- STEP 4: Analyze Index Bloat with pgstattuple
-- ============================================================================
-- pgstattuple can analyze index internal structure and find dead entries.

-- Analyze each index's bloat
SELECT 
    'idx_movies_pkey' AS index_name,
    pg_size_pretty(pg_relation_size('mvcc_demo.idx_movies_genre')) AS size,
    * 
FROM pgstatindex('mvcc_demo.movies_pkey');

-- Check all indexes with a more readable format
SELECT 
    i.indexrelname AS index_name,
    pg_size_pretty(pg_relation_size(i.indexrelid)) AS size,
    s.leaf_pages,
    s.deleted_pages,
    round(s.avg_leaf_density::numeric, 2) AS avg_leaf_density_pct,
    s.leaf_fragmentation
FROM pg_stat_user_indexes i
CROSS JOIN LATERAL pgstatindex(i.indexrelid) s
WHERE i.schemaname = 'mvcc_demo' AND i.relname = 'movies'
ORDER BY pg_relation_size(i.indexrelid) DESC;

-- Interpretation:
-- avg_leaf_density: How full are the leaf pages (100% = no waste)
--   - After updates, this drops because pages have dead entries mixed with live ones
--   - Values below 70% indicate significant bloat
--   - This is the BEST indicator of index bloat
--
-- deleted_pages: Pages where ALL entries are dead (ready for recycling)
--   - Usually 0 because pages have a MIX of live and dead entries
--   - Only becomes non-zero after VACUUM marks fully-empty pages
--   - Don't rely on this metric for bloat detection
--
-- leaf_fragmentation: How out-of-order the physical pages are vs logical order

-- ============================================================================
-- STEP 5: The Uber Problem - Write Amplification
-- ============================================================================
-- In 2016, Uber migrated from PostgreSQL to MySQL primarily due to
-- write amplification from index updates.
--
-- Their tables had many secondary indexes. Each update touched ALL of them.
--
-- Let's simulate this with more updates on indexed columns.

-- Multiple indexed column updates
UPDATE mvcc_demo.movies SET rating = rating - 0.1 WHERE id <= 50000;
UPDATE mvcc_demo.movies SET genre = 'Updated' WHERE id <= 50000;
UPDATE mvcc_demo.movies SET release_year = release_year + 1 WHERE id <= 50000;

-- Check index growth
SELECT 
    i.indexrelname,
    pg_size_pretty(b.baseline_bytes) AS original_size,
    pg_size_pretty(pg_relation_size(i.indexrelid)) AS current_size,
    round(100.0 * (pg_relation_size(i.indexrelid) - b.baseline_bytes) / NULLIF(b.baseline_bytes, 0), 2) AS bloat_pct
FROM index_baseline b
JOIN pg_stat_user_indexes i ON i.indexrelname = b.index_name
WHERE i.schemaname = 'mvcc_demo' AND i.relname = 'movies';

-- ============================================================================
-- STEP 6: VACUUM's Limited Help with Indexes
-- ============================================================================
-- Regular VACUUM removes dead HEAP tuples but index cleanup is less aggressive.
-- Dead index entries pointing to vacuumed tuples become "recyclable" but the
-- index doesn't necessarily shrink.

-- Before vacuum
SELECT 'BEFORE VACUUM' AS state, 
    indexrelname, pg_size_pretty(pg_relation_size(indexrelid)) AS size
FROM pg_stat_user_indexes
WHERE schemaname = 'mvcc_demo' AND relname = 'movies';

VACUUM mvcc_demo.movies;

-- After vacuum
SELECT 'AFTER VACUUM' AS state, 
    indexrelname, pg_size_pretty(pg_relation_size(indexrelid)) AS size
FROM pg_stat_user_indexes
WHERE schemaname = 'mvcc_demo' AND relname = 'movies';

-- Indexes often DON'T shrink much with regular VACUUM!

-- ============================================================================
-- STEP 7: REINDEX - Rebuild Indexes
-- ============================================================================
-- REINDEX rebuilds an index from scratch, eliminating bloat.
-- Like VACUUM FULL, it requires locks (though less severe in PG 12+).

-- REINDEX one index (note: in PG 12+, CONCURRENTLY option available)
REINDEX INDEX mvcc_demo.idx_movies_rating;

-- Check if it shrunk
SELECT 
    indexrelname, 
    pg_size_pretty(pg_relation_size(indexrelid)) AS size
FROM pg_stat_user_indexes
WHERE schemaname = 'mvcc_demo' AND relname = 'movies';

-- REINDEX all indexes on the table
REINDEX TABLE mvcc_demo.movies;

-- Final comparison
SELECT 
    i.indexrelname,
    pg_size_pretty(b.baseline_bytes) AS original_size,
    pg_size_pretty(pg_relation_size(i.indexrelid)) AS after_reindex
FROM index_baseline b
JOIN pg_stat_user_indexes i ON i.indexrelname = b.index_name
WHERE i.schemaname = 'mvcc_demo' AND i.relname = 'movies'
ORDER BY b.index_name;

-- ============================================================================
-- STEP 8: Oracle/MySQL Comparison (Conceptual)
-- ============================================================================
-- PostgreSQL index entry: (genre='Action') -> ctid (42, 15)
--                         Points to PHYSICAL location
--                         If tuple moves, entry is stale!
--
-- Oracle/MySQL secondary index: (genre='Action') -> primary_key (12345)
--                               Points to LOGICAL identifier
--                               Tuple can move without updating this index!
--
-- Result: Oracle/MySQL only update the clustering index on most updates.
--         PostgreSQL updates ALL indexes.
--
-- Trade-off: PostgreSQL avoids an extra lookup (direct to heap)
--            Oracle/MySQL require: secondary index -> primary index -> heap

-- ============================================================================
-- Summary: Index Bloat and Write Amplification
-- ============================================================================
-- 1. PostgreSQL indexes store physical tuple addresses (ctid)
-- 2. Non-HOT updates must update EVERY index on the table
-- 3. HOT updates (non-indexed columns, same page) skip index updates
-- 4. Only ~46% of updates are HOT on average (per Pavlo's analysis)
-- 5. VACUUM doesn't shrink indexes effectively
-- 6. REINDEX is needed but requires locks
-- 7. This was a major factor in Uber's PostgreSQL -> MySQL migration
--
-- Next: 06_vacuum_issues.sql - Why autovacuum can fail you
-- ============================================================================

-- Cleanup temp tables
DROP TABLE IF EXISTS index_baseline;
DROP TABLE IF EXISTS before_indexed_update;
