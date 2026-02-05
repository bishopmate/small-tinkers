-- ============================================================================
-- PostgreSQL MVCC Demonstration - Part 7: Diagnostic Queries
-- ============================================================================
-- This script contains production-ready queries for monitoring MVCC health.
-- Use these to detect problems before they become critical.
-- ============================================================================

SET search_path TO mvcc_demo, public;

-- ============================================================================
-- QUERY 1: Dead Tuple Ratio by Table
-- ============================================================================
-- Identifies tables with high dead tuple counts.
-- Action: Tables with >10% dead tuples may need manual VACUUM or tuning.

SELECT 
    schemaname || '.' || relname AS table_name,
    pg_size_pretty(pg_total_relation_size(schemaname || '.' || relname)) AS total_size,
    n_live_tup AS live_tuples,
    n_dead_tup AS dead_tuples,
    CASE 
        WHEN n_live_tup + n_dead_tup = 0 THEN 0
        ELSE round(100.0 * n_dead_tup / (n_live_tup + n_dead_tup), 2)
    END AS dead_tuple_pct,
    CASE
        WHEN n_dead_tup > n_live_tup THEN '🔴 CRITICAL'
        WHEN 100.0 * n_dead_tup / NULLIF(n_live_tup + n_dead_tup, 0) > 20 THEN '🟠 WARNING'
        WHEN 100.0 * n_dead_tup / NULLIF(n_live_tup + n_dead_tup, 0) > 10 THEN '🟡 ATTENTION'
        ELSE '🟢 OK'
    END AS status
FROM pg_stat_user_tables
WHERE n_live_tup > 0
ORDER BY n_dead_tup DESC
LIMIT 20;

-- ============================================================================
-- QUERY 2: Table Bloat Estimation
-- ============================================================================
-- Estimates how much space is wasted due to bloat.
-- Note: This is an approximation. For precise data, use pgstattuple.

WITH table_stats AS (
    SELECT
        schemaname,
        relname,
        pg_relation_size(schemaname || '.' || relname) AS table_bytes,
        n_live_tup,
        n_dead_tup
    FROM pg_stat_user_tables
    WHERE n_live_tup > 0
),
estimated_live AS (
    SELECT
        schemaname,
        relname,
        table_bytes,
        n_live_tup,
        n_dead_tup,
        -- Estimate bytes per live tuple (rough approximation)
        CASE 
            WHEN n_live_tup > 0 
            THEN (table_bytes::float / (n_live_tup + n_dead_tup + 1))::bigint
            ELSE 0 
        END AS bytes_per_tuple
    FROM table_stats
)
SELECT
    schemaname || '.' || relname AS table_name,
    pg_size_pretty(table_bytes) AS current_size,
    pg_size_pretty((n_live_tup * bytes_per_tuple)::bigint) AS estimated_live_size,
    pg_size_pretty(table_bytes - (n_live_tup * bytes_per_tuple)::bigint) AS estimated_bloat,
    round(100.0 * (table_bytes - n_live_tup * bytes_per_tuple) / NULLIF(table_bytes, 0), 2) AS bloat_pct
FROM estimated_live
ORDER BY table_bytes - n_live_tup * bytes_per_tuple DESC
LIMIT 20;

-- ============================================================================
-- QUERY 3: Index Bloat Analysis (requires pgstattuple)
-- ============================================================================
-- Analyzes B-tree indexes for bloat. Low avg_leaf_density = bloated.

SELECT 
    schemaname || '.' || indexrelname AS index_name,
    pg_size_pretty(pg_relation_size(indexrelid)) AS size,
    s.leaf_pages,
    s.deleted_pages,
    round(s.avg_leaf_density, 2) AS leaf_fill_pct,
    s.leaf_fragmentation AS fragmentation,
    CASE
        WHEN s.avg_leaf_density < 50 THEN '🔴 Heavily bloated - REINDEX recommended'
        WHEN s.avg_leaf_density < 70 THEN '🟠 Moderately bloated'
        WHEN s.avg_leaf_density < 85 THEN '🟡 Slight bloat'
        ELSE '🟢 Healthy'
    END AS status
FROM pg_stat_user_indexes i
CROSS JOIN LATERAL pgstatindex(i.indexrelid) s
WHERE pg_relation_size(indexrelid) > 1024 * 1024  -- Only indexes > 1MB
ORDER BY s.avg_leaf_density ASC
LIMIT 20;

-- ============================================================================
-- QUERY 4: Vacuum Activity History
-- ============================================================================
-- Shows when tables were last vacuumed and their current state.

SELECT 
    schemaname || '.' || relname AS table_name,
    last_vacuum,
    last_autovacuum,
    vacuum_count,
    autovacuum_count,
    n_dead_tup AS current_dead_tuples,
    age(now(), COALESCE(last_autovacuum, last_vacuum)) AS time_since_vacuum,
    CASE
        WHEN last_vacuum IS NULL AND last_autovacuum IS NULL THEN '🔴 Never vacuumed!'
        WHEN age(now(), COALESCE(last_autovacuum, last_vacuum)) > interval '1 day' 
             AND n_dead_tup > 10000 THEN '🟠 Stale - needs vacuum'
        ELSE '🟢 OK'
    END AS status
FROM pg_stat_user_tables
ORDER BY 
    COALESCE(last_autovacuum, last_vacuum) ASC NULLS FIRST,
    n_dead_tup DESC
LIMIT 20;

-- ============================================================================
-- QUERY 5: Long-Running Transactions (Vacuum Blockers)
-- ============================================================================
-- Identifies transactions that may be blocking vacuum.
-- Action: Investigate and consider terminating if safe.

SELECT 
    pid,
    usename,
    application_name,
    client_addr,
    state,
    backend_xmin,
    age(backend_xmin) AS xmin_age,
    xact_start,
    age(clock_timestamp(), xact_start) AS transaction_duration,
    age(clock_timestamp(), query_start) AS current_query_duration,
    wait_event_type,
    wait_event,
    left(query, 80) AS query_preview,
    CASE
        WHEN age(clock_timestamp(), xact_start) > interval '1 hour' THEN '🔴 CRITICAL - May block vacuum'
        WHEN age(clock_timestamp(), xact_start) > interval '10 minutes' THEN '🟠 WARNING'
        WHEN age(clock_timestamp(), xact_start) > interval '5 minutes' THEN '🟡 ATTENTION'
        ELSE '🟢 OK'
    END AS status
FROM pg_stat_activity
WHERE state != 'idle'
  AND xact_start IS NOT NULL
  AND pid != pg_backend_pid()
ORDER BY xact_start ASC
LIMIT 20;

-- ============================================================================
-- QUERY 6: HOT Update Efficiency
-- ============================================================================
-- Shows how effectively HOT updates are being used.
-- Low HOT ratio = excessive index bloat due to updates on indexed columns.

SELECT 
    schemaname || '.' || relname AS table_name,
    n_tup_upd AS total_updates,
    n_tup_hot_upd AS hot_updates,
    n_tup_upd - n_tup_hot_upd AS non_hot_updates,
    CASE WHEN n_tup_upd > 0 
        THEN round(100.0 * n_tup_hot_upd / n_tup_upd, 2)
        ELSE 0 
    END AS hot_update_pct,
    CASE
        WHEN n_tup_upd = 0 THEN '⚪ No updates'
        WHEN 100.0 * n_tup_hot_upd / n_tup_upd >= 80 THEN '🟢 Excellent HOT ratio'
        WHEN 100.0 * n_tup_hot_upd / n_tup_upd >= 50 THEN '🟡 Moderate - review indexes'
        ELSE '🔴 Low - too many index updates'
    END AS status
FROM pg_stat_user_tables
WHERE n_tup_upd > 100  -- Only tables with significant updates
ORDER BY n_tup_upd DESC
LIMIT 20;

-- ============================================================================
-- QUERY 7: Transaction ID Age (Wraparound Prevention)
-- ============================================================================
-- Monitors transaction ID age to prevent wraparound.
-- Action: If age > 500M, aggressive vacuuming is needed!

-- Database level
SELECT 
    datname AS database_name,
    age(datfrozenxid) AS age,
    round(100.0 * age(datfrozenxid) / 2147483647, 2) AS pct_to_wraparound,
    2147483647 - age(datfrozenxid) AS remaining_xids,
    CASE
        WHEN age(datfrozenxid) > 1000000000 THEN '🔴 CRITICAL - Emergency vacuum needed!'
        WHEN age(datfrozenxid) > 500000000 THEN '🟠 WARNING - Plan aggressive vacuum'
        WHEN age(datfrozenxid) > 200000000 THEN '🟡 ATTENTION - Monitor closely'
        ELSE '🟢 Healthy'
    END AS status
FROM pg_database
ORDER BY age(datfrozenxid) DESC;

-- Table level
SELECT 
    schemaname || '.' || relname AS table_name,
    age(relfrozenxid) AS table_age,
    pg_size_pretty(pg_relation_size(schemaname || '.' || relname)) AS size,
    CASE
        WHEN age(relfrozenxid) > 500000000 THEN '🔴 CRITICAL'
        WHEN age(relfrozenxid) > 200000000 THEN '🟠 WARNING'
        ELSE '🟢 OK'
    END AS status
FROM pg_stat_user_tables
ORDER BY age(relfrozenxid) DESC
LIMIT 20;

-- ============================================================================
-- QUERY 8: Currently Running Vacuum/Analyze
-- ============================================================================
-- Shows progress of any running vacuum or analyze operations.

SELECT 
    p.pid,
    a.usename,
    p.datname AS database,
    p.relid::regclass AS table_name,
    p.phase,
    p.heap_blks_total AS total_blocks,
    p.heap_blks_scanned AS scanned_blocks,
    CASE WHEN p.heap_blks_total > 0 
        THEN round(100.0 * p.heap_blks_scanned / p.heap_blks_total, 2)
        ELSE 0 
    END AS pct_complete,
    p.heap_blks_vacuumed AS vacuumed_blocks,
    p.index_vacuum_count AS index_passes,
    p.max_dead_tuples,
    p.num_dead_tuples AS dead_found,
    a.query_start,
    age(clock_timestamp(), a.query_start) AS duration
FROM pg_stat_progress_vacuum p
JOIN pg_stat_activity a ON a.pid = p.pid;

-- ============================================================================
-- QUERY 9: Autovacuum Settings Per Table
-- ============================================================================
-- Shows tables with custom autovacuum settings.

SELECT 
    c.relnamespace::regnamespace || '.' || c.relname AS table_name,
    pg_size_pretty(pg_relation_size(c.oid)) AS size,
    c.reloptions AS custom_settings,
    s.n_dead_tup AS current_dead_tuples,
    s.last_autovacuum
FROM pg_class c
JOIN pg_stat_user_tables s 
    ON s.relid = c.oid
WHERE c.reloptions IS NOT NULL
  AND c.relkind = 'r'
ORDER BY pg_relation_size(c.oid) DESC;

-- ============================================================================
-- QUERY 10: Tables Approaching Autovacuum Threshold
-- ============================================================================
-- Shows tables that are close to triggering autovacuum.

WITH thresholds AS (
    SELECT 
        schemaname,
        relname,
        n_live_tup,
        n_dead_tup,
        -- Get table-specific or default settings
        COALESCE(
            (SELECT option_value::float 
             FROM pg_options_to_table(c.reloptions) 
             WHERE option_name = 'autovacuum_vacuum_scale_factor'),
            current_setting('autovacuum_vacuum_scale_factor')::float
        ) AS scale_factor,
        COALESCE(
            (SELECT option_value::integer 
             FROM pg_options_to_table(c.reloptions) 
             WHERE option_name = 'autovacuum_vacuum_threshold'),
            current_setting('autovacuum_vacuum_threshold')::integer
        ) AS threshold
    FROM pg_stat_user_tables s
    JOIN pg_class c ON c.oid = s.relid
    WHERE n_live_tup > 0
)
SELECT 
    schemaname || '.' || relname AS table_name,
    n_dead_tup AS dead_tuples,
    (threshold + (scale_factor * n_live_tup))::bigint AS vacuum_threshold,
    round(100.0 * n_dead_tup / NULLIF(threshold + scale_factor * n_live_tup, 0), 2) AS pct_to_vacuum,
    CASE
        WHEN n_dead_tup >= threshold + scale_factor * n_live_tup THEN '🔴 Should be vacuuming now!'
        WHEN n_dead_tup >= 0.8 * (threshold + scale_factor * n_live_tup) THEN '🟠 Approaching threshold'
        WHEN n_dead_tup >= 0.5 * (threshold + scale_factor * n_live_tup) THEN '🟡 Halfway to threshold'
        ELSE '🟢 OK'
    END AS status
FROM thresholds
ORDER BY n_dead_tup DESC
LIMIT 20;

-- ============================================================================
-- QUERY 11: Comprehensive Table Health Summary
-- ============================================================================
-- A single query to assess overall table health.

SELECT 
    schemaname || '.' || relname AS table_name,
    pg_size_pretty(pg_total_relation_size(schemaname || '.' || relname)) AS total_size,
    n_live_tup AS live,
    n_dead_tup AS dead,
    round(100.0 * n_dead_tup / NULLIF(n_live_tup + n_dead_tup, 0), 2) AS dead_pct,
    round(100.0 * n_tup_hot_upd / NULLIF(n_tup_upd, 0), 2) AS hot_pct,
    last_autovacuum,
    age(relfrozenxid) AS table_age,
    CASE
        -- Critical conditions
        WHEN n_dead_tup > n_live_tup THEN '🔴 CRITICAL: More dead than live tuples'
        WHEN age(relfrozenxid) > 500000000 THEN '🔴 CRITICAL: XID age too high'
        -- Warning conditions  
        WHEN 100.0 * n_dead_tup / NULLIF(n_live_tup, 0) > 30 THEN '🟠 WARNING: High dead tuple ratio'
        WHEN last_autovacuum IS NULL THEN '🟠 WARNING: Never vacuumed'
        WHEN age(now(), last_autovacuum) > interval '7 days' 
             AND n_dead_tup > 10000 THEN '🟠 WARNING: Vacuum overdue'
        -- Attention conditions
        WHEN 100.0 * n_tup_hot_upd / NULLIF(n_tup_upd, 0) < 30 
             AND n_tup_upd > 1000 THEN '🟡 ATTENTION: Low HOT update ratio'
        ELSE '🟢 OK'
    END AS health_status
FROM pg_stat_user_tables
WHERE n_live_tup > 0
ORDER BY n_dead_tup DESC
LIMIT 20;

-- ============================================================================
-- Summary: Production Monitoring Recommendations
-- ============================================================================
-- 1. Run Query 1 (Dead Tuple Ratio) daily
-- 2. Run Query 5 (Long-Running Transactions) every 5 minutes
-- 3. Run Query 7 (XID Age) weekly
-- 4. Run Query 6 (HOT Ratio) after schema changes
-- 5. Set up alerts for:
--    - Dead tuple % > 20%
--    - Transactions running > 10 minutes
--    - XID age > 500 million
--    - Vacuum not running for > 1 day on busy tables
--
-- Tools for production:
--    - pg_stat_statements: Track slow queries
--    - auto_explain: Log execution plans
--    - pg_repack: Online table/index rebuild (no exclusive lock)
--    - pgBadger: Log analysis
-- ============================================================================
