-- ============================================================================
-- PostgreSQL MVCC Demonstration - Part 1: Setup
-- ============================================================================
-- This script creates the demo environment for exploring MVCC behavior.
-- Target: PostgreSQL 14 on a modest local machine
-- Data: 500,000 rows - enough to show measurable bloat without long waits
-- ============================================================================

-- Enable timing to see how long operations take
\timing on

-- ============================================================================
-- STEP 1: Create Schema
-- ============================================================================
-- Using a dedicated schema keeps demo objects organized and easy to clean up

DROP SCHEMA IF EXISTS mvcc_demo CASCADE;
CREATE SCHEMA mvcc_demo;
SET search_path TO mvcc_demo, public;

-- ============================================================================
-- STEP 2: Install Required Extensions
-- ============================================================================
-- pgstattuple: Provides detailed tuple-level statistics for bloat analysis
-- Note: Requires superuser privileges to create

CREATE EXTENSION IF NOT EXISTS pgstattuple;

-- ============================================================================
-- STEP 3: Create Demo Table
-- ============================================================================
-- We use a "movies" table with ~12 columns to demonstrate version copying.
-- When PostgreSQL updates a row, it copies ALL columns, not just the changed one.
-- More columns = more wasted space per update.

CREATE TABLE mvcc_demo.movies (
    id              SERIAL PRIMARY KEY,
    title           VARCHAR(200) NOT NULL,
    description     TEXT,
    genre           VARCHAR(50),
    director        VARCHAR(100),
    release_year    INTEGER,
    rating          NUMERIC(3,1),
    budget          BIGINT,
    revenue         BIGINT,
    runtime_minutes INTEGER,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
) WITH (fillfactor = 80);
-- Set fillfactor to 80% to leave room for HOT updates
-- HOT requires free space on the SAME PAGE as the old tuple
-- Default fillfactor is 100% (fully packed), which prevents HOT!

-- ============================================================================
-- STEP 4: Create Secondary Indexes
-- ============================================================================
-- PostgreSQL stores physical tuple addresses in ALL indexes.
-- When a non-HOT update occurs, EVERY index must be updated.
-- More indexes = more write amplification.

CREATE INDEX idx_movies_genre ON mvcc_demo.movies(genre);
CREATE INDEX idx_movies_year ON mvcc_demo.movies(release_year);
CREATE INDEX idx_movies_rating ON mvcc_demo.movies(rating);

-- ============================================================================
-- STEP 5: Insert 500,000 Rows of Dummy Data
-- ============================================================================
-- Using generate_series with random data generation.
-- This takes ~30-60 seconds on a modest machine.

INSERT INTO mvcc_demo.movies (
    title,
    description,
    genre,
    director,
    release_year,
    rating,
    budget,
    revenue,
    runtime_minutes,
    created_at,
    updated_at
)
SELECT
    -- Generate random-ish title using md5 hash
    'Movie ' || gs || ': ' || substr(md5(random()::text), 1, 10) AS title,
    
    -- Generate longer description text
    repeat(md5(random()::text), 3) AS description,
    
    -- Pick from a set of genres (for realistic index cardinality)
    (ARRAY['Action', 'Comedy', 'Drama', 'Horror', 'Sci-Fi', 
           'Romance', 'Thriller', 'Documentary', 'Animation', 'Adventure'])
        [1 + floor(random() * 10)::int] AS genre,
    
    -- Generate random director name
    'Director ' || substr(md5(random()::text), 1, 8) AS director,
    
    -- Years between 1950 and 2025
    1950 + floor(random() * 76)::int AS release_year,
    
    -- Rating between 1.0 and 10.0
    round((1 + random() * 9)::numeric, 1) AS rating,
    
    -- Budget: $100K to $300M
    (100000 + floor(random() * 300000000))::bigint AS budget,
    
    -- Revenue: $0 to $2B
    floor(random() * 2000000000)::bigint AS revenue,
    
    -- Runtime: 60 to 240 minutes
    60 + floor(random() * 181)::int AS runtime_minutes,
    
    -- Created at: random time in past 10 years
    NOW() - (random() * interval '3650 days') AS created_at,
    
    -- Updated at: same as created for now
    NOW() - (random() * interval '365 days') AS updated_at
    
FROM generate_series(1, 500000) AS gs;

-- ============================================================================
-- STEP 6: Gather Statistics
-- ============================================================================
-- ANALYZE updates the query planner's statistics about the table.
-- This ensures accurate row count estimates in our diagnostic queries.

ANALYZE mvcc_demo.movies;

-- ============================================================================
-- STEP 7: Verify Setup
-- ============================================================================

SELECT 'Setup Complete!' AS status;

-- Show row count
SELECT count(*) AS total_rows FROM mvcc_demo.movies;

-- Show table size
SELECT 
    pg_size_pretty(pg_relation_size('mvcc_demo.movies')) AS table_size,
    pg_size_pretty(pg_indexes_size('mvcc_demo.movies')) AS indexes_size,
    pg_size_pretty(pg_total_relation_size('mvcc_demo.movies')) AS total_size;

-- Show index sizes individually
SELECT 
    indexrelname AS index_name,
    pg_size_pretty(pg_relation_size(indexrelid)) AS size
FROM pg_stat_user_indexes
WHERE schemaname = 'mvcc_demo' AND relname = 'movies'
ORDER BY pg_relation_size(indexrelid) DESC;

-- Show current tuple statistics (should be 0 dead tuples after fresh insert)
SELECT 
    relname AS table_name,
    n_live_tup AS live_tuples,
    n_dead_tup AS dead_tuples,
    round(100.0 * n_dead_tup / NULLIF(n_live_tup + n_dead_tup, 0), 2) AS dead_pct
FROM pg_stat_user_tables
WHERE schemaname = 'mvcc_demo';

-- ============================================================================
-- Setup complete! Proceed to 02_mvcc_basics.sql to learn how MVCC works.
-- ============================================================================
