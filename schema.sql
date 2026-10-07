CREATE DATABASE IF NOT EXISTS dank;

-----------------------
--- SCRAPING MODELS ---
-----------------------

-- Recently scraped feeds from sites
-- Used for avoiding loading feeds too much.
CREATE TABLE IF NOT EXISTS dank.site_feeds (
    domain LowCardinality(String),
    feed_url String,
    feed_type LowCardinality(String),
    scraped_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(scraped_at)
ORDER BY (domain, feed_url);

-- RawPost data with JSON/XML/HTML and other payloads
-- We only store minimal columns to find posts to process, and dump as much
-- data in `payload` as we can to post-process later.
--
-- Post-processing lets us quickly correct bugs without re-scraping.
CREATE TABLE IF NOT EXISTS dank.raw_posts (
    domain LowCardinality(String),
    post_id String,
    url String,
    post_created_at Nullable(DateTime64(3, 'UTC')),
    scraped_at DateTime64(3, 'UTC'),
    source LowCardinality(String),
    request_url String,
    payload String CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY (domain, toYYYYMM(scraped_at))
ORDER BY (domain, scraped_at, post_id);

-- RawAsset data with JSON/XML/HTML and other payloads
--
-- When scraping assets are saved to a local filesystem path on disk.
CREATE TABLE IF NOT EXISTS dank.raw_assets (
    domain LowCardinality(String),
    post_id String,
    url String,
    asset_type LowCardinality(String),
    scraped_at DateTime64(3, 'UTC'),
    source LowCardinality(String),
    local_path String
)
ENGINE = MergeTree
PARTITION BY (domain, toYYYYMM(scraped_at))
ORDER BY (domain, scraped_at, post_id, url);


-------------------------
--- PROCESSING MODELS ---
-------------------------

-- Processed posts with lots of information.
CREATE TABLE IF NOT EXISTS dank.posts (
    domain LowCardinality(String),
    post_id String,
    url String,
    author String,
    title String,
    html String,
    title_embedding Array(Float32),
    html_embedding Array(Float32),
    created_at DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC'),
    source LowCardinality(String)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (domain, post_id);

-- Processed assets with lots of information.
CREATE TABLE IF NOT EXISTS dank.assets (
    domain LowCardinality(String),
    post_id String,
    url String,
    local_path String,
    content_type LowCardinality(String),
    size_bytes UInt64,
    created_at DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC'),
    source LowCardinality(String)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (domain, post_id, url);


-----------------------
--- WEB VIEW MODELS ---
-----------------------

-- Caching for embeddings used for search in the web app view
CREATE TABLE IF NOT EXISTS dank.web_embedding_cache (
    model_name LowCardinality(String),
    search_text String,
    embedding Array(Float32),
    created_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (model_name, search_text)
TTL toDateTime(created_at) + INTERVAL 6 HOUR DELETE;

-- Scrape history; kept in sync with src/dank/scrape/history.sql.
CREATE TABLE IF NOT EXISTS dank.scrape_runs (
    run_id UUID,
    started_at DateTime64(3, 'UTC'),
    finished_at Nullable(DateTime64(3, 'UTC')),
    elapsed_ms Nullable(UInt64),
    status LowCardinality(String),
    error_type LowCardinality(String),
    sources_selected UInt32,
    sources_started UInt32,
    runtime LowCardinality(String),
    platform LowCardinality(String),
    python_version LowCardinality(String),
    cpu_limit Float64,
    memory_limit_bytes Nullable(UInt64),
    code_version LowCardinality(String),
    source_concurrency UInt32,
    rss_concurrency UInt32,
    media_concurrency UInt32,
    http_concurrency UInt32 DEFAULT 0,
    http_per_host UInt32 DEFAULT 0,
    queue_batches UInt32 DEFAULT 0,
    batch_size UInt32,
    headless UInt8,
    keep_feed_on_fetch_failure UInt8,
    max_entries_per_feed UInt64 DEFAULT 0,
    max_asset_bytes Nullable(Int64),
    feed_staleness_days Int64,
    x_max_posts Int64,
    x_max_scrolls Int64,
    x_scroll_pause_seconds Float64,
    version UInt64,
    posts_queued UInt64,
    posts_saved UInt64,
    media_references UInt64,
    asset_records_saved UInt64,
    files_downloaded UInt64,
    files_cached UInt64,
    files_failed UInt64,
    media_skipped UInt64,
    downloaded_file_bytes UInt64,
    http_requests UInt64,
    http_429 UInt64,
    retries UInt64,
    fetch_failures UInt64,
    parse_failures UInt64,
    request_ms UInt64,
    retry_wait_ms UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (started_at, run_id);

CREATE TABLE IF NOT EXISTS dank.scrape_source_runs (
    run_id UUID,
    source_index UInt32,
    domain LowCardinality(String),
    source LowCardinality(String),
    accounts Array(String),
    tags Array(String) DEFAULT [],
    feed_urls Array(String) DEFAULT [],
    started_at DateTime64(3, 'UTC'),
    finished_at Nullable(DateTime64(3, 'UTC')),
    elapsed_ms Nullable(UInt64),
    collection_ms Nullable(UInt64),
    status LowCardinality(String),
    error_type LowCardinality(String),
    version UInt64,
    posts_queued UInt64,
    posts_saved UInt64,
    media_references UInt64,
    asset_records_saved UInt64,
    files_downloaded UInt64,
    files_cached UInt64,
    files_failed UInt64,
    media_skipped UInt64,
    downloaded_file_bytes UInt64,
    http_requests UInt64,
    http_429 UInt64,
    retries UInt64,
    fetch_failures UInt64,
    parse_failures UInt64,
    request_ms UInt64,
    retry_wait_ms UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (domain, started_at, run_id, source_index);

-- Upgrade existing history tables without rewriting previous observations.
ALTER TABLE dank.scrape_runs
    ADD COLUMN IF NOT EXISTS max_entries_per_feed UInt64 DEFAULT 0;
ALTER TABLE dank.scrape_source_runs
    ADD COLUMN IF NOT EXISTS tags Array(String) DEFAULT [],
    ADD COLUMN IF NOT EXISTS feed_urls Array(String) DEFAULT [];

ALTER TABLE dank.scrape_runs
    ADD COLUMN IF NOT EXISTS http_concurrency UInt32 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS http_per_host UInt32 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS queue_batches UInt32 DEFAULT 0;
