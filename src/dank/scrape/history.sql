CREATE TABLE IF NOT EXISTS scrape_runs (
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

CREATE TABLE IF NOT EXISTS scrape_source_runs (
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
ALTER TABLE scrape_runs
    ADD COLUMN IF NOT EXISTS max_entries_per_feed UInt64 DEFAULT 0;
ALTER TABLE scrape_source_runs
    ADD COLUMN IF NOT EXISTS tags Array(String) DEFAULT [],
    ADD COLUMN IF NOT EXISTS feed_urls Array(String) DEFAULT [];

ALTER TABLE scrape_runs
    ADD COLUMN IF NOT EXISTS http_concurrency UInt32 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS http_per_host UInt32 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS queue_batches UInt32 DEFAULT 0;
