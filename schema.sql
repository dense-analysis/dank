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
    payload String CODEC(ZSTD(3)),
    is_reference UInt8 ALIAS (
        source = 'x'
        AND JSONExtractString(payload, '__typename') = 'Tweet'
        AND JSONExtractString(payload, 'rest_id') != ''
        AND arraySort(JSONExtractKeys(payload)) = ['__typename', 'rest_id']
    )
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
    media_download_types Array(String) DEFAULT ['image', 'audio', 'video'],
    feed_staleness_days Int64,
    x_max_posts Int64,
    x_max_scrolls Int64,
    x_scroll_pause_seconds Float64,
    version UInt64,
    posts_queued UInt64,
    posts_saved UInt64,
    post_references_queued UInt64 DEFAULT 0,
    post_references_saved UInt64 DEFAULT 0,
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
    post_references_queued UInt64 DEFAULT 0,
    post_references_saved UInt64 DEFAULT 0,
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

ALTER TABLE dank.scrape_runs
    ADD COLUMN IF NOT EXISTS media_download_types Array(String)
    DEFAULT ['image', 'audio', 'video'];

ALTER TABLE dank.scrape_runs
    ADD COLUMN IF NOT EXISTS post_references_queued UInt64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS post_references_saved UInt64 DEFAULT 0;
ALTER TABLE dank.scrape_source_runs
    ADD COLUMN IF NOT EXISTS post_references_queued UInt64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS post_references_saved UInt64 DEFAULT 0;

-- Cached community identity, separate from Reddit post media.
CREATE TABLE IF NOT EXISTS dank.reddit_community_icons (
    community String,
    icon_url String,
    local_path String,
    content_type LowCardinality(String),
    scraped_at DateTime64(3, 'UTC'),
    request_url String,
    payload String CODEC(ZSTD(3))
)
ENGINE = ReplacingMergeTree(scraped_at)
ORDER BY community;

CREATE TABLE IF NOT EXISTS dank.reddit_community_totals (
    community String,
    total_posts UInt64,
    source LowCardinality(String),
    updated_at DateTime64(3, 'UTC'),
    scraped_at DateTime64(3, 'UTC'),
    request_url String,
    payload String CODEC(ZSTD(3))
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY community;

ALTER TABLE dank.raw_posts
    ADD COLUMN IF NOT EXISTS is_reference UInt8 ALIAS (
        source = 'x'
        AND JSONExtractString(payload, '__typename') = 'Tweet'
        AND JSONExtractString(payload, 'rest_id') != ''
        AND arraySort(JSONExtractKeys(payload)) = ['__typename', 'rest_id']
    );

-- YouTube feed appearances and cross-source video references.
CREATE TABLE IF NOT EXISTS dank.youtube_video_metadata (
    video_id String,
    observed_at DateTime64(3, 'UTC'),
    retry_after DateTime64(3, 'UTC'),
    status LowCardinality(String),
    provider LowCardinality(String),
    request_url String,
    http_status UInt16,
    error String,
    provider_payload String CODEC(ZSTD(3)),
    payload String CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(observed_at)
ORDER BY (video_id, observed_at);

CREATE TABLE IF NOT EXISTS dank.youtube_thumbnail_observations (
    post_id String,
    observed_at DateTime64(3, 'UTC'),
    captured_url String,
    fetched_url String,
    local_path String,
    content_hash String,
    visual_hash String,
    visual_color String,
    width UInt32,
    height UInt32
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(observed_at)
ORDER BY (post_id, observed_at, content_hash);

ALTER TABLE dank.youtube_thumbnail_observations
ADD COLUMN IF NOT EXISTS title String DEFAULT '';
ALTER TABLE dank.youtube_thumbnail_observations
ADD COLUMN IF NOT EXISTS title_observed_at Nullable(DateTime64(3, 'UTC'));

CREATE TABLE IF NOT EXISTS dank.youtube_channels (
    channel_id String,
    channel_name String,
    channel_url String,
    feed_url String,
    icon_url String,
    local_path String,
    content_type String,
    reported_video_count Nullable(UInt64),
    reported_video_count_text String,
    observed_at DateTime64(3, 'UTC'),
    request_url String,
    payload String CODEC(ZSTD(3))
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY channel_id;

CREATE TABLE IF NOT EXISTS dank.youtube_discoveries (
    run_id String,
    item_id String,
    kind LowCardinality(String),
    observed_at DateTime64(3, 'UTC'),
    feed_url String,
    feed_name String,
    feed_kind LowCardinality(String),
    tab String,
    shelf String,
    rank UInt32,
    region String,
    playlist_id String,
    payload String CODEC(ZSTD(3))
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY (feed_url, run_id, shelf, item_id, rank);

CREATE TABLE IF NOT EXISTS dank.youtube_links (
    video_id String,
    format_hint LowCardinality(String),
    domain String,
    post_id String,
    url String,
    source LowCardinality(String),
    title String,
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY (video_id, domain, post_id, format_hint);

CREATE TABLE IF NOT EXISTS dank.youtube_index_versions (
    version UInt32,
    completed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(completed_at)
ORDER BY version;

CREATE TABLE IF NOT EXISTS dank.youtube_reference_evidence (
    video_id String,
    format_hint LowCardinality(String),
    domain String,
    post_id String,
    url String,
    source LowCardinality(String),
    title String,
    kind LowCardinality(String),
    reference_id String,
    reference_url String,
    author String,
    matched_url String,
    excerpt String,
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY (video_id, domain, post_id, kind, reference_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS dank.youtube_links_from_posts
TO dank.youtube_links AS
SELECT arrayJoin(arrayDistinct(extractAll(
        replaceAll(decodeHTMLComponent(concat(url, ' ', html)), '\\/', '/'),
        '(?i:https?://(?:(?:www|m|music)\\.)?(?:youtube\\.com|youtube-nocookie\\.com)/(?:watch\\?(?:[^\\s"\'<>#]*&)?v=|shorts/|embed/)|https?://(?:www\\.)?youtu\\.be/)([A-Za-z0-9_-]{11})(?:[^A-Za-z0-9_-]|$)'
    ))) AS video_id,
    if(match(replaceAll(decodeHTMLComponent(concat(url, ' ', html)), '\\/', '/'),
        concat('(?i:https?://(?:(?:www|m|music)\\.)?(?:youtube\\.com|youtube-nocookie\\.com)/shorts/)', video_id, '(?:[^A-Za-z0-9_-]|$)')),
        'short', 'video') AS format_hint,
    domain, post_id, url, source, substringUTF8(title, 1, 300) AS title,
    updated_at AS observed_at
FROM dank.posts
WHERE domain != 'youtube.com' AND source != 'youtube'
;

CREATE MATERIALIZED VIEW IF NOT EXISTS dank.youtube_links_from_raw_posts
TO dank.youtube_links AS
SELECT arrayJoin(arrayDistinct(extractAll(
        replaceAll(decodeHTMLComponent(concat(url, ' ', payload)), '\\/', '/'),
        '(?i:https?://(?:(?:www|m|music)\\.)?(?:youtube\\.com|youtube-nocookie\\.com)/(?:watch\\?(?:[^\\s"\'<>#]*&)?v=|shorts/|embed/)|https?://(?:www\\.)?youtu\\.be/)([A-Za-z0-9_-]{11})(?:[^A-Za-z0-9_-]|$)'
    ))) AS video_id,
    if(match(replaceAll(decodeHTMLComponent(concat(url, ' ', payload)), '\\/', '/'),
        concat('(?i:https?://(?:(?:www|m|music)\\.)?(?:youtube\\.com|youtube-nocookie\\.com)/shorts/)', video_id, '(?:[^A-Za-z0-9_-]|$)')),
        'short', 'video') AS format_hint,
    domain, post_id, url, source, substringUTF8(coalesce(nullIf(JSONExtractString(payload, 'title'), ''), nullIf(JSONExtractString(payload, 'note_tweet', 'note_tweet_results', 'result', 'text'), ''), JSONExtractString(payload, 'legacy', 'full_text')), 1, 300) AS title,
    scraped_at AS observed_at
FROM dank.raw_posts
WHERE domain != 'youtube.com' AND source != 'youtube'
;
