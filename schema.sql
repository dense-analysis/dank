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
    source LowCardinality(String),
    INDEX idx_posts_title_tokens title TYPE tokenbf_v1(32768, 4, 0) GRANULARITY 64,
    INDEX idx_posts_html_tokens html TYPE tokenbf_v1(65536, 4, 0) GRANULARITY 64
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
