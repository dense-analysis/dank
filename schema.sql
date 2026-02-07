CREATE DATABASE IF NOT EXISTS dank;

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

ALTER TABLE dank.posts
    ADD COLUMN IF NOT EXISTS title_embedding Array(Float32),
    ADD COLUMN IF NOT EXISTS html_embedding Array(Float32);

CREATE TABLE IF NOT EXISTS dank.site_feeds (
    domain LowCardinality(String),
    feed_url String,
    feed_type LowCardinality(String),
    scraped_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(scraped_at)
ORDER BY (domain, feed_url);
