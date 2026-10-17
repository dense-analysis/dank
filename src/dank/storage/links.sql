CREATE TABLE IF NOT EXISTS post_urls (
    url String,
    domain LowCardinality(String),
    post_id String,
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY (url, domain, post_id, observed_at);

CREATE TABLE IF NOT EXISTS post_links (
    domain LowCardinality(String),
    post_id String,
    target_url String,
    kind LowCardinality(String),
    content_hash FixedString(64),
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY (domain, post_id, target_url, observed_at, content_hash);

CREATE TABLE IF NOT EXISTS link_index_versions (
    version UInt32,
    completed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(completed_at)
ORDER BY version;
