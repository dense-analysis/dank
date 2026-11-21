CREATE TABLE IF NOT EXISTS asset_retention (
    domain LowCardinality(String),
    post_id String,
    url String,
    local_path String,
    first_scraped_at DateTime64(3, 'UTC'),
    scrubbed_at Nullable(DateTime64(3, 'UTC')),
    updated_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (domain, post_id, url, local_path);
