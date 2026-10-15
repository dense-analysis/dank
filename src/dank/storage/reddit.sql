CREATE TABLE IF NOT EXISTS reddit_community_icons (
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
