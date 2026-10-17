CREATE TABLE IF NOT EXISTS post_reference_evidence (
    domain LowCardinality(String),
    post_id String,
    url String,
    source LowCardinality(String),
    title String,
    target_url String,
    kind LowCardinality(String),
    reference_id String,
    reference_url String,
    author String,
    matched_url String,
    excerpt String,
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY (domain, post_id, target_url, kind, reference_id, observed_at);

CREATE TABLE IF NOT EXISTS reference_index_versions (
    version UInt32,
    completed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(completed_at)
ORDER BY version;
