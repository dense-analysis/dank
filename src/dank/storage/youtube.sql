CREATE TABLE IF NOT EXISTS youtube_video_metadata (
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

CREATE TABLE IF NOT EXISTS youtube_thumbnail_observations (
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

ALTER TABLE youtube_thumbnail_observations
ADD COLUMN IF NOT EXISTS title String DEFAULT '';
ALTER TABLE youtube_thumbnail_observations
ADD COLUMN IF NOT EXISTS title_observed_at Nullable(DateTime64(3, 'UTC'));

CREATE TABLE IF NOT EXISTS youtube_channels (
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

CREATE TABLE IF NOT EXISTS youtube_discoveries (
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

CREATE TABLE IF NOT EXISTS youtube_links (
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

CREATE TABLE IF NOT EXISTS youtube_index_versions (
    version UInt32,
    completed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(completed_at)
ORDER BY version;

CREATE TABLE IF NOT EXISTS youtube_reference_evidence (
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

CREATE MATERIALIZED VIEW IF NOT EXISTS youtube_links_from_posts
TO youtube_links AS
SELECT arrayJoin(arrayDistinct(extractAll(
        replaceAll(decodeHTMLComponent(concat(url, ' ', html)), '\\/', '/'),
        '(?i:https?://(?:(?:www|m|music)\\.)?(?:youtube\\.com|youtube-nocookie\\.com)/(?:watch\\?(?:[^\\s"\'<>#]*&)?v=|shorts/|embed/)|https?://(?:www\\.)?youtu\\.be/)([A-Za-z0-9_-]{11})(?:[^A-Za-z0-9_-]|$)'
    ))) AS video_id,
    if(match(replaceAll(decodeHTMLComponent(concat(url, ' ', html)), '\\/', '/'),
        concat('(?i:https?://(?:(?:www|m|music)\\.)?(?:youtube\\.com|youtube-nocookie\\.com)/shorts/)', video_id, '(?:[^A-Za-z0-9_-]|$)')),
        'short', 'video') AS format_hint,
    domain, post_id, url, source, substringUTF8(title, 1, 300) AS title,
    updated_at AS observed_at
FROM posts
WHERE domain != 'youtube.com' AND source != 'youtube'
;

CREATE MATERIALIZED VIEW IF NOT EXISTS youtube_links_from_raw_posts
TO youtube_links AS
SELECT arrayJoin(arrayDistinct(extractAll(
        replaceAll(decodeHTMLComponent(concat(url, ' ', payload)), '\\/', '/'),
        '(?i:https?://(?:(?:www|m|music)\\.)?(?:youtube\\.com|youtube-nocookie\\.com)/(?:watch\\?(?:[^\\s"\'<>#]*&)?v=|shorts/|embed/)|https?://(?:www\\.)?youtu\\.be/)([A-Za-z0-9_-]{11})(?:[^A-Za-z0-9_-]|$)'
    ))) AS video_id,
    if(match(replaceAll(decodeHTMLComponent(concat(url, ' ', payload)), '\\/', '/'),
        concat('(?i:https?://(?:(?:www|m|music)\\.)?(?:youtube\\.com|youtube-nocookie\\.com)/shorts/)', video_id, '(?:[^A-Za-z0-9_-]|$)')),
        'short', 'video') AS format_hint,
    domain, post_id, url, source, substringUTF8(coalesce(nullIf(JSONExtractString(payload, 'title'), ''), nullIf(JSONExtractString(payload, 'note_tweet', 'note_tweet_results', 'result', 'text'), ''), JSONExtractString(payload, 'legacy', 'full_text')), 1, 300) AS title,
    scraped_at AS observed_at
FROM raw_posts
WHERE domain != 'youtube.com' AND source != 'youtube'
;

CREATE TABLE IF NOT EXISTS youtube_transcripts (
    video_id String,
    observed_at DateTime64(3, 'UTC'),
    retry_after DateTime64(3, 'UTC'),
    status LowCardinality(String),
    error String,
    track_payload String CODEC(ZSTD(3)),
    provider_payload String CODEC(ZSTD(3)),
    payload String CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(observed_at)
ORDER BY (video_id, observed_at);

CREATE TABLE IF NOT EXISTS youtube_playlists (
    playlist_id String,
    title String,
    url String,
    description String,
    thumbnail_url String,
    channel_id String,
    channel_name String,
    channel_url String,
    reported_count Nullable(UInt32),
    entry_count UInt32,
    sort_order String,
    snapshot_hash String,
    observed_at DateTime64(6, 'UTC'),
    checked_at DateTime64(6, 'UTC'),
    status LowCardinality(String),
    error_type String,
    payload String CODEC(ZSTD(3))
)
ENGINE = MergeTree
ORDER BY (playlist_id, checked_at);

CREATE TABLE IF NOT EXISTS youtube_playlist_entries (
    playlist_id String,
    snapshot_hash String,
    position UInt32,
    entry_id String,
    video_id String,
    title String,
    channel_id String,
    channel_name String,
    channel_url String,
    thumbnail_url String,
    duration_seconds Nullable(Float64),
    available Bool,
    payload String CODEC(ZSTD(3))
)
ENGINE = ReplacingMergeTree
ORDER BY (playlist_id, snapshot_hash, position);
