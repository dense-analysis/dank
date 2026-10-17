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
    domain String,
    post_id String,
    url String,
    source LowCardinality(String),
    title String,
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY (video_id, domain, post_id);

CREATE TABLE IF NOT EXISTS youtube_index_versions (
    version UInt32,
    completed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(completed_at)
ORDER BY version;

CREATE MATERIALIZED VIEW IF NOT EXISTS youtube_links_from_posts
TO youtube_links AS
SELECT arrayJoin(arrayDistinct(extractAll(
        replaceAll(decodeHTMLComponent(concat(url, ' ', html)), '\\/', '/'),
        '(?i:https?://(?:(?:www|m|music)\\.)?(?:youtube\\.com|youtube-nocookie\\.com)/(?:watch\\?(?:[^\\s"\'<>#]*&)?v=|shorts/|embed/)|https?://(?:www\\.)?youtu\\.be/)([A-Za-z0-9_-]{11})(?:[^A-Za-z0-9_-]|$)'
    ))) AS video_id,
    domain, post_id, url, source, title AS title,
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
    domain, post_id, url, source, JSONExtractString(payload, 'title') AS title,
    scraped_at AS observed_at
FROM raw_posts
WHERE domain != 'youtube.com' AND source != 'youtube'
;
