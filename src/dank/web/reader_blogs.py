"""Saved blog articles and evidence that they came from another feed."""

from __future__ import annotations

from dank.storage.clickhouse import ClickHouseClient

# Domain identity excludes wrappers whose URL belongs to another publisher.
BLOG_ARTICLE_CONDITION = """source IN ('rss', 'linked_article')
    AND replaceRegexpOne(domain, '^www[.]', '') = domainWithoutWWW(url)
    AND domain NOT IN ('x.com', 'twitter.com', 'reddit.com', 'youtube.com',
        'news.ycombinator.com')""".strip()
BLOG_CONDITION = """source IN ('rss', 'linked_article', 'hacker_news')
    AND domain NOT IN ('x.com', 'twitter.com', 'reddit.com', 'youtube.com')
""".strip()

# Archived references remain valid provenance after a feed post changes, but
# observations newer than the processed source/target versions are not visible.
EXTERNAL_BLOG_KEYS_SQL = """
SELECT DISTINCT target.domain, target.post_id
FROM post_reference_evidence AS evidence FINAL
INNER JOIN (SELECT domain, post_id, updated_at FROM posts FINAL) AS origin
    ON origin.domain = evidence.domain AND origin.post_id = evidence.post_id
INNER JOIN post_urls AS alias FINAL ON alias.url = evidence.target_url
INNER JOIN (SELECT domain, post_id, updated_at FROM posts FINAL) AS target
    ON target.domain = alias.domain AND target.post_id = alias.post_id
WHERE evidence.source IN ('x', 'reddit', 'hacker_news', 'rss')
    AND evidence.observed_at <= origin.updated_at
    AND alias.observed_at <= target.updated_at
    AND (origin.domain, origin.post_id) != (target.domain, target.post_id)
UNION DISTINCT
SELECT capture.domain, capture.post_id
FROM raw_posts AS capture
INNER JOIN (SELECT domain, post_id, updated_at FROM posts FINAL) AS target
    ON target.domain = capture.domain AND target.post_id = capture.post_id
WHERE capture.source = 'linked_article'
    AND capture.scraped_at <= target.updated_at
    AND JSONExtractString(capture.payload, 'collection_origin', 'source')
        IN ('x', 'reddit', 'hacker_news', 'rss')
    AND notEmpty(JSONExtractString(capture.payload,
        'collection_origin', 'domain'))
    AND notEmpty(JSONExtractString(capture.payload,
        'collection_origin', 'post_id'))
    AND parseDateTime64BestEffortOrNull(JSONExtractString(capture.payload,
        'collection_origin', 'observed_at'), 3) <= capture.scraped_at
    AND (JSONExtractString(capture.payload, 'collection_origin', 'domain'),
        JSONExtractString(capture.payload, 'collection_origin', 'post_id'))
        != (capture.domain, capture.post_id)
""".strip()
EXTERNAL_BLOG_CONDITION = (
    "(" + BLOG_ARTICLE_CONDITION + ") AND (domain, post_id) IN ("
    + EXTERNAL_BLOG_KEYS_SQL + ")"
)


async def external_blog_count(client: ClickHouseClient) -> int:
    result = await client.fetch_json(
        "SELECT count() AS blog_external_count FROM posts FINAL WHERE "
        + EXTERNAL_BLOG_CONDITION,
    )

    return int(result.rows[0]["blog_external_count"]) if result.rows else 0
