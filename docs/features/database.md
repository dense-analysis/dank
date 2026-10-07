# Stored data and database queries

DANK stores collected and processed records in ClickHouse. Downloaded media
files remain in the [asset directory](media.md). Native runs connect to your
configured ClickHouse instance; Docker provides a database and creates its
tables automatically. See [setup](../setup.md) for both modes.

## Tables

| Table | Contents |
| --- | --- |
| `scrape_runs` | Run timings, outcomes, totals and runtime limits |
| `scrape_source_runs` | Per-source timings, outcomes and counters |
| `site_feeds` | Discovered feed URLs and discovery timestamps |
| `raw_posts` | Source IDs, URLs, timestamps and retained payloads |
| `raw_assets` | Discovered asset references and download paths |
| `posts` | Processed content, author, timestamps and text embeddings |
| `assets` | Processed local-file references, types and sizes |
| `web_embedding_cache` | Cached search-query embeddings |

Raw tables retain repeated captures. Processed tables use replacement engines
to keep the latest version per identity: `(domain, post_id)` for posts and
`(domain, post_id, url)` for assets. Use `FINAL` when querying processed tables
to resolve replacement versions at read time.

See [scrape history](history.md) for run comparisons. History tables also require
`FINAL` to resolve their running and final versions.

## Query from the command line

The query tool uses the ClickHouse connection in `config.toml`:

**Native**

```sh
uv run clickhouse-query -q 'SELECT domain, count() FROM posts FINAL GROUP BY domain'
uv run clickhouse-query -q 'SELECT url, title FROM posts FINAL ORDER BY created_at DESC LIMIT 10'
uv run clickhouse-query -q 'SHOW CREATE TABLE posts'
```

**Docker**

```sh
docker compose run --rm dank clickhouse-query -q 'SELECT domain, count() FROM posts FINAL GROUP BY domain'
docker compose run --rm dank clickhouse-query -q 'SELECT url, title FROM posts FINAL ORDER BY created_at DESC LIMIT 10'
docker compose run --rm dank clickhouse-query -q 'SHOW CREATE TABLE posts'
```

It accepts a single `SELECT`, `SHOW` or `EXPLAIN` statement and rejects write
operations. Long values are abbreviated in the displayed output; use `--full`
to display them without truncation:

**Native**

```sh
uv run clickhouse-query --full -q 'SELECT payload FROM raw_posts LIMIT 1'
uv run clickhouse-query -q 'SELECT count() FROM posts FINAL'
```

**Docker**

```sh
docker compose run --rm dank clickhouse-query --full -q 'SELECT payload FROM raw_posts LIMIT 1'
docker compose run --rm dank clickhouse-query -q 'SELECT count() FROM posts FINAL'
```

Use `LIMIT` in the query to control how many rows are returned. `--full` changes
value formatting, not the number of rows fetched.

## Inspect a retained feed entry

RSS/Atom raw payloads contain `feed_xml` and `page_html`. Entries retained after
an article-fetch failure also carry `page_fetch_status: "failed"`:

**Native**

```sh
uv run clickhouse-query -q "SELECT url, scraped_at FROM raw_posts WHERE source = 'rss' AND JSONExtractString(payload, 'page_fetch_status') = 'failed' ORDER BY scraped_at DESC LIMIT 20"
```

**Docker**

```sh
docker compose run --rm dank clickhouse-query -q "SELECT url, scraped_at FROM raw_posts WHERE source = 'rss' AND JSONExtractString(payload, 'page_fetch_status') = 'failed' ORDER BY scraped_at DESC LIMIT 20"
```

This query shows retained failed captures, including earlier attempts that may
have been followed by a successful capture. It is not a list of pending retries.
See [RSS feeds](rss.md) for the retention setting and
[processing](processing.md) for how raw records become searchable posts.
