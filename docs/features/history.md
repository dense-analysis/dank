# Scrape history and measurements

Each scrape with selected sources creates a run ID and records its outcome in
ClickHouse. History starts with this feature; previous log files are not imported.
The scraper automatically creates missing history tables in the configured
database, including existing Docker volumes. The database user needs permission
to create these tables on first use and add columns when the schema grows.

## What is recorded

| Table | Record |
| --- | --- |
| `scrape_runs` | One run: timing, outcome, totals, runtime resources and limits |
| `scrape_source_runs` | One configured source within a run: timing and counters |

Both tables use versioned replacement rows. Query with `FINAL` to see the latest
state per run/source. Running rows are written at startup and final rows when
the run ends. Each new invocation gets a new ID, preserving previous runs.

Outcomes are `completed`, `partial`, `failed`, `skipped` or `cancelled`.
A partial result has saved records and unsuccessful fetches or downloads.
X login failures also count as fetch failures. A source without discoverable
feeds is skipped unless a fetch/parse failure
was observed. A valid empty feed can complete with zero posts. A hard kill or
database outage can leave a `running` row: this means no final outcome was
recorded, not necessarily that the process is still alive.

Source records snapshot `tags` and the effective `feed_urls` for that run.
Changing configuration does not relabel older runs. Before feed selection,
a running row contains the configured URLs; automatic URLs are recorded in
the final row. Existing history rows have empty arrays for previously
unrecorded values. `scrape_runs.max_entries_per_feed` records the global entry
limit; zero means unlimited, including older runs.

## Timing and counts

- `collection_ms`: source discovery and article collection, before queued
  downloads and writes necessarily finish.
- `elapsed_ms`: elapsed time through that source's completed downloads and
  successful writes. Overall run elapsed time is measured separately.
- `posts_queued` / `posts_saved`: raw captures queued / successfully written,
  not unique new articles. `asset_records_saved` also counts successful writes.
- `media_references`: queued references, before the existing URL deduplication.
- `files_downloaded` / `files_cached`: available media files that did not /
  did exist before their download job. `downloaded_file_bytes` is retained
  new-file size, not total network traffic or discarded download bytes.
- `files_failed` and `media_skipped` separate unsuccessful downloads from
  intentionally retained references and ignored assets.
- `http_requests`, `http_429`, `retries`, `fetch_failures` and
  `parse_failures` explain incomplete results. HTTP counters cover RSS and
  direct media requests made through aiohttp; Chromium and yt-dlp's internal
  requests are not included.
- `request_ms` and `retry_wait_ms` sum individual request/wait durations.
  They can overlap and must not be added to derive elapsed time.

A source can spend time waiting in the current write/download queue. Its
elapsed time includes that delay. Adding source durations does not give the
overall run duration because collection, media and writes can overlap.

## Terminal reporting

Startup shows the run ID, native/container runtime, CPU allowance, detected
memory capacity and effective worker limits. CPU detection includes process
affinity and visible Linux cgroup quotas; memory capacity includes visible
container limits. Memory capacity is not current free memory. Unknown memory
capacity is stored as NULL.

Every five seconds, stdout reports active HTTP requests and media jobs,
waiting retries, queued batches, pending records and saved posts. Source
summaries appear after their pending records are saved. Warnings/errors still
use stderr; both streams also go to the configured log file.

The existing scheduling remains: one source collecting at a time, up to four
RSS requests and four media jobs, with an unbounded batch queue. The recorded
limits, RSS entry/file-size settings, X collection limits and source-code fingerprint
(`code_version`, including the dependency lock when available) provide a
baseline for comparing later changes. Browser collection and embedding work
are not newly parallelised; processing remains a separate command.

## Query history

**Native**

```sh
make query MODE=native ARGS='-q "SELECT run_id, started_at, status, elapsed_ms / 1000 AS seconds, posts_saved, max_entries_per_feed, code_version FROM scrape_runs FINAL ORDER BY started_at DESC LIMIT 10"'
make query MODE=native ARGS='-q "SELECT run_id, domain, status, collection_ms / 1000 AS collection_seconds, elapsed_ms / 1000 AS total_seconds, posts_saved, files_downloaded, files_cached, http_429, retries FROM scrape_source_runs FINAL ORDER BY started_at DESC LIMIT 20"'
```

**Docker**

```sh
make query ARGS='-q "SELECT run_id, started_at, status, elapsed_ms / 1000 AS seconds, posts_saved, max_entries_per_feed, code_version FROM scrape_runs FINAL ORDER BY started_at DESC LIMIT 10"'
make query ARGS='-q "SELECT run_id, domain, status, collection_ms / 1000 AS collection_seconds, elapsed_ms / 1000 AS total_seconds, posts_saved, files_downloaded, files_cached, http_429, retries FROM scrape_source_runs FINAL ORDER BY started_at DESC LIMIT 20"'
```

Query `tags` and `feed_urls` in `scrape_source_runs` to inspect source labels
and selected feeds. These are source-level snapshots, not article topic labels.

Compare equivalent source selections, outcomes and file reuse before judging
speed. Use `run_id` to join source records to their runtime settings.
