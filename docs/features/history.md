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
state per run/source. A running row is written when a run or source starts;
its final row is written when it finishes. Completed sources are visible while
other sources continue. Each new invocation preserves previous runs.

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
`media_download_types` snapshots the allowed download types; an empty array
means downloads were disabled. Older runs default to all three types, matching
the behaviour before this option existed.

## Timing and counts

- `collection_ms`: source discovery and article collection, before queued
  downloads and writes necessarily finish.
- `elapsed_ms`: elapsed time through that source's completed downloads and
  successful writes. Overall run elapsed time is measured separately.
- `posts_queued` / `posts_saved`: raw captures discovered / successfully written,
  not unique new articles. `asset_records_saved` also counts successful writes.
- `media_references`: queued references, before the existing URL deduplication.
- `files_downloaded` / `files_cached`: available media files that did not /
  did exist before their download job. `downloaded_file_bytes` is retained
  new-file size, not total network traffic or discarded download bytes.
- `files_failed` and `media_skipped` separate unsuccessful downloads from
  intentionally retained references, disabled media types and ignored assets.
  Intentional skips do not make a run partial or failed.
- `http_requests`, `http_429`, `retries`, `fetch_failures` and
  `parse_failures` explain incomplete results. HTTP counters cover RSS and
  direct media requests made through aiohttp; Chromium and yt-dlp's internal
  requests are not included. Redirect hops count as separate HTTP requests.
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

## Run logs

`make scrape` and `make process` save a separate plain-text log for each run,
while continuing to display progress and errors in the terminal. This works
in Docker and native mode. Files live on the host in the checkout where you
ran Make, so they survive removal of Docker's temporary command containers:

```text
logs/scrape/2026-10-15T14-30-00Z-12345.log
logs/process/2026-10-15T14-40-00Z-12378.log
```

Names contain the UTC start time and a process ID to separate simultaneous
runs. Each file records the command and arguments, mode, working directory,
checkout revision (including whether it has changes), both output streams,
finish time, duration in seconds and exit status. Nonzero command exits still
fail Make. Ctrl+C records exit 130; an uncatchable kill or power loss can
leave a partial log without a final status. Output streams remain separate
in the terminal and are combined in the archive.

Logs are ignored by Git and kept until you delete them. The application still
writes its configured `dank.log`; direct `uv run` or `docker compose run`
commands do not create these per-run archives. The recorded checkout revision
identifies host source; Docker uses the application image last built.

Find recent runs and review warnings, errors and outcomes:

```sh
ls -t logs/scrape/*.log
ls -t logs/process/*.log
rg --no-ignore -n 'WARNING|ERROR|Traceback|Exit status:' logs/scrape logs/process
```

For a comparison, ask an agent to inspect these folders and compare matching
commands and source selections, checking errors, counts and elapsed times.
An exit status of zero does not rule out source warnings or partial results.

## Concurrency

These optional settings have the same defaults in native and Docker runs:

```toml
[scrape]
source_concurrency = 4
http_concurrency = 16
http_per_host = 2
media_concurrency = 4
queue_batches = 2
```

Up to four source pipelines collect, download and save independently. Each
post batch is committed before discovery advances, so a later collection
failure or cancellation cannot discard those saved posts. Media keeps its
own batching: each source has at most two queued media batches, and a full
queue pauses its producer. Batch sizes vary with discovered media, so this
bounds the backlog, not memory in bytes.
A source slot is reused after its downloads and writes finish. Set
`source_concurrency = 1` for sequential sources.

The HTTP limits are shared across homepage, feed, article and direct media
requests, including redirect destinations. Hosts are grouped by hostname,
including across configured sources. Four RSS fetch jobs per source and four
media jobs across the run provide additional bounds. X and Reddit use separate
browsers, profiles and locks, so either can collect while the other waits.
Sources on the same site share a browser and run one at a time. Both still
count toward `source_concurrency`; RSS-only runs do not launch either browser.
Chromium and yt-dlp's internal HTTP requests are outside the direct HTTP
limiter; processing remains a separate command.

All values must be positive integers. CPU/memory capacity is reported for
tuning; limits do not automatically grow with core count. `scrape_runs` records
the effective source worker count, HTTP/media limits and per-source queue
capacity. Older rows have zero for unrecorded HTTP limits and the previously
unbounded queue. Limits, entry/file-size settings and `code_version` (including
the dependency lock) make comparisons reproducible.

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
