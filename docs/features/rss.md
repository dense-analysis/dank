# RSS and Atom feeds

DANK collects posts from websites with RSS 1.0, RSS 2.0 or Atom feeds.
For configured domains other than `x.com`, it reads selected or discovered
feeds and fetches their linked article pages.

## Configure

Add domains and RSS options to `config.toml` in the checkout root. Use the
[native or Docker setup](../setup.md) to create it. These RSS options apply
to both modes and show their default values:

```toml
sources = [
    "blog.codinghorror.com",
    { domain = "martinfowler.com", tags = ["architecture", "ddd"] },
    { domain = "order-order.com", feed_urls = ["https://order-order.com/feed/"], tags = ["news"] },
]

[rss]
feed_staleness_days = 14
keep_feed_on_fetch_failure = false
max_entries_per_feed = 0
```

Plain domain strings remain supported. A non-empty `feed_urls` array selects
only those HTTP(S) feeds, bypassing homepage discovery and its cache. Omitted
or empty arrays use automatic discovery. Feeds may live on another host.
Explicit feeds are not written to the discovery cache, so two source entries
on the same domain can select different feeds independently.

Optional `tags` are manual source labels: trimmed, lowercased and deduplicated.
They are saved with effective feed URLs in [source history](history.md), not
inferred as article topics or applied as article-search filters. Tags also
work for X sources; explicit RSS feeds are not supported for `x.com`.

Automatically discovered feed URLs are cached in ClickHouse. `feed_staleness_days` controls when DANK
refreshes feed discovery; it does not schedule scraping. Each scrape run reads
the cached feeds and collects their current entries. Duplicate article URLs
across feeds are fetched once per domain during a run. Within each article
batch, links that differ only by a fragment such as `#comment-123` share one
page fetch while retaining their separate feed entries and original URLs.

In automatic mode, the BBC uses built-in feed URLs; other sites need
discoverable feed links on their homepage.

Set `[rss].max_entries_per_feed = 20` to select at most 20 entries from each
feed on every run. Zero or omission preserves unlimited fetching and feed
order. With a positive limit, dated entries are selected newest first;
undated entries fill remaining places in feed order. Ties preserve feed
order, and dates without a timezone are treated as UTC.

The limit is applied before article requests, media discovery and cross-feed
URL deduplication. It is an entry limit, not a successful-download target, and
is not a cursor through the backlog. Feed documents are still downloaded in
full. Selection counts and the effective limit appear in logs and history.

## Collect and process

**Native**

```sh
uv run scrape --headless --domains '^blog\.codinghorror\.com$'
uv run process --age 24h
```

**Docker**

```sh
docker compose run --rm dank scrape --headless --domains '^blog\.codinghorror\.com$'
docker compose run --rm dank process --age 24h
```

`--domains` filters the domains already listed in `sources`. Processing uses
all configured sources within the selected collection-time window.

Each raw post retains the article URL, originating feed URL, collection time,
publication time when available, and a JSON payload containing the individual
feed entry's XML and fetched article HTML. Images, audio, video and supported
embeds found on article pages enter the [media download](media.md) workflow.

## Rate limits

Homepage, feed and article requests retry HTTP 429 responses up to three
times after the initial attempt, waiting 2, 4 and 8 seconds. A valid
`Retry-After` header (seconds or an HTTP date) can increase each wait up to
30 seconds. Longer requested waits skip the URL rather than retrying early.
Missing or invalid headers use the exponential delays.

A 429 starts a shared cooldown for that hostname. New homepage, feed, article
and direct media requests to it wait without occupying global HTTP slots;
other hosts can continue and save results. Requests already in flight finish.
Each request waits at most 30 seconds for a shared cooldown, including any
extensions; longer waits fail that URL without sending it early.
See [concurrency settings](history.md#concurrency) to adjust the worker limits.

These defaults apply to native and Docker runs without configuration changes.
Retries, wait durations and exhausted limits appear in the terminal; progress
updates continue while waiting. Other HTTP errors are not retried. Requests
retain their existing timeouts, and Ctrl+C can interrupt retry waits.

## Article fetch failures

By default, entries whose article fetch fails or returns an empty body are
skipped. Set `keep_feed_on_fetch_failure = true` to retain those entries with
empty article HTML and `page_fetch_status: "failed"` in the raw payload.

The [processor](processing.md) can use the retained feed's title, author,
publication date and summary or full content. Keeping the entry does not
schedule a later retry after these immediate attempts are exhausted. If the
feed itself cannot be fetched or parsed, there are no entries to retain.

See the [README](../../README.md) for ClickHouse setup and shared settings.

## Progress and errors

Both native and Docker runs show source numbers, feed discovery, article-fetch
counts and media progress. Updates repeat every five seconds while a stage is
pending, followed by saved-post and available-file totals at completion.

Fetch failures include the URL and HTTP status at warning level, including
`Retry-After` when a server supplies it. A source with no usable feeds is
explicitly reported as skipped. This makes failed discovery distinguishable
from a source that simply has no entries.

Normal progress uses stdout; warnings and errors use stderr. Both remain in
the configured log file, and `logging.level` controls verbosity. To capture
separate streams:

**Native**

```sh
make scrape MODE=native >scrape.out 2>scrape.err
```

**Docker**

```sh
make scrape >scrape.out 2>scrape.err
```

Docker Make commands disable pseudo-TTY allocation to preserve the streams.
Compose's own container-startup messages may also appear in stderr.

Article progress counts unique page URLs per batch, including failed fetches;
retries do not advance the count. The article batch summary counts feed entries
with and without fetched HTML, including entries that share a page. Media
counts are for unique URLs in each batch. "Available" includes reused local
files; "not downloaded" includes failed downloads, size-limit skips and
reference-only assets. Scraping does not run the separate processing command.
