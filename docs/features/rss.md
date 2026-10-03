# RSS and Atom feeds

DANK collects posts from websites with RSS 1.0, RSS 2.0 or Atom feeds.
For configured domains other than `x.com`, it discovers feeds from the site's
homepage, reads their entries and fetches the linked article pages.

## Configure

Add domains and RSS options to `config.toml` in the checkout root. Use the
[native or Docker setup](../setup.md) to create it. These RSS options apply
to both modes and show their default values:

```toml
sources = ["blog.codinghorror.com"]

[rss]
feed_staleness_days = 14
keep_feed_on_fetch_failure = false
```

Feed URLs are cached in ClickHouse. `feed_staleness_days` controls when DANK
refreshes feed discovery; it does not schedule scraping. Each scrape run reads
the cached feeds and collects their current entries. Duplicate article URLs
across feeds are fetched once per domain during a run. Within each article
batch, links that differ only by a fragment such as `#comment-123` share one
page fetch while retaining their separate feed entries and original URLs.

The BBC uses a built-in set of feed URLs. Other sites need discoverable feed
links on their homepage.

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
