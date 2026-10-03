# RSS and Atom feeds

DANK collects posts from websites with RSS 1.0, RSS 2.0 or Atom feeds.
For configured domains other than `x.com`, it discovers feeds from the site's
homepage, reads their entries and fetches the linked article pages.

## Configure

Add domains and RSS options to your private `config.toml`. The values below
show the defaults for RSS options:

```toml
sources = ["blog.codinghorror.com"]

[rss]
feed_staleness_days = 14
keep_feed_on_fetch_failure = false
```

Feed URLs are cached in ClickHouse. `feed_staleness_days` controls when DANK
refreshes feed discovery; it does not schedule scraping. Each scrape run reads
the cached feeds and collects their current entries. Duplicate article URLs
across feeds are fetched once per domain during a run.

The BBC uses a built-in set of feed URLs. Other sites need discoverable feed
links on their homepage.

## Collect and process

```sh
uv run scrape --domains '^blog\.codinghorror\.com$'
uv run process --age 24h
```

`--domains` filters the domains already listed in `sources`. Processing uses
all configured sources within the selected collection-time window.

Each raw post retains the article URL, originating feed URL, collection time,
publication time when available, and a JSON payload containing the individual
feed entry's XML and fetched article HTML. Images, audio, video and supported
embeds found on article pages enter the [media download](media.md) workflow.

## Article fetch failures

By default, entries whose article fetch fails or returns an empty body are
skipped. Set `keep_feed_on_fetch_failure = true` to retain those entries with
empty article HTML and `page_fetch_status: "failed"` in the raw payload.

The [processor](processing.md) can use the retained feed's title, author,
publication date and summary or full content. Keeping the entry does not
schedule a retry. If the feed itself cannot be fetched or parsed, there are
no entries to retain.

See the [README](../../README.md) for ClickHouse setup and shared settings.
