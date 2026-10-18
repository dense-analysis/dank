# Hacker News

DANK uses [hckr news](https://hckrnews.com/) to collect stories that reached
Hacker News’ front page, ordered by the time they appeared there. The reader’s
**Hacker News** collection groups stories by UTC day and loads older collected
stories as you scroll. No browser or login is required.

## Configure

Add this entry alongside your existing sources in private `config.toml`:

```toml
sources = [
  { domain = "hckrnews.com", hacker_news_filter = "top50" }
]

[hacker_news]
days = 7
comment_limit = "all"
detail_max_posts = 20
detail_timeout_seconds = "all"
```

`news.ycombinator.com` is also accepted. Both names use the same canonical
Hacker News source and hckrnews selection rules. Collection defaults to the
past seven UTC calendar days including today, with **Top 50%** selected.

| Option | Reader label |
| --- | --- |
| `top10` | Top 10 |
| `top20` | Top 20 |
| `top50` | Top 50% (default) |
| `all` | All |

The filters use hckrnews’ daily points thresholds, include tied scores, and
scale cutoffs by the observed portion of the day. They are not Hacker News’
time-weighted homepage ranking. **All** means former front-page stories,
not every submission to Hacker News. Rankings are saved with each capture;
new votes can change them on later collections.

Reader filters apply to collected stories. To make the complete daily range
available under All, collect with `hacker_news_filter = "all"`. The reader
still starts at Top 50%. Search and date filters do not recalculate rankings.

## Add RSS or use it alone

A source is the identity you browse; its adapters determine how DANK collects
it. To keep hckrnews and the official API and add the
[HN RSS feed](https://news.ycombinator.com/rss), use this source definition:

```toml
[[sources]]
domain = "news.ycombinator.com"
hacker_news_filter = "top50"

[[sources.adapters]]
type = "hacker_news"

[[sources.adapters]]
type = "rss"
feed_urls = ["https://news.ycombinator.com/rss"]
```

The `hacker_news` adapter retains the existing archive and API behavior.
Adapters run in the configured order; a network failure in one does not prevent
the next from collecting. History records the selected adapter types and feed
URLs. Existing source definitions without adapters keep their current behavior.

For RSS alone, the ordinary feed configuration also works:

```toml
sources = [
  { domain = "news.ycombinator.com", feed_urls = ["https://news.ycombinator.com/rss"] }
]
```

Alternatively, select only the `rss` adapter. With no explicit feed URLs, it uses
HN's official RSS URL. RSS-only discovery does not call hckrnews or the HN API;
it still collects linked publisher pages through the article pipeline. The
separate detailed-scrape command remains available if discussions are wanted.

RSS entries use their HN discussion IDs, so repeat captures enrich the same
story. Two HN submissions linking to the same article retain separate identities.
Raw feed XML, feed URLs and capture times are retained. New captures preserve
previously collected archive rankings, API story bodies and discussions; native
fields take precedence when present. Older snapshots remain stored.

RSS provides the current feed, not the archive's seven-day history, points or
daily ranking eligibility. RSS-only stories appear under **All** in the Hacker
News collection. The Top filters require saved hckrnews eligibility, and unknown
counts remain unknown. `[rss].max_entries_per_feed` applies to RSS collection;
`hacker_news_filter` applies only when the `hacker_news` adapter is selected.

## Collect

```sh
make scrape MODE=native ARGS='--domains "^news\.ycombinator\.com$"'
make process MODE=native ARGS='--age 24h --domains "^news\.ycombinator\.com$"'
```

Omit `MODE=native` for Docker. Normal collection saves the listing and official
HN story metadata, including the author and actual submission time, without
fetching comments. Front-page arrival still determines daily grouping and
rankings; the timestamp on each card is the submission time.

Linked publisher pages are collected through the existing article pipeline.
Opening a story reads its saved original; **Discussion** opens its saved HN
thread. Already collected destinations are reused, including recognized X,
Reddit and YouTube URLs. Social posts without an existing capture remain linked
externally; they are not treated as generic publisher pages. Publisher failures
and unsupported document types remain visible as unavailable content.

In **All**, resolved HN links and RSS aggregator records appear under the
original source, with an
expandable **Links** list leading back to collected references, including
Lobsters feed entries that point to that publisher URL. HN self-posts
remain their own entries. Exact URLs and observed redirects establish identity;
query strings are preserved and unrelated articles are not merged by title.
The Links index includes links found in other processed sources too. Processing
an HN collection also processes its newly collected publisher pages.

## Detailed scrape

```sh
make scrape MODE=native ARGS='--hacker-news-details'
make process MODE=native ARGS='--age 24h --domains "^news\.ycombinator\.com$"'
```

This separate pass uses the [official Hacker News API](https://github.com/HackerNews/API)
to capture story text and comments for up to `detail_max_posts` pending saved
stories. It considers snapshots collected within the configured `days` window,
prioritizing unfinished discussions before new captures. It does not recollect
listings or publisher pages. Previously capped discussions become eligible
again with the new `"all"` default; complete captures are skipped.

`comment_limit = "all"` traverses every reachable comment and reply, without a
numeric cap. `detail_timeout_seconds = "all"` removes the overall thread timeout;
each individual request still has a timeout and bounded retries. Both settings
accept positive numbers if a bounded capture is wanted. Increasing a numeric
comment limit makes earlier capped captures eligible again.

Requests use bounded concurrency and shared per-host limits. A failed request
leaves an explicit partial capture, preserves previously saved comments, and
does not prevent other branches from completing. Replies retain their parents
and API order; deleted/dead comments are omitted from display. Counts can change
after collection because these are snapshots of a live discussion.

Recent days come from the homepage and older days from hckrnews’ daily archive.
Unavailable or malformed days are reported as collection failures while
successful days remain saved. See [known issues](../known-issues.md).
