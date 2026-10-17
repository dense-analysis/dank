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
comment_limit = 50
detail_max_posts = 20
detail_timeout_seconds = 60
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

## Collect

```sh
make scrape MODE=native ARGS='--domains "^news\.ycombinator\.com$"'
make process MODE=native ARGS='--age 24h --domains "^news\.ycombinator\.com$"'
```

Omit `MODE=native` for Docker. Normal collection saves titles, outbound links,
authors, points, comment counts and original source records. It does not fetch
comments or archive the external articles. Open the original article from the
reader, or open its saved Hacker News discussion view.

## Detailed scrape

```sh
make scrape MODE=native ARGS='--hacker-news-details'
make process MODE=native ARGS='--age 24h --domains "^news\.ycombinator\.com$"'
```

This separate pass uses the [official Hacker News API](https://github.com/HackerNews/API)
to capture story text and comments for up to `detail_max_posts` pending saved
stories. It considers snapshots collected within the configured `days` window,
newest stories first. It does not visit external article pages or recollect the
listing. Completed and bounded partial discussions are skipped on subsequent
passes; failed captures remain eligible.

Each discussion is bounded by `comment_limit` and `detail_timeout_seconds`.
Replies retain their parents and API order. Deleted or dead comments are not
shown. The reader distinguishes comments not collected, complete snapshots,
partial snapshots and failures, and links to the live discussion. Previously
saved comments remain available after a normal collection or failed retry;
these are snapshots, not live discussions.

Recent days come from the homepage and older days from hckrnews’ daily archive.
Unavailable or malformed days are reported as collection failures while
successful days remain saved. See [known issues](../known-issues.md).
