# Reddit communities

DANK collects posts from community listing pages with a Chromium browser.
It reads the page's network responses and rendered posts while scrolling,
without using RSS feeds or requesting Reddit's public JSON API. Comments and
individual post pages are not collected.

## Configure

Add Reddit alongside your existing sources in private `config.toml`:

```toml
sources = [
  "blog.codinghorror.com",
  { domain = "https://www.reddit.com", feed_urls = [
    "r/LocalLLaMA",
    "https://www.reddit.com/r/accelerate/"
  ] }
]

[reddit]
max_posts = 100
max_scrolls = 20
scroll_pause_seconds = 1.5
stall_scrolls = 3
```

`reddit.com` and `www.reddit.com` also work as source domains. Community
shorthand and full community URLs are equivalent; duplicates within a source
are removed without regard to case. Use community URLs without sort paths or
query strings. Collection defaults to **Top, past day**, suitable for a daily
scrape. Each source can override `reddit_sort` with `best`, `hot`, `new`,
`top`, or `rising`. For example, to use Best instead:

```toml
sources = [
  { domain = "reddit.com", feed_urls = ["r/LocalLLaMA"], reddit_sort = "best" }
]
```

`reddit_time_filter` defaults to `day` and accepts `hour`, `day`, `week`,
`month`, `year`, or `all`. It applies only to Top. To give communities different
sorts, use separate source tables. Best uses Reddit's ranking without a daily
window; Top + day selects its highest-ranked posts from the past day.

## Run

Native collection needs a Chromium-based browser, like X collection:

```sh
make scrape MODE=native ARGS='--headless --domains "^reddit\.com$"'
make process MODE=native ARGS='--age 24h --domains "^reddit\.com$"'
```

For Docker, omit `MODE=native`. Docker already includes Chromium and runs
collection headlessly. The shared browser profile persists between runs.
An inaccessible community, login requirement, or blocked page is reported as
a collection failure; DANK does not bypass those checks.

## Collection limits

Limits apply per community per run. DANK keeps unique Reddit post IDs and stops
at `max_posts`, `max_scrolls`, or `stall_scrolls` consecutive attempts without
new posts. `max_scrolls = 0` collects the initial page only. Ads and posts from
other communities are excluded. Repeated runs update the same post identity
while retaining raw collection snapshots.

Ranked listings are not chronological. An old pinned post does not stop
collection, and scrolling cannot prove that every worthwhile post was found.
Top + day asks Reddit for its daily ranking; the limits bound how much of that
listing DANK captures. Run collection regularly to build your local history.
Progress logs report each community and why collection stopped.

## Read

When Reddit communities are configured, the reader shows a collapsible
**Reddit** section below **All stories**, with a link for each community,
including communities with no collected posts yet. Reddit stays separate from
All stories. Search and reader filters operate on the selected collection.

Community icons are discovered automatically for every configured community
on each scrape and cached locally when image downloads are enabled. Adding a
new `r/community` needs no icon setting. The sidebar, cards and article header
use the cached icon; a missing icon shows the community's initial. A failed
refresh keeps the last usable icon and can retry on the next scrape.

Under **Filters → Flair**, choose labels found on your collected posts.
A community view offers only that community's labels. The overall Reddit view
combines identical labels across communities. Multiple selected labels match
any of those flairs and combine with the other filters. Counts describe the
collected posts, not Reddit's full catalogue of available flairs. Search,
article links and Back navigation preserve the selected labels.

Posts include their title, author, available body and media, outbound link,
and captured score/comment counts where present. A comment count is metadata,
not a downloaded discussion. Listing pages can truncate post bodies or omit
media; the reader only has what the listing supplied. Its date ordering sorts
collected posts locally and does not change the configured Reddit collection
sort. Counts are snapshots, not live Reddit values.

The scraper follows Reddit's current page markup, so site changes may require
an update. Original JSON or post HTML is retained with every raw snapshot for
diagnosis and reprocessing. Existing [media controls](media.md) apply.
