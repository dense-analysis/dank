# Reddit communities

DANK collects posts from community listing pages with a Chromium browser.
It reads the page's network responses and rendered posts while scrolling,
then visits each discovered post for its full body, media and discussion.
Post details use the same browser session: captured network responses, a
bounded comments JSON request where Reddit permits it, and rendered comments
as a fallback. Access restrictions remain visible as capture failures.

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
capture_comments = true
comment_limit = 50
comment_sort = "best"
download_videos = false
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

Comments default to **Best**, with up to **50 comments including replies**
per post. `comment_limit` accepts 1–500. `comment_sort` accepts `best`, `top`,
`new`, `controversial`, `old`, or `qa`; Reddit supplies that ordering. Set
`capture_comments = false` to collect the post without its discussion.
These settings affect future captures. They do not remove existing archives.

Videos default to remote playback: DANK keeps the video link and available
poster without downloading the video. To archive Reddit videos, set
`download_videos = true` and include `"video"` in `[media].download_types`.
Both settings must allow it. Native Reddit streams are assembled with their
audio using yt-dlp; media size limits still apply. Remote links depend on
Reddit remaining available and can expire. Existing [media controls](media.md)
also apply to images and other media.

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

Each detail visit has a 35-second budget and shares the browser session's
request pacing. A blocked or timed-out detail page keeps the listing post.
Comments can be fewer than the configured limit when Reddit returns collapsed
threads, unavailable replies, or stops loading; DANK makes at most five
additional scroll attempts and reports that more discussion is available.
It does not expand every hidden reply. Raw post and comment payloads are
retained for reprocessing.

## Read

When Reddit communities are configured, the reader shows a collapsible
**Reddit** section below **All stories**, with a link for each community,
including communities with no collected posts yet. Reddit stays separate from
All stories. Search and reader filters operate on the selected collection.
A community view keeps its name and icon in a compact sticky header, labels
search with that community, and omits repeated community identity from cards.
Scores use an up arrow and number.

Community icons are discovered automatically for every configured community
on each scrape and cached locally when image downloads are enabled. Adding a
new `r/community` needs no icon setting. The sidebar, community header and article header
use the cached icon; cards also show it in the combined Reddit view; a missing icon shows the community's initial. A failed
refresh keeps the last usable icon and can retry on the next scrape.

Under **Filters → Flair**, choose labels found on your collected posts.
A community view offers only that community's labels. The overall Reddit view
combines identical labels across communities. Multiple selected labels match
any of those flairs and combine with the other filters. Counts describe the
collected posts, not Reddit's full catalogue of available flairs. Search,
article links and Back navigation preserve the selected labels. Click a flair
badge on a post or article to open that community’s feed with just that flair
selected.

Posts include their title, author, full available body, playable video,
outbound links and captured score/comment counts. The discussion keeps reply
nesting and Reddit's captured ordering. A footer links to Reddit when more
comments exist. Older posts and captures made with comments disabled explain
that comments were not captured; failed collection is shown separately.
Counts and discussions are snapshots, not live Reddit values.

Remote media has a small notice; **Settings** can hide that notice without
changing playback or download policy. Downloaded videos play locally when
available. Reader date ordering sorts collected posts locally and does not
change the configured Reddit collection sort.

A failed detail refresh keeps the last successful body and media. Failed or
disabled comment refreshes retain an existing archived discussion with its
original capture date and the latest attempt state. A successful empty
capture replaces an older discussion. Historical posts without a discussion
archive remain clearly marked as not captured.

The scraper follows Reddit's current page markup, so site changes may require
an update. Original JSON or post/comment HTML is retained with every raw
snapshot for diagnosis and reprocessing.
