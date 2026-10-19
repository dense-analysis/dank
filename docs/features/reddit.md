# Reddit communities

DANK collects community listings first and saves each post immediately, including
available body text, media and current counts. Normal scrapes reuse suitable
saved details and defer individual thread visits so one community's discussion
archive does not delay collecting the others.

A separate `--reddit-details` pass refreshes pending post bodies and discussions.
It reads public HTML in the existing browser session, including embedded comment
templates and bounded top-level comment pages. A rendered post visit remains
available when a document cannot provide a usable capture. Access restrictions
remain visible as failures.

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
request_interval_seconds = 2.0
detail_cache_hours = 24.0
max_posts = 100
max_scrolls = 20
scroll_pause_seconds = 1.5
stall_scrolls = 3
capture_comments = true
comment_limit = 50
comment_sort = "best"
download_videos = false
collect_linked_articles = false
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

The separate detail pass captures comments in **Best** order by default, with up to **50 comments including replies**
per post. `comment_limit` accepts 1–500. `comment_sort` accepts `best`, `top`,
`new`, `controversial`, `old`, or `qa`; Reddit supplies that ordering. Set
`capture_comments = false` to refresh post bodies without discussions.
These settings affect future captures. They do not remove existing archives.

Videos default to remote playback: DANK keeps the video link and available
poster without downloading the video. To archive Reddit videos, set
`download_videos = true` and include `"video"` in `[media].download_types`.
Both settings must allow it. Native Reddit streams are assembled with their
audio using yt-dlp; media size limits still apply. Remote links depend on
Reddit remaining available and can expire. Existing [media controls](media.md)
also apply to images and other media.

## Linked publisher articles

Set `[reddit].collect_linked_articles = true` to save the publisher pages linked
by Reddit link posts. It defaults to `false`; disabling it stops future publisher
fetches without removing saved articles or their excerpts.

DANK saves the Reddit listings first, then collects the external pages with
bounded requests and the existing publisher recovery/media settings. A readable
saved publisher article is reused; an empty saved article can be repaired under
its existing identity. Repeated destinations are fetched once per run. Reddit,
social/video platform destinations and self-posts are excluded. Post/scroll
limits still bound how many Reddit links are discovered.

Publisher articles remain separate from their Reddit discussions. New captures
record the Reddit community, referring post and its collection time as their
collection origin. Reusing an article does not replace its original origin.
Collection times are observations by your DANK instance, not proof of when a
community first published or noticed a story. Blocked, paywalled and unsupported
pages can remain unavailable; existing saved content remains usable.

To fetch publisher pages for already saved Reddit link posts, without revisiting
Reddit communities or discussions:

```sh
make scrape MODE=native ARGS='--headless --reddit-linked-articles'
make process MODE=native ARGS='--age 24h --domains "^reddit\.com$"'
```

This mode also requires `collect_linked_articles = true`. It considers the latest
saved link-post snapshots in configured communities, up to `max_posts` per
community, and applies the same reuse and destination rules. It does not create
new Reddit observations. Processing Reddit also prepares the publisher pages
collected through it, so their excerpts and saved articles become available.

## Run

Native collection needs a Chromium-based browser, like X collection:

```sh
make scrape MODE=native ARGS='--headless --domains "^reddit\.com$"'
make process MODE=native ARGS='--age 24h --domains "^reddit\.com$"'
```

To refresh deferred details separately:

```sh
make scrape MODE=native ARGS='--headless --reddit-details'
make process MODE=native ARGS='--age 24h --domains "^reddit\.com$"'
```

`--reddit-details` selects only configured Reddit communities and their pending
snapshots saved in the last 24 hours, up to `max_posts` per community. Successful
latest captures are skipped. It does not recrawl the community listings and
cannot be combined with RSS retries or targeted X refreshes.

For Docker, omit `MODE=native`. Docker already includes Chromium and runs
collection headlessly. Reddit uses its own browser and persistent profile at
`<storage.data_dir>/reddit-browser-profile`, independently of X's saved login.
An X scrape waiting on its browser does not hold Reddit's browser lock.
Each listing post, completed detail capture or cache hit is saved before
advancing collection or waiting for media downloads. A later community failure does not discard saved posts.
An inaccessible community, login requirement, blocked page or collection error
is logged and counted as a fetch failure; DANK continues with the next community.
Successful captures alongside failures produce a partial source/run outcome.
A recognized empty listing is normal and does not count as a fetch failure;
an unrecognized blank page is still reported as a failure.

## Community total estimates

Normal collection also looks up each configured community in [Arctic Shift](https://github.com/ArthurHeitmann/arctic_shift/tree/master/api#subreddits).
DANK saves the archive's post count, its `num_posts_updated_at` timestamp, the
retrieval time and original JSON response. The reader shows collected posts out
of this approximate total; hover it for provenance and both dates. No lookup runs
when browsing or during the separate detail pass. Media download settings do not
affect this metadata.

This is an archive count, not an authoritative total of posts currently visible
on Reddit. Missing records, archive lag and deleted or removed posts can cause
differences. An unavailable or invalid lookup is logged and counted in the run's
fetch/parse failures; listing collection continues and the last good estimate
stays available. Without a valid saved estimate, only the collected count appears.

## Pacing and saved details

DANK spaces Reddit document requests, navigation and pagination by at least
`request_interval_seconds` (default **2 seconds**, allowed 1–60). A 429 or
network-security block defers further Reddit requests to a later run.
This is a conservative DANK setting, not a published Reddit quota. The
clock is shared across Reddit sources in a run. Browser subrequests still
belong to Reddit's pages, so spacing cannot guarantee access or eliminate 429s.
Normal collection makes no individual thread requests. During a detail pass,
most uncached posts use one document request, plus comment pages only when
needed to approach the configured limit. The first post initializes the detail
tab normally; later document requests avoid loading its scripts, styles and
other page resources. Logs identify each detail and report reused, fetched
and deferred counts for every listing batch.

Successful details are reused for up to `detail_cache_hours` (default **24 hours**,
allowed 0–168; zero disables reuse). Reuse requires matching visible post
content, media identity and comment count, plus compatible comment sort/limit.
Scores and ranking can change without forcing another detail visit. Listing
metadata is still saved on each run. Expired, changed or failed details are marked for the separate refresh pass. The cache reads the existing database in both
native and Docker mode, and keeps the original capture timestamps across reuse.
Edits to comments or content not exposed by the listing may remain unseen until
the cache expires. Lower the age or set it to zero when a fresh discussion matters.

After a comments JSON endpoint denies access with 401/403, DANK uses rendered
threads for the remainder of that run instead of making another denied JSON
request for every post. A new run may try that endpoint again.

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

The listing tab is activated before navigation and each scroll, and the detail
tab is activated before each post visit. Reddit's next-page loader can remain
idle in a background tab; restoring the listing after detail visits lets
pagination continue instead of stopping at the initial few cards.

HTTP failures from listings, pagination and thread requests retain their status
instead of being reported as empty pages. A 429 or explicit network-security
block stops further Reddit requests for the current run, including later
Reddit source entries. Other sources continue, saved posts remain available,
and queued media is drained. The Reddit result is partial when records were
saved, otherwise failed. Network-security blocks are recorded separately from
429s. Shared host cooldowns are still respected; waits longer than 30 seconds
are deferred without early requests. Blank or unavailable first pages
(including Reddit's HTTP-200 community-not-found dialog) get one retry after ten
seconds. Private, banned, login and verification restrictions remain failures.
A header or icon alone cannot prove an empty listing; only an explicit empty
state is accepted. Ordinary community-specific failures remain visible and
do not prevent collection of other communities.

If a detail visit is rate limited or access-blocked, the remaining details are
deferred to a later run. Their listing posts are still saved, with an explicit
rate-limit or access-block reason in capture metadata, instead of issuing a
request per post against the same restriction. Cooldown waiting has a separate budget from
reading a detail page.

Each document request has a 10-second fetch timeout and a 15-second browser
command timeout. A rendered fallback has a 35-second page-read budget plus
intentional pacing waits for at most one JSON request and five comment scrolls. A blocked or timed-out detail page keeps the listing post.
Comments can be fewer than the configured limit when Reddit returns collapsed
threads, unavailable replies, or stops loading; DANK makes at most five
additional scroll attempts, stopping when the top-level load-more control is
gone, and reports that more discussion is available.
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

A deferred or failed detail refresh keeps the last successful body and media.
Deferred, failed or disabled comment refreshes retain an existing archived discussion with its
original capture date and the latest attempt state. The reader distinguishes
a deferred refresh from a failed request or disabled comment collection. A successful empty
capture replaces an older discussion. Historical posts without a discussion
archive remain clearly marked as not captured.

The scraper follows Reddit's current page markup, so site changes may require
an update. Original JSON or post/comment HTML is retained with every raw
snapshot for diagnosis and reprocessing.
