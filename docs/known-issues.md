# Known collection and processing issues

Use this register to understand collection-quality problems, the handling
already in place, and the gaps that remain. Detailed commands live in the
linked feature guides; live outcomes live in [run history and logs](features/history.md).

Last reviewed: **2026-10-17**, against the implementation on `main`.
Publisher examples describe observed captures, not a claim that a site always fails. “Mitigated”
means a fallback exists; “handled” means the specific case has a tested fix.

## Blocked Reddit collection and cancellation

The 2026-10-19 run `c01507f1` saved 3,044 posts, but spent its last 33 minutes
waiting on repeated Reddit network-security blocks after the other 56 sources
finished. Five-minute cooldowns and a fresh retry for each community compounded
the delay. Ctrl+C reached cleanup, but browser-driver shutdown did not finish.

An explicit Reddit network-security block or HTTP 429 now defers further Reddit
requests for the current run, including later Reddit source entries. Saved posts
are retained and queued media can finish; other sources and final enrichment
continue. The source remains partial or failed. Other shared cooldowns have a
30-second wait ceiling. An unusable initial listing still gets one ten-second
retry; ordinary community restrictions do not stop unrelated communities.
This bounds the failure without bypassing Reddit's access restrictions.

Browser shutdown bounds the graceful close, transport close and driver cleanup
to five seconds each, with a final kill of the owned process if necessary.
Cancellation never enters the native browser's keep-open debugging mode.
Final run history is written after cleanup; already interrupted historical runs
can still show `running` when their final row was never saved. See
[Reddit recovery tests](../tests/scrape/test_reddit_recovery.py) and
[browser shutdown tests](../tests/scrape/test_browser_session.py).

Controlled Docker verification on 2026-10-19 used real Chromium and an isolated
ClickHouse database with simulated sources. The blocked run finished partial
in 1.0 seconds with both saved posts retained and the other source completed.
Real Ctrl+C removed both normal and deliberately stalled-browser containers;
both runs recorded `cancelled`, preserving their two saved posts.

## Linked collection and discovery timing

Optional Reddit publisher collection is controlled by
`[reddit].collect_linked_articles` (default `false`). It reuses readable saved
articles, saves Reddit discoveries before publisher requests and retains
explicit origin metadata for new fetches. Backfill is bounded by configured
community post limits. A failed or unsupported publisher page remains unavailable.

The Info icon opens collection details regardless of earlier references. A
separate eye requires independent discovery before the post's verified
publication time. Its tooltip includes the gap from the displayed discovery to
publication, using the graph’s duration format. Earlier collection order alone
does not qualify. Current-post
origins, tied timestamps and unknown publication/collector data do not produce
that signal. Native video playback alone also does not produce an eye.

Collection info reports first observed article/link times independently of
successful page collection, and does not infer missing legacy collector routes.
Processed-version cutoffs prevent a pending raw capture from upgrading an older
displayed article. Edited posts compare the current link's first observation,
not the post identity's original timestamp. Polling and selection bias remain;
these observations do not establish publication priority or copying.

Connections scopes referencing posts to the active source/community; All and
Blogs support broader exploration. Article-first layout is conceptual, not a
chronological or causal claim. The graph's First discovered annotation shows
saved source/time history without expanding that source's other references;
its dashed connection is distinct from hyperlink arrows and reference counts.
The current social post shows verified publication and its gap from discovery,
using neither post collection nor article download time as a publication fallback.
Before/after, exact ties and unavailable timing remain explicit. See [feature details and future analysis](
features/collection-provenance.md), [collection tests](
../tests/scrape/test_reddit_linked_articles.py), and [provenance tests](
../tests/web/test_reader_provenance.py).

## Blog source icons

The reader captures a source favicon when its sidebar icon first loads, without
collecting articles. It checks the source homepage and a bounded set of declared
raster icons, then `/favicon.ico`; copies are resized and saved locally as PNG.
Unknown sources and private network destinations are rejected. Up to three
captures run at once, with a 15-second budget per source and image size limits.

Successful icons are reused for 30 days. Failures retry after six hours while
the web app remains running, and a failed refresh preserves an older good icon.
Sources offering only SVG icons, blocked pages or unsupported images keep the
reader fallback. See [source icon tests](../tests/web/test_reader_source_icons.py).

## Publisher excerpts across feeds

Reddit link posts with only a URL and preview image now use a matching saved
publisher excerpt. The same fallback applies to empty RSS aggregator entries
and missing descriptions on link-only X previews. HN keeps its existing fallback;
YouTube video cards have no publisher-article destination to borrow from.

Matches use exact URL identities and observed redirects, with one batched lookup.
Own post text, captions and captured X descriptions take precedence. Multiple
unexplained links and quoted posts do not select an arbitrary article. Missing
publisher captures remain missing; browsing does not trigger collection, and
Reddit cards still open their discussions. See
[publisher excerpt tests](../tests/web/test_reader_publisher_excerpts.py) and
[cross-feed browser tests](../reader/tests/publisher-excerpts.spec.ts).

## Hacker News stories, articles and discussions

Hacker News uses hckrnews’ UTC front-page history and daily score cutoffs, with
Top 50% as the default. Cards show the actual submission time from the official
HN API (or the archive’s submission time), never a guessed front-page time.
Unavailable metadata remains explicitly unknown. Normal collection fetches story
metadata and linked publisher pages; comments require a separate detailed pass.

Optional RSS collection uses native HN discussion IDs and preserves the raw feed
entry. It can run alone or alongside the archive/API adapter. Composed snapshots
retain previously captured rankings, story bodies and comments; adapters share
publisher fetches within a source run. Network failures leave the source partial
when another adapter succeeds. RSS-only items have no inferred daily ranking or
scores and are available under **All**. RSS does not reconstruct front-page arrival
times or historical days. See [RSS tests](../tests/scrape/test_hacker_news_rss.py)
and [composition tests](../tests/scrape/test_hacker_news_runner.py).

Detailed capture now defaults to `comment_limit = "all"` with no overall thread
timeout. Item requests have a 30-second timeout, at most three attempts, bounded
concurrency, and shared host limits. Previously capped threads are retried first.
Failures preserve saved comments and remain partial; complete means every
reachable branch observed during that run was fetched, not that a growing live
thread will never acquire new comments. The API no longer truncates at 500.

Publisher pages use the existing article extraction and recovery rules. A denial,
challenge or unsupported document remains an unavailable capture. Public paywall
excerpts may remain excerpts; collection does not imply subscriber access.
Original publisher dates/bylines are extracted when present. Readable existing
original-source captures are reused, rather than overwritten by an aggregator’s title or body.

Resolved HN links open the collected destination; All shows its original source
with incoming Links. HN cards without submission content now reuse the saved
publisher excerpt; submissions with their own text keep it. This is a display
fallback and does not replace stored HN content or fetch missing articles.
Empty or unavailable publisher content still has no excerpt.
[Reader link tests](../tests/web/test_reader_links.py) and
[browser tests](../reader/tests/linked-posts.spec.ts) cover this behavior. RSS aggregator copies, including existing Lobsters records,
remain references to a separately collected publisher record. Ask/Tell HN
citations do not replace their containing post.
Known platform post IDs and observed redirects resolve aliases; ambiguous URLs
are kept separate. Links are tied to processed versions and content hashes, so
unprocessed captures and reprocessing cannot expose stale link relationships.
Malformed legacy anchor URLs are skipped without preventing startup or indexing
other links in the same post. Incoming lists show up to 50 references with the full count when more exist;
this display limit does not apply to comments.

See [configuration and collection](features/hacker-news.md),
[collector tests](../tests/scrape/test_hacker_news.py),
[article tests](../tests/scrape/test_linked_articles.py), and
[link identity tests](../tests/test_links.py).

## YouTube feed discovery

YouTube collection saves feed appearances and their original renderers, with
video downloads off by default. Shelf headings, rank and playlist identity come
from YouTube; country codes record the configured regional context and capture
times record when DANK observed the listing.
Separate discovery records preserve repeated appearances across feeds and runs.
Descriptions and comments remain deferred; feed snapshots are separate from
caption captures. Live and upcoming broadcasts are excluded.

The browser follows Hype's regional **Top 100 → View all** link, expands News
sections and uses bounded scrolling. The UK playlist fallback is used only for
the GB region. Source layouts, regional availability and recommendation order
can change; a successful bounded run is not a complete archive of an infinite
feed. Unrecognized/empty or failed feeds increment failure counters and allow
later feeds to continue. Already persisted appearances remain available.
Channel latest-video listings stop after consecutive known items; this heuristic
does not apply to ranked or personalized feeds.

Signed-in homepage **Recommendations** use a 25-second navigation/listing budget,
rapid multi-screen scrolling and varied 0.3–0.65 second pauses. This pass replaces
the normal per-feed item/scroll/pause settings, with 2,000-item and 200-scroll
safeguards. A stable bottom without continuation is checked for two seconds;
a visible but unchanged continuation is allowed eight seconds without new
responses, items, page growth or loading. Logs distinguish end, stall and budget
outcomes. Guard/budget/stall exits retain their discoveries and report partial
collection; a completed pass requires an observed end. Slow YouTube responses
can still leave recommendations undiscovered; there is no claim that a timed
pass exhausts every homepage.

Whole news/Playables subtrees are excluded in initial data and continuation
responses, and DOM fallback retains all shelf ancestors and destination context.
This prevents nested Shorts or repeated DOM cards from leaking out of excluded
shelves. Actual recommended news/gaming videos outside those shelves remain
eligible, and News feeds keep their existing coverage. Unknown future renderer
shapes or changed shelf names can require parser updates. Buffered discoveries
survive a listing deadline or later HTTP failure; an empty result remains a
reported failure. Storage, thumbnails and caption enrichment happen after this
listing budget. [Recommendation regressions](../tests/scrape/test_youtube_recommendations.py)
cover exclusions, item kinds, partial captures, bounded navigation, end/stall
handling and unchanged News collection; live account depth remains a separate
operational check.

Google sign-in uses one 1Password item read per attempt and generates any
requested authenticator code in memory. A successful callback is insufficient:
login must authenticate a clean homepage and a new browser instance. A refused
vault request stops the attempt; collection never falls back to signed-out
recommendations. Native profile cookies are checked first. Only a complete,
definitively signed-out homepage permits one cache restore per browser session;
a fresh document must then authenticate. Loading or uncertain states trigger
neither cookie injection nor vault access. Authentication failure stays local
to YouTube collection. The private cache is refreshed only after verification,
with page scripts stopped; invalid/expired caches preserve the saved file.
See [session cache tests](../tests/test_youtube_session.py)
and [login checks](../tests/scrape/test_youtube_login.py). Live restart and paired
collection verification remain necessary before treating an account as ready.

The paired comparison keeps yt-dlp's flat results separate: shelf context and
text-only posts are unavailable, and Shorts shelves may be omitted. Reports
include raw/unique category totals, partial outcomes, start skew and video-ID
overlap. A matched cookie snapshot and close starts reduce timing differences
but cannot force YouTube to return identical recommendations. See
[comparison tests](../tests/scrape/test_youtube_comparison.py).

- [ ] **P2: Verify regional Hype playlist freshness across days.** The configured
  US playlist was supplied after viewing Hype through a US VPN on 2026-10-17.
  A same-day public yt-dlp metadata check returned **United States's top hyped
  videos** with 100 items. This verifies its current identity, not future
  freshness. DANK follows it directly while GB still discovers **View all**.
  [YouTube documents](https://support.google.com/youtube/answer/15334500?hl=en)
  country-specific rankings updated about every few minutes, but does not
  promise stable playlist URLs or cross-country selection. No daily-only update
  schedule is assumed. Compare each region's saved playlist IDs, ordered video
  IDs/ranks and capture times over several days, including a weekly boundary.
  Unchanged captures warrant checking against that country's live leaderboard;
  they do not alone prove staleness. If the URL rotates or the playlist freezes,
  implement regional discovery using a verified country-specific session/network
  path. There is no automatic stale verdict or region-switch guarantee yet.
  [Configuration tests](../tests/test_youtube_config.py) and
  [collection tests](../tests/scrape/test_youtube_collection.py) cover distinct
  regions, direct playlist selection and retained snapshot context.

Signed-out runs use a temporary profile; optional signed-in runs use YouTube's
own saved profile and require a valid session. Neither shares X's or Reddit's
browser. The reader's **From your feeds** index recognizes explicit YouTube
links in saved source data. It does not resolve opaque shorteners such as `t.co`
unless the original capture includes the expanded destination.

See [YouTube settings](features/youtube.md),
[runner regressions](../tests/scrape/test_youtube_runner.py) and
[session regressions](../tests/scrape/test_youtube_login.py).

**Live verification, 2026-10-17:** signed-out Docker run
`08b7d6c0-c6a1-4dcb-af6b-5db8eff0716b` saved 715 appearances / 602 unique items
from UK Hype, the three default News tabs, and Channel 4 News videos/Shorts.
All thumbnail references were saved with zero failures; video downloads remained
off. Repeat run `5d17dbdc-0617-4afc-ba00-356500ecf303` stopped at Hype's 100 items
without scrolling and at five known channel videos, completing in 20.2 seconds.
Reader checks reconciled all 100 Hype ranks across pages and channel-wide counts
across feeds. The existing collection supplied 430 distinct linked videos.

### Attribution of videos linked from other feeds

The reader uses a separate evidence index rebuilt from saved post bodies,
explicit article scopes, and identifiable comments. Each occurrence retains its
excerpt, matched destination, capture time and post/comment permalink. Multiple
captures collapse by video and occurrence identity; older mentions survive a
later capture that omits comments. Counts, search and pagination use this same
index. Missing YouTube titles use a neutral video-ID label until public YouTube
preview metadata is collected; source headlines are never promoted to titles.

The previous index searched entire payloads and attributed every match to the
parent post. This included navigation, recommendations, quoted X posts, Reddit
crossposts and parent articles archived alongside comment-feed entries. These
are not accepted as direct links from the collected post. Identified RSS-page
comments remain comment mentions; unknown page layouts or comments without
stable identities are conservatively omitted. This verifies archived evidence,
not whether an external link is still available or a comment is still public.

Startup backfills the evidence index in bounded batches before serving the
reader; an interrupted backfill can retry without duplicating references. New
captures and processed articles add evidence when persisted. Publisher pages
collected from aggregator links (`linked_article`) use the same article/comment
attribution rules as RSS captures. Existing version-2 indexes backfill only those
publisher records on upgrade; fresh indexes still scan all supported sources.
The old index and original source archive are retained for comparison. A read-only audit is
available with `uv run python -m dank.web.reader_youtube_index --output audit.json`
(add `--config` when needed). It reports verified/excluded legacy associations,
their source excerpts, and the accepted evidence.

**Archive audit, 2026-10-17:** of 485 legacy video-to-post associations, 465
were supported by attributable content and 20 were excluded: seven parent
articles attached to comment-feed entries, seven nested quotes/crossposts,
five navigation/recommendation links and one secondary video-description link.
The accepted archive contains 449 post links and 17 distinct comment mentions
across 437 videos. No raw source captures were removed.

Covered by [extraction tests](../tests/test_youtube_references.py),
[migration tests](../tests/web/test_reader_youtube_index.py),
[API tests](../tests/web/test_reader_youtube.py) and
[reader browser tests](../reader/tests/youtube-collection.spec.ts).

### Linked video preview metadata

Linked videos now have a canonical metadata archive separate from recommendation
feed appearances. Public YouTube oEmbed responses provide the real title, channel
attribution and thumbnail; original responses and every attempt are saved in
`youtube_video_metadata`. The collector does not download media or claim to have
collected descriptions, transcripts or comments. Channel handles do not establish
a channel ID when the provider does not return one.

Normal collection enriches up to 100 due, verified linked videos with two workers
under the shared HTTP limiter. `--youtube-metadata` runs that queue without other
collection; repeat `--youtube-video-id VIDEO_ID` to limit it to known videos.
Reader collection uses an explicit same-origin JSON POST, validates that the video
already exists in saved evidence or feed discoveries, serializes concurrent
requests, and limits request and response sizes. GET requests remain read-only.

Successes are cached for seven days. Network, malformed-response and transient
HTTP failures retry after one hour; unavailable/private responses (401, 403, 404,
410) retry after one day. These are local retry policies, not guarantees that a
video is deleted or private. The latest attempt is shown separately from last
successful capture availability. Failed refreshes preserve prior title/thumbnail
metadata; genuine selected-feed titles remain tied to their capture. The queue
is intentionally bounded; large backlogs need subsequent runs. Preview thumbnails
use the provider URL and can become unavailable independently of saved metadata.

Tests cover provider-payload retention, malicious URLs, malformed/oversized and
unavailable responses, retry caching, duplicate requests, provenance preservation
and source-title isolation in [collector tests](../tests/scrape/test_youtube_metadata.py)
and [API tests](../tests/web/test_reader_youtube_metadata.py).

### YouTube transcripts

Existing public captions are collected for up to 25 videos encountered in the
current scrape, and on first opening a known video without saved captions.
No archive-wide transcript backfill runs. Successful captions are stored with
original caption JSON, track metadata, language, origin and timing; successes
are reused indefinitely. Missing tracks wait one day, failed retrieval waits one
hour. Read-only transcript requests never contact YouTube. On-demand collection
is serialized per local web process with a bounded wait; requests from separate
scraper/server processes can still overlap.

YouTube may block caption discovery or return empty/invalid caption responses.
These are failures, not evidence that captions do not exist. Each attempt has a
40-second deadline and size limits; the extractor subprocess is terminated on
cancellation. A failed attempt stops further transcript collection in that scrape.
Only JSON3 tracks are supported, preferring English then the original language,
and creator-provided over automatic within each language. Machine translations
are excluded. Automatic captions can contain errors; saved text is not a verified
verbatim transcription. Saved captions are not yet included in semantic search.

Validation includes track choice, parsing, negative caching, current-run scoping,
read-only access, first-open collection and desktop/mobile rendering. A live
2026-10-19 check retrieved 19 creator-caption segments for `L_6pAVHYoFM`; this
establishes that sample, not universal availability. See
[collector tests](../tests/scrape/test_youtube_transcripts.py),
[API tests](../tests/web/test_reader_youtube_transcripts.py) and
[reader tests](../reader/tests/youtube-transcripts.spec.ts).

### YouTube channel identity and totals

Channel identity comes from the channel page metadata/header, never from
recommended videos. Avatar caching uses the same bounded image download and
signature checks as Reddit icons; a failed or disabled refresh preserves an
existing cached icon. Displayed channel totals can be rounded by YouTube;
missing totals remain unknown. Collected channel counts include unique videos
and Shorts across feeds, excluding community posts. Existing channels gain
this metadata on their next collection. After first-visit consent, collection
waits up to five seconds for delayed channel initial data. A public Channel 4
News check on 2026-10-17 captured its avatar and displayed `26k videos` total.
See [channel metadata tests](../tests/scrape/test_youtube_channels.py)
and [source API tests](../tests/web/test_reader_youtube_sources.py).

### Thumbnail recovery and observed versions

Title/image combination history now records the listing title and its capture
time with each fetched thumbnail. Multiple listing captures sharing an image URL
retain their own metadata. Pair dates are aggregated per image and title; visually
grouped encodings merge dates only for identical titles. Historical matches use
an exact URL in one recorded scrape, reject conflicting titles or overlapping
runs, and are labelled as matched. Fallback images and records outside a known
run remain unpaired. Read-only ClickHouse cases verify recurrence, reused URLs,
conflicting titles, fallback images and exact date ranges; tests cover capture
metadata, pair grouping and per-image UI captions.


Captured thumbnail URLs can expire or return 404 while another rendition for
that video remains available. Collection now tries bounded same-video fallback
URLs on 404, retaining the captured URL and the URL actually fetched. Failed
candidates are not stored as images; a successful fallback counts as recovery.
`--retry-youtube-thumbnails` repairs saved missing images without recrawling feeds;
repeat `--youtube-thumbnail-id VIDEO_ID` to select videos. This also adopts their
older cached files without changing the originals.

Thumbnails are re-fetched on later observations, even if their URL is unchanged.
Immutable files use content hashes; actual image formats determine extensions
(including AVIF delivered at a `.jpg` URL). A conservative perceptual comparison
groups near-identical renditions for display while retaining every original.
Small edits can be grouped by this heuristic; it does not prove two captures
came from the same experiment. The reader exposes the latest eight visual
versions from the latest 64 distinct content hashes, and 20 distinct observed
titles, with first/last capture dates. These are display limits, not deletion
rules. Old unvisited videos and unobserved A/B-test versions cannot gain history
without another capture. Known-overlap stopping still refreshes encountered
videos before ending the channel pass.

Live verification on 2026-10-17 recovered all six missing thumbnail URLs with
five `sddefault.jpg` images and one `hqdefault.jpg`. Existing captures of
`bVRiUa04bHE` yielded two distinct images and two titles while remaining one
video; Chrome verified hover crossfades without playback at desktop and mobile
widths.

Regression coverage checks 404 recovery, same-URL changed bytes, immutable
storage, resolution deduplication, dated title history and stable video identity.
Browser coverage checks hover/focus crossfades, image failures, static reduced
motion and desktop/mobile history views. See [YouTube guide](features/youtube.md),
[reader tests](../reader/tests/youtube-variants.spec.ts) and
[reader API tests](../tests/web/test_reader_youtube.py).


## Reddit community total estimates

Community headings use Arctic Shift's archived post count as an estimate, marked
**≈**, rather than claiming a complete Reddit total. Hover text identifies Arctic
Shift, its count update timestamp and DANK's retrieval time. Normal Reddit scrapes
refresh the metadata once per configured community per run, independently of media
settings; browsing and detail-only refreshes make no archive requests. Counts can
lag or differ from currently visible Reddit posts. Failed, missing or invalid
responses leave the previous estimate and dates intact and do not stop listing
collection; fetch/parse failures remain visible in run history. An older archive
update cannot replace a newer saved one. Communities without a saved estimate
keep their collected-only heading. See [collection details](features/reddit.md#community-total-estimates),
[enrichment tests](../tests/scrape/test_reddit_totals.py) and
[reader acceptance tests](../reader/tests/collections.spec.ts).

## Publisher recovery

The follow-up to run `b34b70fc-18eb-4ddf-b48c-e6d5eb0b4b73` identified several
recoverable request and media failures:

- HTTP requests now identify DANK and its project URL. The generic Python
  User-Agent was rejected by Ars Technica, a Microsoft article and Wikimedia
  image delivery. Wikimedia's [published policy](https://foundation.wikimedia.org/wiki/Policy:Wikimedia_Foundation_User-Agent_Policy)
  requires a descriptive client identifier and contact information.
- A feed explicitly linking back to its own page with **Read full article** is
  treated as an excerpt even inside `content:encoded`. When a page is available,
  the processor uses its article body; ordinary full feeds remain preferred and
  failed fetches retain their excerpt. This repairs Ars Technica's short previews
  after a successful page recovery, including articles whose canonical URL
  changed. All 68 saved Ars entries were reprocessed with this correction.
- RSS media requests carry the source page's origin as their referrer. This
  restores the `shkspr.mobi` favicon images without sending article paths,
  query strings or credentials. HTTPS origins are not sent to HTTP media.
  Extensionless WebP images are also recognized during processing, so these
  recovered icons use their local files in the reader.
- Internet Archive storage-node links are normalized to the stable
  `/download/<identifier>/<filename>` address in collection and reader HTML.
  The old `ia600209.us.archive.org` host was unreachable; the canonical URL
  works. Its 16,821,362-byte video exceeds the configured 10 MiB limit and is
  correctly skipped, rather than treated as a connection failure.
- Automatically discovered and cached WordPress comment feeds are excluded
  from article collection. Explicit `feed_urls` still take precedence. This
  removes Order Order's unrelated, rate-limited comments feed from normal runs.
- Etsy engineering articles, Netflix Tech Blog and The Register can recover
  HTTP 403 or recognized verification pages through a separate RSS browser.
  Recovery preserves the browser's document response and final URL, records
  `page_fetch_method = "browser"`, and counts failures only when recovery fails.
  Each attempt has a 20-second budget and at most one retry. Repeated failures
  suspend browser recovery for that host for the rest of the run. HTTP 429
  retries retain their existing pacing; subscriber pages and other hosts do
  not receive this fallback. X, Reddit and YouTube browsers remain independent.

Real access problems remain visible: Niche Gamer's feed still presents a
Cloudflare verification screen after a bounded browser check; some Etsy pages
request interactive verification; LWN explicitly requires a subscription for
restricted articles. Two diagrams in Microsoft's MSTest article return 404.
The `stormatics.tech` / `dbadevops.com` destinations initially closed connections
in HTTP and Chromium; both original article URLs recovered on the final rerun.
No verification screen, subscription notice or missing-file response is
saved as a successful article or silently reclassified as an intentional skip.

See [recovery regressions](../tests/scrape/test_publisher_recovery.py) and
[browser recovery](../src/dank/scrape/page_recovery.py).

**Live verification, 2026-10-17:** rerun
`3bb45aab-d1bd-4799-ad0a-3ee0dce6ae4a` selected all 13 affected source entries:
nine completed, three remained partial, and Niche Gamer remained blocked.
It retained 202 post captures; media failures fell from 12 to the two missing
Microsoft diagrams. The original failed-page set contained 70 article URLs;
44 recovered, while Etsy's 20 and LWN's six remained inaccessible. A separate
saved-page retry (`16a5ec3f-c06c-49c4-b090-b04b1317624e`) recovered 81 older
captures outside the current feeds, leaving five older Register captures
incomplete after bounded recovery. Original payloads remain available.

## Latest full-scrape check

On **2026-10-17**, Docker run `19bb2610-7163-4591-ba27-f409be8eaa69`
used `851e95a` and the existing 55-source configuration. It exited normally after
**6m25s**, with outcome **partial**: 43 sources completed, 10 were partial, and
Niche Gamer and The Register failed. X and Reddit both completed. The archived
log is `logs/scrape/2026-10-17T14-12-27Z-58404.log`.

All **2,456 post captures plus 25 ID-only references** were saved: 2,481 raw
rows, independently reconciled against run/source counters and account logs.
The post captures represent **2,440 distinct posts**, not necessarily new posts.
References remain in raw storage and do not inflate the post totals. Of their
25 IDs, 23 also have full content in this run; two are reference-only.
All 2,440 distinct posts and all 24 available YouTube files were processed and
independently checked against their latest captures; the reader remained available.

| Source | Post captures | Distinct posts | ID-only references | Result |
| --- | ---: | ---: | ---: | --- |
| RSS | 806 | 806 | 0 | Some feeds, articles and media remained unavailable. |
| X | 1,063 | 1,047 | 25 | All 36 account timelines completed; one browser timeout recovered on retry. |
| Reddit | 587 | 587 | 0 | All 15 communities completed, including 27 LLMDevs posts; two were explicitly empty. |

X again timed out for 30s at account 21, `lexfridman`. Restarting with the saved
session collected 32 posts on retry and all later accounts completed. Reddit
took **3m01s**; DomainDrivenDesign and ExperiencedDevs had explicit empty Top/day
listings, while other communities yielded 1–100 posts. Both browser sources had
zero final fetch, parse, rate-limit or media failures. Normal runs deferred
individual discussion refreshes as intended; X saved no conversation snapshots.

YouTube retained **24 available MP4s from 52 references**; the other **28 were
size-excluded**, with no unexplained missing files or download failures. All 24
files were cached from earlier runs. FFprobe confirmed audio/video streams and
the unchanged 10 MiB cap for every file (largest: 10,229,642 bytes); FFmpeg fully
decoded both streams of the three largest files without errors. One extractor
warning reported unavailable formats for `EEUdslTwYZ8`; the final outcome was an
explicit no-fitting-format size skip. This pass verified cache reuse, size
classification and existing files; the fresh Docker download/playback evidence
is recorded below.

Overall media: **51 new, 3,515 cached, 582 skipped, 11 failed**. The same eleven
publisher-media failures recurred on Wikimedia, Microsoft, `shkspr.mobi` and
Internet Archive. All **63 fetch failures** were RSS-related. The four HTTP 429
responses were attempts on one Order Order article. The Register's feed now
returned a verification page, preventing collection of its articles; Niche
Gamer's feeds remained blocked by 403 responses. Publisher recovery, smaller X
video variants, browser progress and bounded media jobs remain on the task list below.

## Full-scrape improvement tasks

These follow-ups came from run `3024d25d-d50e-4cde-be87-4b8e70224e22`
on 2026-10-17 (Europe/London), using commit `0e0de9a`. Its archived log is
`logs/scrape/2026-10-16T23-41-08Z-50060.log` (filenames use UTC).
The run used all 55 configured sources, 36 X accounts and 15 Reddit communities.
It exited with `RedditScrapeError` after **6m39s**, with **1,420 raw records saved**.
The saved rows were independently counted in ClickHouse within the run's time
window; these are captures, not necessarily previously unseen posts.

| Source | Saved records | Distinct post IDs | Outcome |
| --- | ---: | ---: | --- |
| RSS | 826 | 826 | Some feeds/articles/media failed; retained records remain available. |
| X | 594 | 581 | Partial: the 21st account timed out; 15 were not attempted. |
| Reddit | 0 | 0 | Failed on the 12th community; 32 queued posts were never flushed. |

The source outcomes were 27 completed, 26 partial and 2 failed. The browser
timeout prevented an indefinite X wait, and Reddit ran independently, but
this run did **not** establish reliable collection of either browser source.
Media counters reported 389 new files, 2,508 cached, 154 failed and 183
intentionally skipped. All 55 saved YouTube references lacked a local file;
logs contained errors for 53 distinct YouTube IDs. Size-limit rejections
produced 91 warnings for 85 distinct URLs and are included in failure counts.

- [x] **P1: Save small batches before a later source error can discard them.**
  Reddit queued 32 posts but saved none: both post and media buffers stayed below
  the 50-record flush threshold when a later community raised an exception.
  Post batches are now committed before discovery advances or waits on the
  media queue. Media retains bounded batching; database write failures still
  stop collection. Regressions retain all 32 posts after later failure or
  cancellation and preserve committed posts while media is blocked or fails.
  [Persistence](../src/dank/scrape/runner.py),
  [regressions](../tests/scrape/test_scrape_history.py).
- [x] **P1: Isolate empty or failed Reddit communities.** A zero-post result for
  `r/DomainDrivenDesign` raised `RedditScrapeError`, failed the entire run and
  prevented the final three communities from being attempted. A live check
  confirmed Reddit's explicit empty-listing message there. That message now
  ends the community normally; blocked or unrecognized listings and other
  community errors increment fetch failures, log their community/error type,
  and allow later communities to run. Saved records plus errors produce a
  partial outcome. Cancellation still propagates. Regression coverage includes
  empty, blocked and broken communities followed by successful collection.
  [Collector](../src/dank/scrape/reddit/__init__.py),
  [empty/access tests](../tests/scrape/test_reddit.py).

  **Live verification, 2026-10-17:** Reddit-only Docker run
  `da22432f-1c25-48e2-beb1-8a19b0baab7b` attempted all 15 communities and
  exited normally in 5m28s. All 30 collected posts were saved (30 distinct IDs),
  with detail/comment capture recorded. The first three rows were verified in
  ClickHouse while the run was still active. Five listings, including
  `DomainDrivenDesign`, were reported as unrecognized during this run and counted
  as failures; a later audit found HTTP 429 responses for all five in the network
  log. The source/run outcome was partial, with no lost completed
  batches. This verifies persistence and continuation, not complete Reddit
  coverage. The empty-state recognizer is regression-tested against the message
  observed in the standalone listing check. Pagination remains open below.
- [x] **P1: Collect X timelines before refreshing discussions.** The full check
  `8d54ed62` spent about 55s refreshing five discussions after its first 10s
  timeline. Normal scrapes now collect timelines only. `--x-details` separately
  refreshes recent saved pending posts, bounded by the existing per-account
  conversation limit; `--x-post` still revisits explicit saved posts. The reader
  retains earlier archives when the latest timeline has no discussion snapshot.
  [Workflow](features/x.md#conversation-snapshots),
  [CLI and selection tests](../tests/scrape/test_x_conversation_cli.py).
- [x] **P1: Recover X collection after a browser timeout.** The earlier run
  timed out on four `claudeai` discussions, then on navigation to the fifth
  account, leaving 31 accounts unattempted. The collector now closes the owned
  browser and retries a timed-out/disconnected account once with the saved
  session, filters already-saved records from retries, and continues after a
  second failure. Discussion timeouts restart the browser before the next post.
  Per-account outcomes distinguish completed, partial and failed attempts;
  cleanup no longer logs false success. Tests force failures during startup,
  navigation, inspection, scrolling and response drains, and cover deduplication,
  later-account continuation and cancellation. Sign-in/profile-lock failures and
  a browser that cannot close safely still stop X; no automatic sign-in occurs.
  The original browser failure remains unexplained.
  [Recovery regressions](../tests/scrape/test_x_recovery.py).


  **Docker verification, 2026-10-17:** run
  `03ccaaa1-29db-448f-8041-0d6cf4ba1423` completed all **36 account timelines** in
  **6m23s**, saving **1,065 records / 1,049 distinct raw IDs**. Processing made
  1,046 posts readable; the three remaining IDs are content-free references
  described below. Earlier discussion archives were retained with their original
  capture times. Account 21,
  `lexfridman`, genuinely timed out after 30s; the browser restarted with the saved
  session and its one retry collected 32 posts. All later accounts completed.
  ClickHouse confirms zero final fetch/parse failures, zero media failures,
  zero 429s and one retry. No discussion snapshots or visits occurred during the
  timeline pass. Unit tests additionally cover mid-account retries without
  duplicate posts/assets, persistent failures followed by healthy accounts,
  bounded cleanup and login/cancellation behavior. A separate bounded refresh
  (`9061e460`) selected and saved two discussions in 24.8s without any timeline
  visits: one captured, one partial with 22 replies/context and a remaining cursor.
- [ ] **P2: Diagnose the recurring X browser timeout.** Both the X-only check
  `03ccaaa1` and full run `19bb2610` timed out after 30s at account 21,
  `lexfridman`. Restarting the browser with the saved session collected 32 posts
  on retry in both runs and allowed later accounts to continue. Recovery is
  working; the logs do not distinguish an account-specific problem from browser
  lifetime or navigation/transport failure. Capture the failed browser stage and
  compare the account in a fresh browser and a different position in the pass.
  Done when the trigger is reproduced and addressed without weakening deadlines
  or hiding failed attempts.
- [x] **P2: Distinguish ID-only X references from readable posts.** The three
  reference-only Tweet objects from `03ccaaa1` remain in `raw_posts`, with their
  original payloads. New run/source counters separate post captures from
  `post_references_queued` / `post_references_saved`; terminal summaries,
  progress, account totals and post limits use the same distinction. References
  and full content deduplicate independently, so either arrival order preserves
  both captures. Processing and discussion selection exclude references before
  choosing the latest content snapshot. A computed `raw_posts.is_reference`
  column also classifies historical rows without rewriting JSON; old run totals
  are not retroactively recalculated.

  **Docker replay verification:** all 1,065 captures from `03ccaaa1` were replayed
  through the real writer into an isolated database. The result was 1,062 post
  captures and three references; all 1,065 payloads were preserved unchanged.
  Migration from the old schema, repeated migration, historical classification
  and a later reference following a full post were verified against ClickHouse.
  A live check of SitDeck, karpathy and slatestarcodex (`aabbc202`) completed in
  31.4s with 103 post captures and nine references, all 112 rows retained, and
  no fetch/media failures. References can coexist with full posts for the same ID;
  the reference count describes captures, not missing-post IDs.
  [Reference regressions](../tests/scrape/test_x_references.py),
  [run-count definitions](features/history.md#timing-and-counts).
- [x] **P1: Verify Reddit pagination beyond the initial cards.** A controlled
  Docker check reproduced three cards while the listing was hidden, then 27
  after activating it and scrolling. The collector now activates the listing
  before navigation/scrolling and the detail tab before each post visit.
  A listing-only check with detail-tab handoffs extracted 66 unique posts from
  LocalLLaMA, 100 from conspiracy, 41 from gamedev and 97 from godot across
  multiple batches. DomainDrivenDesign had an explicit empty daily listing;
  ExperiencedDevs had two eligible posts. Regression tests cover both tab
  handoffs and the existing post cap. Persisted verification run
  `d06ad28c-8c45-4499-b99b-52f2609aa2dc` saved all 66 LocalLLaMA posts plus four
  accelerate posts before it was cancelled to improve rate-limit handling.
  Those rows remain saved. This establishes pagination beyond the initial cards,
  not exhaustive coverage of the selected ranking.
  [Listing collector](../src/dank/scrape/reddit/__init__.py).
- [x] **Identify HTTP errors before classifying Reddit listings as empty.**
  All five failures in the earlier run were HTTP 429 responses, not evidence
  that those communities were unavailable or empty. Listing/pagination and
  thread requests now preserve HTTP status and Retry-After. A 429 counts as a
  rate-limit failure. The initial 30-second cooldown was insufficient; the
  current five-minute Reddit cooldown is described below.
  Waiting for that cooldown is separate from the detail-page read deadline.
  JSON endpoint denials still allow the existing public rendered-thread fallback.
  After a rate-limited detail visit, the remaining details in that batch are
  deferred while their listing posts are retained with a rate-limit reason.
  Access restrictions and quota exhaustion can still leave a partial run;
  increasing pagination depth also increases detail-request volume.
  [Rate-limit handling](../src/dank/scrape/reddit/access.py),
  [regressions](../tests/scrape/test_reddit_access.py).

  **Final Docker verification:** run `7eedea39-748f-46b1-86f2-9cace3a06c16`
  used the existing configured limits and attempted all 15 communities in
  11m19s. It saved all 39 collected posts (26 LLMDevs, 13 AbsoluteUnits),
  independently verified in ClickHouse. Thirteen listings were rate limited;
  one detail 429 caused three further detail visits to be deferred. All four
  affected listing posts were saved, with explicit capture reasons. The outcome
  was correctly partial, with no failed media downloads. Full Reddit coverage
  remains constrained by the site's quota; the collector does not bypass it.
- [x] **P1: Collect Reddit posts before refreshing discussions.** Normal scrapes
  save community listings immediately, reuse unchanged successful details for
  up to 24 hours, and defer individual thread visits. `--reddit-details` refreshes
  pending snapshots from the last 24 hours separately, bounded by `max_posts`
  per community. Deferred snapshots preserve existing full bodies, media and
  discussions with their original capture dates; the reader explains the state.
  Detail refreshes use inert HTML documents and cursor-based comment fragments,
  with rendered visits as a fallback. Live checks retained the same 35- and
  27-comment discussions in 2.3 and 4.0 seconds respectively.
  Reddit requests start two seconds apart. A security block or HTTP 429 now
  defers further Reddit requests for the run, rather than waiting through a
  five-minute cooldown for every community. Already-saved listings are retained.
  See the blocked-collection update above; the following checks predate it.

  **Docker verification, 2026-10-17:** run
  `05b39a46-62e9-4d54-b521-e2886f438a12` attempted all 15 configured communities
  in **3m03s**, saved **604 distinct posts**, reused **126 detail archives**, and
  deferred **478 refreshes** without individual thread requests or HTTP 429s.
  Fourteen communities completed, including an explicitly empty daily listing
  for DomainDrivenDesign. LLMDevs returned no posts; a separate browser check
  confirmed Reddit's network-security block. The run correctly remained partial.
  A bounded separate refresh check completed two pending posts successfully.
  [Configuration](features/reddit.md#run),
  [cache tests](../tests/scrape/test_reddit_cache.py),
  [refresh tests](../tests/scrape/test_reddit_refresh.py).
- [x] **P2: Recover unavailable Reddit listings and verify LLMDevs.** The earlier failed listing returned an
  HTTP-200 community-not-found dialog; a later probe returned a network-security
  block. Fresh checks with the same Docker profile and Top/day URL returned posts,
  so neither a permanent ban nor a credential problem was established.
  The collector now retries an unusable first page once (ten-second delay),
  recognizes the matching unavailable-community response, and defers further
  Reddit requests after a network-security block. A header without posts
  no longer masquerades as an empty successful listing. Already-saved posts are
  never restarted by these retries; private/login restrictions and persistent
  failures remain visible. The Docker run `ff77e843` completed all 15 communities
  in 2m58s: 622 distinct posts saved, including 25 from LLMDevs across two pages;
  DomainDrivenDesign was explicitly empty for Top/day. There were no collection
  failures, parse failures, 429s or retries. Six media failures (five oversized
  GIFs and one preview-image 403) kept the overall outcome partial. Reddit access
  can still change; this successful pass does not establish the cause of its
  earlier responses. A fresh-process repeat (`82188e64`) saved the same 25
  LLMDevs posts in 11.9s without collection or media failures.
  [Recovery tests](../tests/scrape/test_reddit_recovery.py).
- [x] **P2: Preserve Reddit preview-image signatures.** The HTTP client rewrote
  `crop=1%3A1%2Csmart` as `crop=1:1,smart` in a signed external-preview URL.
  A controlled Docker check reproduced 403 twice for the rewritten URL and 200
  twice for the original URL (4,167-byte JPEG), without changing cookies or
  refreshing the signature. Preview downloads now preserve the escaped URL
  through redirects. Genuine 403 responses still count as failures and retain
  their post/reference. This fixes the observed URL, not every possible CDN denial.
  [Signed-URL regressions](../tests/scrape/test_signed_media_urls.py).

  **Docker verification, 2026-10-17:** run `813da81f` completed all 15 communities
  in 3m21s, saving 605 distinct posts and reporting zero collection, parse, 429
  or media failures. Six oversized files counted as skips under the unchanged
  10 MiB cap. DomainDrivenDesign and ExperiencedDevs were explicitly empty for
  Top/day. A separate retry of the exact six earlier failures saved the 4,167-byte
  preview JPEG and retained five size-skipped GIF references, with zero failures.
- [x] **P1: Restore YouTube format discovery and downloads.** The pinned
  yt-dlp `2026.8.19` now includes its matching `yt-dlp-ejs 0.8.0` solver package.
  Fresh public-video probes returned separate streams but no combined MP4,
  explaining the old selector's failure even after solver installation.
  The collector now chooses complete H.264/AAC formats within the combined size
  budget and merges them into MP4. CLI exception swallowing is disabled so size
  exclusions remain skips. Generic failures are no longer repeated without cookies.
  Isolated staging removes incomplete downloads, and only the requested completed
  file can populate its URL-specific cache. Private, removed or blocked videos
  still fail visibly. [Downloader](../src/dank/scrape/assets/audio_video.py),
  [selection tests](../tests/scrape/test_youtube_formats.py),
  [download regressions](../tests/scrape/test_youtube_downloads.py).

  **Docker verification, 2026-10-17:** three real videos (`jNQXAC9IVRw`,
  `A0fECnj5sas`, `aqz-KE-bpKQ`) saved complete H.264/AAC MP4s of 745,939,
  9,394,397 and 8,365,076 bytes under a 10 MiB cap. FFmpeg decoded both streams
  fully without errors. Docker Chromium played each with decoded audio/video,
  then sought near the end and reached `ended` without a media error. A 1 KiB
  check and a fourth video with no fitting format both counted as skips, with
  no published/partial files. A saved Register article's previously failed video
  was also downloaded and persisted through the normal configured Docker path.
  This establishes these samples, not universal YouTube availability.
- [ ] **P2: Expose browser-stage progress.** Browser detail work is absent from
  HTTP-active counters, and source-history counters are finalized only at source
  end. Heartbeats stayed at zero active HTTP/media while Reddit worked on posts.
  Add meaningful account/community/detail progress and distinguish buffered
  records from durable rows. Done when a heartbeat identifies active browser
  work, waiting and stalled work without inspecting network traffic.
  [Progress metrics](../src/dank/scrape/metrics.py).
- [x] **P2: Count known media size exclusions as skips.** Five Reddit GIFs of
  roughly 15–46 MB exceeded the configured 10 MiB cap and were previously counted
  as failures. Oversized estimates, HTTP headers/streams and completed media files
  now use an explicit size-exclusion signal, increment `media_skipped`, and log
  the reason at info level. Partial files are removed; posts and raw references
  are retained. Other failures remain failures. Regressions check exact-limit
  downloads, missing/incorrect headers, cleanup and a completed source outcome.
  [Size-limit tests](../tests/scrape/test_media_limits.py),
  [outcome counters](../src/dank/scrape/assets/__init__.py).
- [ ] **P2: Choose smaller X video variants and retain per-asset outcomes.**
  YouTube now selects fitting complete formats and classifies known size aborts
  as skips. X still selects the largest variant before applying the size policy;
  prefer a smaller eligible variant where available. Raw references retain only
  the URL/path, not a durable per-asset outcome reason.
  [Variant selection](../src/dank/scrape/x/payloads.py).
- [ ] **P2: Classify failed publisher media and make retries selective.** Runs
  `8d54ed62` and `19bb2610` each logged five Wikimedia image 403s, two Microsoft
  image 403s, three `shkspr.mobi` favicon 401s and an Internet Archive connection
  failure. The header, referrer and Archive-link repairs above recover those
  cases; Microsoft's two missing images remain publisher-side 404s. Durable
  per-asset reason/status and selective media retries remain follow-ups. Let a scoped
  retry revisit unavailable media without recollecting every source; investigate
  each host before assuming stale URLs or an authentication requirement.
  [Media pipeline](../src/dank/scrape/assets/http.py).
- [ ] **P2: Make blocked feed/article recovery actionable.** Niche Gamer's feeds
  returned 403; article fetches also failed across several publishers, and
  Order Order exhausted its 429 retries. Run `19bb2610` also logged article
  403s for Ars Technica (20 URLs), Etsy (20), Netflix Tech Blog (10), LWN (6)
  and Microsoft (1), plus connection failures after redirects from `postgr.es`
  to `stormatics.tech` and `dbadevops.com`. The Register previously returned 15
  article verification pages; in `19bb2610` its feed itself returned a verification
  page, so no articles were collected. Previously saved articles remain stored.
  Retain host/status and retry eligibility
  in a source summary, distinguish retained feed text from a fetched article,
  and review supported feed alternatives. Done when operators can identify
  affected sources and choose a scoped retry or configuration change without
  searching the whole log. Existing access restrictions may remain.
- [ ] **P2: Bound and cancel yt-dlp jobs.** Code inspection shows downloads run
  through `asyncio.to_thread` without a DANK-owned overall deadline; cancelling
  an await does not stop the worker. Network reads now have a 15-second timeout
  and extraction/download/fragment retries are limited to one, but this does not
  bound the entire job or a stuck FFmpeg process. This is a potential stall risk, not an
  observed yt-dlp hang in this run. Add a bounded, cancellable execution model.
  Done when an intentionally stalled downloader releases its media slot, cleans
  partial output and permits graceful shutdown within a tested deadline.
- [ ] **P3: Reduce routine network log noise.** Full GraphQL query parameters
  and Reddit telemetry responses obscure warnings and useful stage messages.
  Keep account/community progress and aggregate capture counts at INFO; move
  detailed network diagnostics to DEBUG with sensitive URL data omitted.
  Done when a normal run can be audited without hundreds of transport lines.

## What the reader tells us

There are currently **two collection-quality pills**:

| Indicator | Trigger | Reader behavior and limits |
| --- | --- | --- |
| **Not collected** | The sanitized article body has neither non-whitespace text nor an image/audio/video/source element with a `src`. | Shows “Full text not collected” and an original-source link. Opening it does not mark it read. This describes the output, not whether scraping or extraction caused the loss. |
| **Incomplete scrape** | Content is available, but its RSS capture records a failed page fetch or matches the known Register verification screen. | Keeps available content readable and warns that text, formatting or media may be missing. It does not measure how much is missing. |
| No quality pill | Content is available without either recorded failure signal. | This is not proof that the article is complete. Summaries, unknown challenge screens, wrong-body extraction and missing media can escape detection. |

The incomplete check matches the **capture behind the displayed processed
article**, including its collection timestamp. A new successful scrape clears
it after processing, not merely after downloading. Recovering formatting from
an older saved page can leave the newer capture's failure warning in place.
An empty body with a failed-capture flag still shows **Not collected**.

“Article/Post” identifies content type; “Read” is reading state. Neither is a
collection outcome. API/loading errors have retry messages, and image-load
failures have a placeholder. These are separate from the two quality pills.

Implementation: [content check](../reader/src/lib/article.ts),
[card precedence](../reader/src/components/PostCard.tsx),
[capture lookup](../src/dank/web/reader_api.py).
Coverage: [reader journeys](../reader/tests/reader.spec.ts) and
[API tests](../tests/web/test_reader.py).

## Known source cases

The configured source can differ from the publisher being fetched. For example,
the Microsoft article below was collected through `lobste.rs`; source filters
and recovery commands use that configured source identity.

| Source / publisher | Observed problem | Current handling | Status / remaining gap |
| --- | --- | --- | --- |
| `arstechnica.com` | On 2026-10-15, “Fridge hack costing millions and sparking fires stemmed from poor security” returned HTTP 403. The retained feed supplied five paragraphs, with no article-page HTML. | Retained failure metadata produces **Incomplete scrape** while the feed text stays readable. Saved failed pages can be retried and then processed. [Fetch handling](features/rss.md#article-fetch-failures), [retry tests](../tests/scrape/test_retry_pages.py). | **Mitigated.** The badge is evidence of a failed capture, not confirmation that those five paragraphs are the whole article. A retry can still be denied. |
| `www.theregister.com` | HTTP 403 responses; an HTTP 200 “Are we human?” verification shell; full feed text flattened into one block. | The known verification shell counts as a failure, including older saved shells. Matching article pages restore paragraphs, inline markup and supported figures; processing can reuse an older matching capture. [Challenge detection](../src/dank/html_utils.py), [structure recovery](../src/dank/process/feed_html.py), [tests](../tests/process/test_feed_html.py). | **Mitigated.** The reported Metropolitan Police article was restored to 28 paragraphs on 2026-10-15. Other captures can remain blocked. Detection recognizes this particular verification screen, not every anti-bot page. |
| `techcommunity.microsoft.com`, collected through `lobste.rs` | The “Microsoft Defender for Linux” capture had a loading shell in its HTML body while the article lived in embedded page data. Ordinary extraction produced no readable text. | The processor reads the matching Khoros blog message from saved Next.js JSON when no readable article container exists. Empty/script-only containers permit recovery; rendered text stays preferred. Unrelated cache entries, comments and drafts are excluded, scripts are not executed, and normal sanitization still applies. [Extractor](../src/dank/process/page.py), [regression tests](../tests/process/test_embedded_article.py). | **Handled for this layout.** Merged and reprocessed on 2026-10-15: all 587 words, 12 paragraphs, six headings, four lists and seven links verified on desktop/mobile; the image was separately collected and verified from the local cache. Original raw capture and post metadata stayed unchanged. Other client-rendered layouts remain unsupported; reprocessing does not automatically collect media found only in embedded JSON. |
| BBC domains | Homepage discovery needs an explicit source-specific fallback. | The scraper supplies built-in BBC RSS URLs; configured `feed_urls` bypass automatic discovery. [Discovery behavior](features/rss.md#configure), [tests](../tests/scrape/test_rss_fallback.py). | **Handled for the supplied feeds.** This does not guarantee their availability or coverage of every BBC section. |
| `x.com` | Login/session challenges or confirmation requirements can stop timeline collection. | Session-only collection; `make x-login` checks the saved session first and reports reuse when valid, skipping 1Password access and Docker desktop startup. New sign-in uses the same profile as collection. Expired or challenged sessions count as a fetch failure and stop X collection. [X guide](features/x.md), [collector](../src/dank/scrape/x/__init__.py). | **Mitigated.** The RSS incomplete flag does not describe X collection failures. Use run history and logs. Optional 1Password autofill fills visible X fields once without submitting them; manual sign-in remains available when the CLI is missing or access is denied. Complete verification in the sign-in browser. A visible temporary-login-limit message stops sign-in immediately; do not retry until X clears the restriction. X may still reject automated browsers or change its authenticated navigation markers. Session expiry, cleanup and profile locking are covered in `tests/scrape/test_x_session.py` and `tests/scrape/test_browser_session.py`; browser defaults and limit handling are tested locally without live login attempts. Reuse is reported only after detecting authenticated navigation. X may still require verification or temporarily limit login attempts. |
| `reddit.com` | Community listings may be blocked, empty, truncated, or stop loading more posts. | Browser collection preserves available listing content, excludes ads and other communities, and stops at configured post/scroll/stall limits. Top for the past day is the default. Access failures are reported explicitly. [Reddit guide](features/reddit.md), [collector tests](../tests/scrape/test_reddit.py). | **Bounded collection.** Individual post bodies/media and up to 50 comments including replies (Best by default) are now captured with configurable limits and sort. Body extraction prefers Reddit's semantic article fragment so expand/collapse controls stay out of reader text; older markup falls back to its body slot. Blocked detail requests retain the feed post; comments are explicitly captured, disabled, or failed, while historical posts remain not captured. Rendered-thread fallback can return fewer than the limit because replies are collapsed or loading stops; the reader indicates more comments. Failed detail refreshes preserve the last successful body/media. Failed or disabled comment refreshes preserve the archived discussion with its original capture date and latest attempt state; successful empty captures replace it. Native video is remote by default; optional downloads use yt-dlp to assemble audio/video, gated by both Reddit and global media settings. Remote links can expire or be blocked. A completed run does not prove complete daily coverage. [Detail capture tests](../tests/scrape/test_reddit_details.py). The RSS incomplete flag and `--retry-failed-pages` do not apply to Reddit. Community icons are discovered from the public header; layout changes or a missing custom icon use initials or the previous cache. Invalid image responses are discarded so a later scrape can retry. Flair choices reflect collected posts and their latest raw snapshots, not the full community flair catalogue. [Icon cache tests](../tests/scrape/test_community_icons_storage.py), [flair tests](../tests/web/test_reader_flairs.py). |

## Shared handling and gaps

These are implemented safeguards or known limitations across sources. Generic
test fixtures establish behavior, not an incident at a particular publisher.

| Problem | Handling today | Limit / follow-up |
| --- | --- | --- |
| Graph connections can lose provenance or misattribute links in page chrome and embedded foreign posts | A separate evidence index extracts owned post text, structured HN/Reddit comments with verified ancestry, scoped publisher bodies and identifiable article comments. X quotes, recommendation payloads, comment author/reply controls and self-links are excluded. URL aliases resolve collected destinations; publisher path/query meaning is preserved, and only saved YouTube metadata supplies video titles. Raw captures are unchanged. | **Saved evidence only.** The first startup scans the archive once in 64-row batches; future collection/processing writes evidence at the persistence boundary. Graphs show up to 100 occurrences in each direction with totals; older evidence retains its date and exact lookup. Only processed origins are navigable; newer unprocessed evidence is withheld. Missing raw ownership fields, uncollected comments, unsupported article layouts and X replies not saved as standalone posts remain outside coverage. No semantic, sentiment or topic relationships are inferred. [Extraction tests](../tests/test_references.py), [API and migration tests](../tests/web/test_reader_connections.py). |
| Article fetch fails, times out or returns an empty body | By default the entry is skipped. With `keep_feed_on_fetch_failure = true`, the feed entry is retained with `page_fetch_status: "failed"`. Warnings record the URL and available error detail. | Retention is opt-in. The raw flag does not retain a structured HTTP status or detailed reason; inspect logs for that. `--retry-failed-pages` is manual, not a scheduled retry. [RSS guide](features/rss.md#article-fetch-failures). |
| HTTP 429 / host cooldown | RSS/homepage requests have up to three retries with 2/4/8-second backoff; `Retry-After` and shared per-host cooldowns are respected within the 30-second wait limits. | Other HTTP errors and timeouts do not get those same-run retries. Direct-media 429s start a cooldown but that download is not retried in the same run. [Limits](features/rss.md#rate-limits), [tests](../tests/scrape/test_fetch_logging.py). |
| Missing or malformed feeds | Explicit feed URLs can bypass discovery. Fetch/parse failures are counted and logged; a source without usable feeds is reported as skipped or failed according to its counters. | An unavailable or invalid feed yields no entries to retain, and therefore may produce no reader card or pill. A valid empty feed can complete with zero posts. [History](features/history.md#what-is-recorded), [runner tests](../tests/scrape/test_scrape_runner.py). |
| Browser profile stays locked after a Docker scrape | Chromium gets up to five seconds to exit normally before driver cleanup, allowing it to remove its singleton links. | **Handled for normal shutdown.** Two consecutive one-off Docker scrapes on 2026-10-16 reused a profile and left no singleton links while preserving profile data. Crashes or forced termination may still leave a lock; verify that its owner is gone before clearing stale links. [Browser lifecycle tests](../tests/scrape/test_browser_session.py). |
| X stalls while Reddit waits for the shared browser | X and Reddit use separate browser processes, persistent profiles and per-site locks. X keeps its existing login profile; Reddit starts with its own profile. X browser startup/navigation and scroll steps have 30-second budgets; a timeout records a fetch failure, closes its browser and stops that X source. | **Cross-site blocking removed; X driver waits bounded.** A Docker run inspected on 2026-10-17 had saved 826 RSS posts and no X posts before X stopped responding and Reddit remained queued. The run was cancelled with saved rows intact. Regression tests verify Reddit persistence while X waits, same-site serialization, separate media cookie paths, timeout reporting and capture-handler cleanup. [Runner tests](../tests/scrape/test_scrape_history.py), [X tests](../tests/scrape/test_x_collection.py), [concurrency](features/history.md#concurrency). Both sites still use the configured source-worker limit. These bounds do not retry failed accounts or impose an overall run deadline. |
| X account pages display posts but collection captures none | Capture recognizes `UserOriginalsTimeline` as well as the older account timeline endpoints; it still excludes the personalized home timeline. | **Observed endpoint change handled.** On 2026-10-17 a saved session in headless Docker received 27 parseable posts from `UserOriginalsTimeline`, which the old capture filter ignored. After the fix, a single-account Docker scrape saved 33 X posts in 6.4 seconds with no fetch failures; media downloads were disabled. No new login was needed. This establishes that headless collection can work with that session, not that X will always accept it. [Capture regression tests](../tests/scrape/test_x_collection.py). Future endpoint or payload changes may need another update. |
| Large HTTP header fields | Direct HTTP requests allow header fields up to 16 KiB, including redirect responses. | Larger fields still fail. This is a parser-limit fix, not an access-control workaround. [Regression tests](../tests/scrape/test_http_header_limits.py). |
| X text repeats, links disappear or attachments are absent | Processing prefers full note text, escapes literal text, preserves paragraphs and expands saved URL entities. Reader cards and detail share post content, link cards and ordered media; matching local files take precedence over captured X URLs. Metadata uses the exact processed capture rather than a newer pending scrape. | **Saved content recoverable.** The 2026-10-17 `@3blue1brown` example already contained its YouTube destination, card title and thumbnail; stripping trailing short links and requiring downloaded assets hid them. Use the [X reprocessing command](features/x.md#reading-saved-posts) to rebuild saved records. Remote media can expire; unsupported video formats retain their poster and original-post link. Missing raw metadata falls back to processed text and saved media. Reply, repost, like and view counts describe that same capture; missing or malformed counts remain unknown rather than zero. Conversation snapshots are collected separately with bounded detail browsing; reprocessing alone fetches neither conversations nor missing media. [Processing tests](../tests/process/test_process_x.py), [content tests](../tests/test_x_content.py), [reader tests](../tests/web/test_reader_x.py). |
| X, YouTube or Reddit video keeps playing outside the reading view | A shared reader controller pauses the previous player when another starts, and pauses a player when it leaves the viewport or the reader navigates. Pausing preserves position; returning to view requires manual resume. Feed and opened-post players share the latest per-video position within the reader tab, including backward seeks. Reloading clears this navigation state. Reddit cards and opened posts share the same controller, including HLS and verified MP4 fallbacks; delayed loading cannot restart a cancelled player. | **Reader behavior covered.** YouTube is controlled through its iframe message channel, restricted to the matching frame and origin. The bridge waits for readiness before starting, cancels stale starts, and cleans up listeners and polling. This provider protocol may change; unavailable embeds retain **Watch on YouTube**. [Playback lifecycle tests](../reader/src/lib/playback.test.ts), [browser coverage](../reader/tests/youtube.spec.ts), [navigation handoff tests](../reader/tests/playback-position.spec.ts). |
| Reddit video URLs repeat or portrait players have large side bars | The reader hides only raw references matching a displayed clip, including archived copies and signed URL variants. Failure restores the reference; descriptive links and unrelated clips stay visible. Player sizing uses poster dimensions before playback, then the video's intrinsic dimensions. | **Existing saved posts supported.** The API retains the original clip URL beside local media. Missing or incorrect posters are corrected when video metadata loads; streams still load only on demand. [Reader tests](../reader/tests/reddit-video-layout.spec.ts), [URL matching tests](../reader/src/lib/redditText.test.ts). |
| X conversations contain only part of the replies or thread context | Collection saves bounded, focal-post-validated `TweetDetail` snapshots after account timelines, or for selected saved posts via `--x-post`. Parent context and replies are archived together; ads, nested quotes and unrelated recommendations are excluded from conversation slots. The reader retains earlier snapshots when a later attempt fails or skips detail capture. | **Partial coverage is explicit.** Budgets, remaining cursors, hidden branches, missing context and reply-count shortfalls mark snapshots partial. Missing responses, sign-in failures and HTTP failures never claim zero replies or completeness. A live signed-in Docker check on 2026-10-17 saved 22 related posts from a validated detail response and correctly reported a remaining cursor. Capture does not expand hidden branches or download extra reply media; remote media may expire. [Conversation guide](features/x.md#conversation-snapshots), [collector tests](../tests/scrape/test_x_conversations.py), [CLI tests](../tests/scrape/test_x_conversation_cli.py). |
| Flattened feeds, lost figures/captions, relative URLs | Full plain-text feeds recover structure only from complete ordered text matches. Supported figures between matching sibling blocks are retained. WordPress caption containers become figures; final response URLs and valid HTML base URLs resolve links/media. | Formatted feeds stay authoritative; incomplete matches stay unchanged. Older captures without redirect metadata may need a fresh scrape. These rules do not validate every page layout. [Processing behavior](features/processing.md), [caption tests](../tests/test_readable_html.py), [redirect tests](../tests/scrape/test_rss_redirects.py). |
| Missing or failed media downloads | Failed direct HTTP downloads remove partial files; absent local files are omitted from processed assets. `files_failed` and `media_skipped` distinguish failures from reference-only, disabled and known size-excluded downloads. | Media failure alone does not trigger **Incomplete scrape**. An article may remain readable with remote images or unavailable media. Configured size limits can also prevent downloads. [Media guide](features/media.md), [asset tests](../tests/scrape/test_scrape_assets.py). |
| Extensionless images, observed on Microsoft Community Hub and `shkspr.mobi` | Downloaded PNG and WebP images with no filename type are recognized from their signatures, letting the reader use local copies. Known filename types stay authoritative. | **Handled for PNG and WebP.** Other unknown formats remain unclassified. Existing asset processing is incremental; already-processed unknown metadata needs a refreshed raw asset. This does not discover or download embedded media automatically. [Media guide](features/media.md), [classification and reader tests](../tests/process/test_process_assets.py). |
| Processing fails or produces no usable post | Raw captures remain available for reprocessing. Converters can return no post and be skipped; embedding/database exceptions can stop processing after earlier batches have been saved. | There is no per-article processing-error pill or durable processing-status ledger. The reader may show an older processed version or no record. Use process logs and compare raw/processed timestamps. [Processor](../src/dank/process/runner.py), [tests](../tests/process/test_process_runner.py). |

Scrape outcomes such as `partial`, `failed`, `skipped` and `cancelled` are
**run/source outcomes**, not additional article pills. `partial` means failures
occurred alongside saved records; intentional media skips alone do not make a
run partial. A zero command exit or `completed` run does not prove article
completeness. See [history semantics](features/history.md#what-is-recorded).

## Choose the recovery path

1. **Identify the failing stage.** Check the source/post identity, the raw
   capture behind the displayed version, and matching scrape/process logs.
   Record whether the problem is fetching, extraction, processing or media.
2. **Missing/blocked article page:** run a scoped
   [`scrape --retry-failed-pages`](features/rss.md#article-fetch-failures),
   then process the new captures. This works after an entry leaves its feed;
   it cannot bypass a publisher's refusal or recover an entry never retained.
3. **Usable page already saved:** fix/validate extraction and use scoped
   [`process --reprocess`](features/processing.md#run). Reprocessing alone
   cannot fetch a missing page. Preserve original raw captures.
4. **Verify the result:** compare the extracted text and structure with the
   saved source, inspect the reader, and check the relevant warning state.
   Mark an issue handled only after the change is in `main` and verified.

`--domains` selects configured sources; the processing age window uses capture
time. Native and Docker commands are in the linked guides. Per-run archives
are created by `make scrape` / `make process`; an ordinary direct invocation
does not create those archives.

## Keep this register current

When adding a quality indicator, fallback, domain workaround or recovery fix,
update the relevant row with its trigger, affected source/example, actual
handling, remaining gap, verification date and code/test reference. Add a new
row only for a distinct problem. Keep proposed or unmerged fixes visibly
separate from shipped handling, and retain useful resolved cases so the
reason for a workaround is not lost. Keep credentials and full raw payloads
out of this document; link to code/tests and summarize dated observations.


### X saved reply relationships

X feeds default to Posts; Posts & replies keeps strict date ordering while
showing parent links. Independently saved posts are connected in the opened
reader without requiring a discussion capture. This does not collect missing
replies: profile timelines can omit them, and a saved parent or child does not
prove complete coverage. Unknown reply metadata remains visible in Posts.
Related lookups use displayed raw versions and are bounded to 200 posts and 20
ancestor levels; gaps and limits are explicit. Archive dates and capture counts
retain their original meaning. Covered by
[relationship tests](../tests/web/test_reader_x_saved.py) and
[reader acceptance tests](../reader/tests/x-saved-replies.spec.ts).

### X embedded quoted posts

On 2026-10-18 the saved Eric Weinstein post `2110938986801938839` contained
Haider's quoted post `2110797855879471162` and its video, but the reader omitted
the entire quote. Feeds and opened conversations now render one embedded level
from the parent snapshot, with a separately saved post as the ID-only fallback.
Quoted posts and their media are already discovered by timeline collection;
this change exposes those captures without another scrape. Saved local files
are preferred, and remote playback still depends on the captured URL remaining
available. The example's direct video returned HTTP 403 in Chrome; a targeted
retry saved its captured 720p rendition within the existing download limit and
verified local playback. Missing/deleted quote content stays explicit; this
does not collect all replies or fetch missing quotes when opening the reader.
Covered by
[quote API tests](../tests/web/test_reader_x_quotes.py),
[collection tests](../tests/scrape/test_x_references.py) and
[reader tests](../reader/tests/x-reader.spec.ts).

## Refresh from the reader

Reader Refresh starts a collection for the current source context or saved post.
All-view platform choices and source/tag filters narrow collection. Search text,
dates, flairs and read markers remain display filters; they do not change a
source's configured collection limits. Individual Reddit/X post refreshes
request details and comments explicitly. YouTube video refreshes retry metadata
and reuse saved successful transcripts; community-post detail refresh and the
computed **From your feeds** view do not have a standalone collector.

Up to three independent refreshes run in separate child processes, with at most
five outstanding requests and duplicate-click reuse. Overlapping source domains
and shared browser profiles wait; an unrestricted All refresh runs alone and
keeps its place ahead of later requests. Command-line scrapes remain exclusive;
reader jobs wait and retry when that lock is held. Public article recovery uses
separate temporary browser profiles so Blogs and Hacker News can run together.

Processing uses only the posts saved by that run. The notice distinguishes running
and waiting jobs, and **Show updates** opens the completed job's source view or
post instead of reloading an unrelated current feed. Each completed result has
its own action. Partial collection and processing failures remain visible.
Completion means the attempted collection finished, not that every post on a
platform is archived. See [concurrency tests](
../tests/scrape/test_refresh_concurrency.py) for source/profile locking.

Recent job results persist locally. A server restart marks unfinished jobs as
interrupted instead of replaying them. The queue and status are local to the
running DANK installation; provider blocks, configured collection limits and
unavailable original pages still apply. See [scope tests](
../tests/web/test_reader_refresh_scope.py) and [job lifecycle tests](
../tests/web/test_reader_refresh.py).
