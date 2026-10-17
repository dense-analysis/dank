# Known collection and processing issues

Use this register to understand collection-quality problems, the handling
already in place, and the gaps that remain. Detailed commands live in the
linked feature guides; live outcomes live in [run history and logs](features/history.md).

Last reviewed: **2026-10-17**, against the implementation on `main`.
Publisher examples describe observed captures, not a claim that a site always fails. “Mitigated”
means a fallback exists; “handled” means the specific case has a tested fix.

## Latest full-scrape check

On **2026-10-17**, Docker run `8d54ed62-e308-49ec-9e3c-10c1e2dc8ff1`
used `bec2f83` and the existing 55-source configuration. It exited normally after
**6m00s**, with outcome **partial**: 42 sources completed, 12 were partial, and
Niche Gamer failed. All **1,565 collected records were saved** (1,545 distinct
source/post IDs). All 1,545 distinct posts and all 24 available YouTube files
were processed and independently checked against the latest saved captures.
The archived log is `logs/scrape/2026-10-17T13-19-46Z-27115.log`.

| Source | Saved records | Distinct posts | Result |
| --- | ---: | ---: | --- |
| RSS | 826 | 826 | Some feeds, articles and media remained unavailable. |
| X | 145 | 125 | Four account timelines collected; the fifth timed out; 31 accounts were not attempted. Twenty records are conversation snapshots of timeline posts. |
| Reddit | 594 | 594 | All 15 communities finished, including 29 LLMDevs posts. DomainDrivenDesign and ExperiencedDevs were explicitly empty for Top/day. |

Reddit took **3m03s** with no collection failures, media failures or 429s. X had
four conversation timeouts on `claudeai`, then a navigation timeout on
`White_Rabbit_OG`; the timeout stopped X while preserving saved rows. Of 20 X
conversation attempts, eight captured, eight were partial under cursor/reply
limits, and four failed. The logs do not establish the browser's underlying
failure; they do establish that subsequent accounts cannot currently recover.

YouTube produced **24 available MP4s from 52 references**; the other **28 were
size-excluded**, with no unexplained missing files or YouTube download failures.
Every available file was independently checked with FFprobe for video/audio
streams and the unchanged 10 MiB cap; the largest was 10,229,642 bytes. The
extractor logged two recoverable initial-data warnings and successfully used
its API fallback. This run did not repeat playback of every video; the three
Docker decode/playback checks below remain the playback evidence.

Overall media: **141 new, 2,996 cached, 297 skipped, 11 failed**. Real media
failures remain on Wikimedia, Microsoft, `shkspr.mobi` and Internet Archive.
The four HTTP 429 responses were retries of one Order Order article, not Reddit
or YouTube limits. Subsequent X-only verification (`03ccaaa1`, below) completed
all 36 accounts after the recovery and posts-first changes. Scoped publisher/media
retries remain open; the checkboxes below track the current work.

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
  Reddit requests start two seconds apart and double their interval after a 429,
  up to 60 seconds. A shared five-minute cooldown honors longer Retry-After
  values, with a ten-minute wait budget that does not hold HTTP capacity. An
  initially limited community gets one retry; partially saved listings are not
  restarted and duplicated.

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
  recognizes the matching unavailable-community response, and applies the shared
  cooldown to network-security blocks before one retry. A header without posts
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
- [ ] **P2: Classify failed publisher media and make retries selective.** Run
  `8d54ed62` logged five Wikimedia image 403s, two Microsoft image 403s, three
  `shkspr.mobi` favicon 401s and an Internet Archive connection failure. These
  remain real download failures, distinct from the fixed Reddit signed-preview
  URL and configured size skips. Retain per-asset reason/status and let a scoped
  retry revisit unavailable media without recollecting every source; investigate
  each host before assuming stale URLs or an authentication requirement.
  [Media pipeline](../src/dank/scrape/assets/http.py).
- [ ] **P2: Make blocked feed/article recovery actionable.** Niche Gamer's feeds
  returned 403; article fetches also failed across several publishers, and
  Order Order exhausted its 429 retries. The latest check also logged article
  403s for Ars Technica (20 URLs), Etsy (20), Netflix Tech Blog (10), LWN (6)
  and Microsoft (1), plus 15 Register verification pages and connection failures
  after redirects from `postgr.es` to `stormatics.tech` and `dbadevops.com`.
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
| Extensionless images, observed on Microsoft Community Hub | A downloaded image with no known filename type is recognized as PNG from its signature, letting the reader use its local copy instead of the remote URL. Known filename types stay authoritative. | **Handled for PNG.** Other unknown formats remain unclassified. Existing asset processing is incremental; already-processed unknown metadata needs a refreshed raw asset. This does not discover or download embedded media automatically. [Media guide](features/media.md), [classification and reader tests](../tests/process/test_process_assets.py). |
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
