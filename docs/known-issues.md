# Known collection and processing issues

Use this register to understand collection-quality problems, the handling
already in place, and the gaps that remain. Detailed commands live in the
linked feature guides; live outcomes live in [run history and logs](features/history.md).

Last reviewed: **2026-10-17**, against the implementation on `main`.
Publisher examples describe observed captures, not a claim that a site always fails. “Mitigated”
means a fallback exists; “handled” means the specific case has a tested fix.

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
- [ ] **P1: Recover X collection after a browser timeout.** X saved 594 records
  before its 21st account timed out; 15 accounts were never attempted. The
  30-second timeout prevented an indefinite wait and Reddit continued.
  Record per-account outcomes and add bounded browser recovery/checkpointing.
  Done when a forced mid-run browser failure retains saved records, reports
  the affected account, and allows remaining accounts to be attempted without
  automatic sign-in retries. The underlying renderer failure remains unknown.
  [Collector](../src/dank/scrape/x/__init__.py).
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
- [ ] **P2: Investigate Reddit preview-image 403 responses.** One external-preview
  image failed in the same run. Check URL freshness and signed-query handling;
  neither cause is established. Preserve the post and distinguish unavailable
  media from collection failures.
- [ ] **P1: Restore YouTube format discovery and downloads.** Logs reported
  unavailable requested formats and missing challenge-solver scripts. The
  container has Node and yt-dlp `2026.8.19`, but no `yt-dlp-ejs`. Pin compatible
  solver support using the [official EJS setup](https://github.com/yt-dlp/yt-dlp/wiki/EJS),
  then inspect formats before changing the combined-MP4 selector. A separate
  metadata-only check also hit HTTP 429 and a sign-in challenge; missing EJS
  is not established as the sole cause. Report blocked access and honor cooldowns.
  Done when representative available videos download with playable audio/video within
  the size policy in Docker and native mode. Keep removed/unavailable videos
  distinct, and avoid repeating deterministic format failures with cookies
  and then without cookies. [Downloader](../src/dank/scrape/assets/audio_video.py).
- [ ] **P2: Expose browser-stage progress.** Browser detail work is absent from
  HTTP-active counters, and source-history counters are finalized only at source
  end. Heartbeats stayed at zero active HTTP/media while Reddit worked on posts.
  Add meaningful account/community/detail progress and distinguish buffered
  records from durable rows. Done when a heartbeat identifies active browser
  work, waiting and stalled work without inspecting network traffic.
  [Progress metrics](../src/dank/scrape/metrics.py).
- [ ] **P2: Separate size exclusions from download failures.** The configured
  10 MiB cap rejected many videos, while empty download paths count as failures.
  The posts-first Reddit run also rejected five GIFs of roughly 15–46 MB under
  that cap; those posts were saved.
  X selects the largest variant before applying that cap. Preserve a structured
  outcome such as size-excluded, unavailable or failed, and consider a smaller
  eligible variant. Done when policy exclusions do not inflate failure counts,
  usable smaller variants can be saved, and the configured limit is still enforced.
  [Variant selection](../src/dank/scrape/x/payloads.py),
  [outcome counters](../src/dank/scrape/assets/__init__.py).
- [ ] **P2: Make blocked feed/article recovery actionable.** Niche Gamer's feeds
  returned 403; article fetches also failed across several publishers, and
  Order Order exhausted its 429 retries. Retain host/status and retry eligibility
  in a source summary, distinguish retained feed text from a fetched article,
  and review supported feed alternatives. Done when operators can identify
  affected sources and choose a scoped retry or configuration change without
  searching the whole log. Existing access restrictions may remain.
- [ ] **P2: Bound and cancel yt-dlp jobs.** Code inspection shows downloads run
  through `asyncio.to_thread` without a DANK-owned overall deadline; cancelling
  an await does not stop the worker. This is a potential stall risk, not an
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
| X, YouTube or Reddit video keeps playing outside the reading view | A shared reader controller pauses the previous player when another starts, and pauses a player when it leaves the viewport or the reader navigates. Pausing preserves position; returning to view requires manual resume. Reddit cards and opened posts share the same controller, including HLS and verified MP4 fallbacks; delayed loading cannot restart a cancelled player. | **Reader behavior covered.** YouTube is controlled through its iframe message channel, restricted to the matching frame and origin. The bridge waits for readiness before starting, cancels stale starts, and cleans up listeners and polling. This provider protocol may change; unavailable embeds retain **Watch on YouTube**. [Playback lifecycle tests](../reader/src/lib/playback.test.ts), [browser coverage](../reader/tests/youtube.spec.ts). |
| X conversations contain only part of the replies or thread context | Collection saves bounded, focal-post-validated `TweetDetail` snapshots after account timelines, or for selected saved posts via `--x-post`. Parent context and replies are archived together; ads, nested quotes and unrelated recommendations are excluded from conversation slots. The reader retains earlier snapshots when a later attempt fails or skips detail capture. | **Partial coverage is explicit.** Budgets, remaining cursors, hidden branches, missing context and reply-count shortfalls mark snapshots partial. Missing responses, sign-in failures and HTTP failures never claim zero replies or completeness. A live signed-in Docker check on 2026-10-17 saved 22 related posts from a validated detail response and correctly reported a remaining cursor. Capture does not expand hidden branches or download extra reply media; remote media may expire. [Conversation guide](features/x.md#conversation-snapshots), [collector tests](../tests/scrape/test_x_conversations.py), [CLI tests](../tests/scrape/test_x_conversation_cli.py). |
| Flattened feeds, lost figures/captions, relative URLs | Full plain-text feeds recover structure only from complete ordered text matches. Supported figures between matching sibling blocks are retained. WordPress caption containers become figures; final response URLs and valid HTML base URLs resolve links/media. | Formatted feeds stay authoritative; incomplete matches stay unchanged. Older captures without redirect metadata may need a fresh scrape. These rules do not validate every page layout. [Processing behavior](features/processing.md), [caption tests](../tests/test_readable_html.py), [redirect tests](../tests/scrape/test_rss_redirects.py). |
| Missing or failed media downloads | Failed direct HTTP downloads remove partial files; absent local files are omitted from processed assets. `files_failed` and `media_skipped` distinguish failures from intentional reference-only/disabled downloads. | Media failure alone does not trigger **Incomplete scrape**. An article may remain readable with remote images or unavailable media. Configured size limits can also prevent downloads. [Media guide](features/media.md), [asset tests](../tests/scrape/test_scrape_assets.py). |
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
