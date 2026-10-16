# Known collection and processing issues

Use this register to understand collection-quality problems, the handling
already in place, and the gaps that remain. Detailed commands live in the
linked feature guides; live outcomes live in [run history and logs](features/history.md).

Last reviewed: **2026-10-16**, against the implementation on `main`.
Publisher examples describe observed captures, not a claim that a site always fails. “Mitigated”
means a fallback exists; “handled” means the specific case has a tested fix.

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
| `x.com` | Login/session challenges or confirmation requirements can stop timeline collection. | Session-only collection; `make x-login` saves sign-in in the same profile used by native or Docker scraping. Expired or challenged sessions count as a fetch failure and stop X collection. [X guide](features/x.md), [collector](../src/dank/scrape/x/__init__.py). | **Mitigated.** The RSS incomplete flag does not describe X collection failures. Use run history and logs. Complete verification in the sign-in browser. A visible temporary-login-limit message stops sign-in immediately; do not retry until X clears the restriction. X may still reject automated browsers or change its authenticated navigation markers. Session expiry, cleanup and profile locking are covered in `tests/scrape/test_x_session.py` and `tests/scrape/test_browser_session.py`; browser defaults and limit handling are tested locally without live login attempts. Actual X authentication remains unverified while the reported login restriction is active. |
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
| Large HTTP header fields | Direct HTTP requests allow header fields up to 16 KiB, including redirect responses. | Larger fields still fail. This is a parser-limit fix, not an access-control workaround. [Regression tests](../tests/scrape/test_http_header_limits.py). |
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
