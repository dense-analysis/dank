# DANK reader

DANK's new reader frontend: compact stories, focused articles, combined search,
source filters and reading preferences. The original viewer remains
at `/`; the new reader lives at `/reader/`. Both belong to this repository.

## Develop

Use Node 22.12+ and npm. From this directory:

```sh
npm ci
npm run dev
```

Run DANK from the checkout root in another terminal:

```sh
uv run web --no-reload --port 8091
```

Open [localhost:5174/reader/](http://127.0.0.1:5174/reader/).
The development server proxies the reader API and downloaded media to DANK on
port 8091. Configure DANK normally before starting it; see the main setup guide.

For a single native server, run `npm run build`, then start DANK. The same web
process serves the reader at `/reader/`. Docker builds and includes the reader
automatically; open `/reader/` on the normal DANK server.

## Install on a phone

Open `/reader/` using a trusted HTTPS address reachable from your phone. In
Android Chrome, choose **Menu → Install and create shortcut → Install**.
DANK then opens from its icon in a standalone window, without browser controls.
Plain HTTP on a LAN IP does not enable this installation; the phone must trust
the HTTPS certificate.

The installed reader needs the DANK server to stay running and reachable. On a
local network, keep the host awake and the phone on that network. Reopen or
refresh DANK after deploying updates. Offline reading is not included.

## Checks

```sh
npm run check       # formatting, lint, unit tests, strict types, production build
npm run test:e2e    # browser acceptance with isolated fixture data
```

Browser tests use Google Chrome. Install Chrome or adjust the Playwright
browser channel. No live collection is needed for the fixture tests.

## Behaviour

Presentation should be consistent across sources even when collection methods
such as RSS, APIs and browser scraping differ. Equivalent content uses shared
preview rules, while source identity and unavailable content remain explicit.
Link-only Reddit posts, empty RSS aggregator entries and link-only X previews
can reuse an already saved publisher excerpt. Exact URLs and observed redirects
establish the match. A post's own text and captured X descriptions take priority;
ambiguous links, quotes and unavailable publisher content keep their existing
presentation. This does not fetch articles or change where Reddit cards open.

- Source tags select publishers, not article topics. Choices within a source or
  tag group are OR; groups, author, dates and search combine with AND.
- Search combines word matches with related ideas. Relevance puts stories
  containing every search term first, then related results from DANK's embedding
  model. Source, tag, author and date filters apply to both.
- Search previews show matching passages and highlight the search terms.
- Date bounds include the entire selected UTC calendar day. Newest and oldest
  orders paginate; relevance shows up to 30 best matches and labels the cap.
- Scrolling near the bottom loads more stories automatically. If loading fails,
  existing stories stay visible and the next page can be retried.
- Author names apply an exact name filter while preserving the current view's
  search and filters. Author and date chips can be removed individually; date
  shortcuts select today or the past 7 / 30 UTC calendar days, including today.
- Read markers, article positions and reading preferences stay in this browser.
  Search and filter state lives in the URL.
- Articles have a dedicated reading page with a shareable URL. Back to stories
  and browser Back restore filters, loaded pages, keyboard focus and scroll
  position; browser Forward returns to the article.
- Stories without collected article content have a “Not collected” badge and
  a separate notice linking to the original. Opening them does not mark them read.
- Stories with available text but a failed article-page fetch have an
  “Incomplete scrape” badge and notice: text, formatting or media may be missing.
  The badge clears after a successful page retry is processed.
- Feed cards open their primary article or post from the surrounding space as
  well as their title. Cmd-click (Ctrl-click on Windows/Linux) and middle-click
  open background tabs, keeping the feed and its filters in place. Clickable
  areas keep the hand cursor; text selection and nested controls stay independent.
  This applies in All, Blogs, X, Reddit, Hacker News and YouTube.
  Source names filter to that source;
  unlinked article images open at full size in a new tab.
- Transparent article images offer a **Light background** toggle on hover or
  keyboard focus, always visible on touch screens. Toggle it again to restore
  the reader background. Images whose pixels cannot be inspected are left alone.
- **Automatically improve image backgrounds** is off by default in Settings.
  Enable it to give sparse transparent figures a light background when contrast
  clearly improves. Mixed or uncertain images keep their original background;
  the per-image toggle overrides the automatic choice while the article is open.
- **Media previews** in Settings independently fit image and video frames to
  their aspect ratios on Reddit and X. Both are on by default; turn either off
  for wider frames. The full media remains visible, and choices stay in this browser.
- **Raw media links** in Settings independently controls duplicate image/gallery
  URLs and video URLs on Reddit and X, including YouTube video previews in X posts.
  Both hiding options are on by default. Turn either off to show those links
  alongside previews; the choices stay in this browser.
- Social images and videos fit within 480px or 60% of the viewport height,
  whichever is smaller, with the full attachment visible.
- Footers share sizing and spacing across All and source feeds. Wide columns
  keep metrics or article details on the left and read/source icons on the right.
  At 760px or narrower, metrics spread from the content’s
  left edge with larger labels and icons; the read marker and source link stay
  compact at the right. This also applies beside the sidebar and in split windows.
- Posts and articles show minutes or hours ago for today, then the local date
  and time from yesterday onward. Hover a timestamp for its exact time.
  YouTube's dedicated view keeps the relative publication labels from YouTube;
  the mixed All feed uses the shared post date style.
- Reopening an article resumes where you left off.
- Code blocks use their declared language for syntax highlighting, with copy
  controls and horizontal scrolling. Unsupported or unlabelled code stays plain.
- Tables retain merged cells, captions and row/column groups, with horizontal
  scrolling when they are wider than the reading column.
- Settings offer DM Sans, Newsreader and Georgia, plus article text size
  and a live preview. Preferences save in this browser and also apply to the
  article toolbar. The interface uses a charcoal dark theme with mint accents.

## Collections

**All** combines collected blogs, X posts, Reddit discussions and Hacker News.
When a Hacker News story links to a collected article, All shows the original
source once. **Links** on feed cards lists collected posts referencing
that item. Open any article or post and choose **Connections → Graph** to explore
saved incoming and outgoing links, or **List** to read the matching excerpts.
Post links and comment mentions remain distinct. Select a node to inspect its
evidence, open the exact highlighted link or comment in DANK, or explore a
connected post's graph. Known videos open their YouTube details; uncollected
destinations have an external link. **Back to connections** restores the graph,
filter and viewport within the tab; copied links retain a return destination.
Counts cover saved evidence, with explicit limits for large neighborhoods.
Historical links retain their capture dates and show an archived excerpt if
the matching passage is absent from the current post. These are observed links,
not inferred topic or semantic relationships.

Collected linked articles show their publication date when available
and explicitly label missing dates. Hacker News keeps its own daily listing and discussions.

**Blogs**
shows articles; **X** and **Reddit** each have their own feed and expandable
sidebar list. Search, source filters and dates apply within the selected view.

Sidebar groups remember whether you expanded or collapsed them in this browser,
including YouTube's nested lists, across navigation and page refreshes.
On phones, groups start collapsed except for the active feed. Navigation stays
within reach while scrolling; tap outside the drawer to close it. Saved group
choices still take precedence.

Expand **X** to choose a configured account, including accounts with no posts
yet. Each entry uses the account’s display name, collected post count and captured profile image,
with an initial when no image is available. X profile images load from its image
host; Reddit icons use cached local assets. Account selection follows the post's
handle, independently of the author filter, and stays in the URL through search
and article navigation.

The frosted account header shows the display name and handle, a back arrow to
all X posts, and a collected / total posts pill. The total is X's reported count
from the saved profile, not a live count; hover the pill for exact numbers.
When no total was captured, only the collected count appears. Search and filters
do not change these account-wide counts.

X feeds default to **Posts**, excluding known replies, including self-replies.
Choose **Posts & replies** to include all collected posts and replies. Both
use the selected date/search order; latest first remains the default. The
selection stays in the URL through search, article navigation and browser Back.
Unknown reply metadata does not hide a post, and account counts still cover all
collected posts. The mixed All feed continues to include collected replies.

Replies link to their saved parent, or to X when the parent is not collected.
Opening a post connects independently saved parent context and replies, even
without a separate discussion capture. Short corrections are included. These
local relationships do not imply complete conversation coverage; archived
capture status, counts and dates remain separate. Related posts are deduplicated
by identity and retain their actual reply branches.

X cards and opened posts show the account's avatar, display name and handle,
with the post text once. Feed previews show about 300 characters, preserving
paragraphs and links. **Show more** expands ordinary posts completely. Posts of
roughly 500 words or 3,000 characters expand to about 1,200 characters, with
**Read full post** and an estimated reading time leading to the complete reader.
These thresholds keep feed previews manageable. The text area animates open
and closed. **Show more** keeps your reading position steady as text opens below. **Show less** also scrolls smoothly to the post's top, below the
sticky header. Reduced motion makes both changes instant. Media players
retain their playback position. Replies
without a separate reader destination can still expand completely. Identity
links open the account's collected posts, with initials when an avatar is unavailable.

Quoted posts appear inside a bordered card with the original account, text and
attachments. Select the embedded card to open its saved post; media controls and
text expansion work independently. Quotes without a standalone saved post link
to X, and missing content is explicit. Quotes also appear in captured replies.

Captured links retain their destinations and preview cards. Photos, native
videos and GIFs appear as attachments, preferring local files when available.
Portrait videos keep their natural proportions within the available space.
Raw photo and video references are hidden while their attachments are shown
and return if the image or player fails. Raw URLs represented by clickable preview cards, including
YouTube cards, are also hidden. Descriptive links and links without previews
remain available. YouTube previews play inline when selected, with a separate
YouTube source icon beside **Remote video - YouTube** for opening the video
on its own site. Closing the video
or navigating to another view stops playback. X, YouTube and Reddit videos pause
when scrolled fully out of view or when another video starts. Their position is
preserved; scrolling back does not automatically resume them. Opening a post and
returning to the feed keep the latest position for each video, including a
backward seek. Press **Play** to continue from that point. Video positions stay
in this open reader tab; reloading the page clears them.

Remote media is labelled and video plays only on demand. Click anywhere on a post
card to open it, or use its text or timestamp link. Cmd-click (Ctrl-click on
Windows/Linux) opens the post in a background tab with the current feed filters.
Clickable card areas keep the hand cursor. Account links, inline links, Show more
and media controls work independently; selecting text keeps the feed open.
The footer shows captured comments, reposts, likes and views, with a dash for
unavailable counts. Opening a post shows full content, saved parent context,
and captured replies in thread order.
Author continuations are labelled, and missing reply parents are explicit.

**Partial conversation** means a limit, interrupted request, unexpanded branch
or missing context prevented a complete capture. Saved reply counts stay
separate from X's reported counts. Older posts show **Replies not collected**;
failed or disabled collection has its own status. A failed refresh keeps the
previously saved conversation and its capture date. **View conversation on X**
opens the current conversation. Collection runs with the X scraper, not when
opening a post; see [X collection](../docs/features/x.md) for limits and targeted
capture of a saved post.

## Reddit

Expand Reddit's community list to browse each configured subreddit, including
those with no collected posts yet.

A community's name and cached icon stay visible in a compact sticky header.
The same frosted header includes a back arrow to Reddit and its collected count.
After collection, it also shows an approximate community post total from Arctic
Shift's archive, marked **≈**. Hover the count for exact numbers, provenance,
the archive count's update date and when DANK retrieved it. These totals count
archived posts and may differ from Reddit's current public posts. A failed refresh
keeps the last saved estimate and its dates; communities without an estimate show
only the collected count. Search and filters do not change community-wide counts.
Its cards omit the repeated community identity; the combined Reddit feed keeps
community names and icons on each card. Search identifies its current scope,
such as **Search r/LocalLLaMA**, **Search Reddit**, or the selected source.

**Filters → Flair** offers labels found in collected posts for that community,
or combined labels across all communities. Multiple selected flairs match any
of them and combine with search, author and date filters. Click a flair badge
on a post or article to open that community's feed with just that flair selected.
Article links and Back navigation preserve the active filters.

Scores appear as an up arrow and number; scores, comment counts and flairs are
collected snapshots. Saved comments appear beneath a post in their collected
order, with replies indented and a link to Reddit when more remain. Older posts
and skipped or failed comment collections show an explicit notice.

Reddit videos play on demand in feed cards and opened posts, including audio
when provided by the source. Only one video plays at a time across All; starting
a Reddit, X or YouTube video pauses the previous player.
Downloaded videos play locally; remote videos stream from Reddit and carry a
small label. Raw video URLs represented by a player are hidden in cards and
opened posts; descriptive links remain. Failed players restore the raw link.
Raw image and gallery links are hidden when a clickable image preview represents
them. Missing or failed images keep their original links available.
Portrait players use poster proportions before playback and the video's actual
dimensions once loaded. Captions use **Remote video - Reddit**, with the globe
icon. Native videos share the post’s original-source link; a video from another
site keeps its own source icon beside the caption. If playback fails, the post’s
source link and restored raw video link remain available.

Settings can hide captured comments or remote video labels in this browser.
Collection options, including video downloads and comment limits/sort, remain
in the collector configuration. Community icons use initials when a cached
image is unavailable.

The reader orders stored posts by date or search relevance. Reddit collection
defaults to Top for the past day; source configuration can change the sort
and time window before scraping. Refreshing this view reloads collected posts.

## Hacker News

**Hacker News** browses collected hckr news stories with **Top 10**, **Top 20**,
**Top 50%** (default), and **All**. Selections use hckr news's daily score cutoffs,
including ties. They filter saved content; changing the selection does not collect
additional stories. Chronological views separate front-page days in UTC and load
older stories as you scroll. Search, dates, the daily selection and article Back
navigation stay in the URL.

Cards show the submitter, captured points and comment counts, plus separate original-article and
Hacker News discussion links. When a submission has no body, its card uses an
excerpt from the saved publisher article, with the same formatting and search
preview rules as blog cards. HN's own text takes precedence; unavailable or empty
publisher content leaves the excerpt absent. Reading the feed makes no new
publisher requests. Story titles open the collected original article when
available; the comment count opens saved Hacker News comments. The discussion also
links back to the collected article. Timestamps show the original Hacker News submission time;
the day headings continue to group front-page arrivals. Opening a story shows any saved post text and comments
from a detailed scrape, with replies indented. Full saved discussions have no reader comment cap. Normal collection skips comments;
not captured, partial and failed captures are explicit. Counts are saved snapshots.
See [Hacker News collection settings](../docs/features/hacker-news.md).

## YouTube

**YouTube** saves the context of discovery: Hype rankings, News topics, channel
feeds, Shorts and community posts. Its expandable sidebar groups **News** by
Top stories, Business and World, **Hyped** by country, and **Channel Feeds** by
channel. News opens Top stories; Hyped opens the UK feed when configured.
Topic and country tabs, search, and Videos / Shorts / Posts filters stay in the URL.

Channel headers share the frosted back-button and avatar layout used by X and
Reddit. Cached icons fall back to initials. The count compares collected videos
(including Shorts, excluding community posts) with YouTube's last reported
channel total; rounded source totals are marked approximate. Unknown totals
stay absent, and search or format filters do not change these channel-wide counts.
Rank badges appear on Hype cards; channel cards have no ranking.

When a video has multiple saved thumbnails, hover or keyboard-focus its preview
to gently cycle through them. This does not start a video. Reduced-motion mode
keeps the preview still. Discovery details show the saved images and distinct
observed titles with their capture dates; title changes never create another
video card. Each thumbnail lists its associated titles with first/last dates for
that specific combination. Older pairings are labelled when matched from saved
captures; uncertain or recovered-image pairings remain unspecified. The card keeps the title from the selected feed snapshot.

Landscape videos, portrait Shorts and community posts have distinct layouts.
Select a card, title or its information link to inspect the captured shelf, rank,
region, playlist and collection time. **Feed snapshot** means the feed entry is
saved; it does not claim transcripts or comments were collected. The collector
configuration controls feed selection, Shorts and video downloads.

Playback starts only when selected. Downloaded videos play locally; other videos
connect to YouTube, with a separate link to the original. Playback pauses when
scrolled away or another player starts, and its position survives navigation
between the collection and discovery details in this reader tab.

**From your feeds** combines verified YouTube links in collected posts and
comments. Discovery details distinguish **Post link** from **Comment mention**,
with a matching excerpt, capture date, exact video URL (including timestamps)
and a direct link to the source post or comment. **Open comment in DANK**
opens the saved discussion, scrolls to the matching link, and highlights its
exact visible label. This temporarily reveals hidden comments without changing
your preference. **Original comment** opens the source site. Shared reader URLs
retain the post, comment and video identity; Back and Forward preserve the
YouTube search and selection. If the comment or link is missing from the current
saved post, DANK shows the archived excerpt and original link when available
instead of highlighting another comment. Counts describe distinct
post/comment references, so multiple comments in one thread remain distinct.
Videos without a collected YouTube title use their video ID; a source headline
is never presented as the video's title. Opening a linked video collects its
public preview details when missing, with visible progress and cached retries.
The title, channel and thumbnail are saved once per video and shared by all
references. A configured YouTube feed or video download is not required.

Switch between **List** and **Graph** to inspect the same verified evidence.
The graph follows sources through posts and comments to the shared video;
select a node for its evidence and reader links. Pan, zoom, Fit and Reset work
with mouse, touch and keyboard. The view is part of the URL, and recent graph
selection and viewport survive reader navigation within the tab. No semantic
or topic relationships are inferred by this view.

See [YouTube collection settings](../docs/features/youtube.md).

## Structure

React + TypeScript + Vite, with native CSS, Lucide icons, TanStack Query for
request caching, DOMPurify for the article boundary, and Biome for checks.
Fonts are served locally. No separate frontend application server is required
for production.

`RelationshipGraph` renders domain-neutral nodes and edges using lazily loaded
Cytoscape.js. `youtubeReferenceGraph` and `postConnectionsGraph` map verified
source evidence into that model. `PostConnections` owns loading and view state;
its navigation callbacks keep article routing outside the graph renderer.
Additional relationship views can supply their own adapters.

`src/components` owns presentation; `src/lib` owns API contracts, validation,
URL filters and persistence; `src/hooks` bridges browser state. The additive
Python API is in `src/dank/web/reader_api.py` and `reader_query.py` in DANK.

The article adapter sanitizes imported HTML and creates hosts for React portals.
Image toggles and code-copy controls own their state in React; image analysis is
isolated in `src/lib/imageAnalysis.ts` with sources for its contrast formulas.

Keep source identity as `(domain, post id)`. Treat all collected HTML and URLs
as untrusted. Keep source selection distinct from collector configuration.
