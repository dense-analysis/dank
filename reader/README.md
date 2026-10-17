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

## Checks

```sh
npm run check       # formatting, lint, unit tests, strict types, production build
npm run test:e2e    # browser acceptance with isolated fixture data
```

Browser tests use Google Chrome. Install Chrome or adjust the Playwright
browser channel. No live collection is needed for the fixture tests.

## Behaviour

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
- Article titles support opening new tabs. Source names filter to that source;
  unlinked article images open at full size in a new tab.
- Transparent article images offer a **Light background** toggle on hover or
  keyboard focus, always visible on touch screens. Toggle it again to restore
  the reader background. Images whose pixels cannot be inspected are left alone.
- **Automatically improve image backgrounds** is off by default in Settings.
  Enable it to give sparse transparent figures a light background when contrast
  clearly improves. Mixed or uncertain images keep their original background;
  the per-image toggle overrides the automatic choice while the article is open.
- Reopening an article resumes where you left off.
- Code blocks use their declared language for syntax highlighting, with copy
  controls and horizontal scrolling. Unsupported or unlabelled code stays plain.
- Tables retain merged cells, captions and row/column groups, with horizontal
  scrolling when they are wider than the reading column.
- Settings offer DM Sans, Newsreader and Georgia, plus article text size
  and a live preview. Preferences save in this browser and also apply to the
  article toolbar. The interface uses a charcoal dark theme with mint accents.

## Collections

**All** combines collected blogs, X posts and Reddit discussions. **Blogs**
shows articles; **X** and **Reddit** each have their own feed and expandable
sidebar list. Search, source filters and dates apply within the selected view.

Expand **X** to choose a configured account, including accounts with no posts
yet. Each entry uses the account’s display name, collected post count and captured profile image,
with an initial when no image is available. X profile images load from its image
host; Reddit icons use cached local assets. Account selection follows the post's
handle, independently of the author filter, and stays in the URL through search
and article navigation.

X cards and opened posts show the account's avatar, display name and handle,
with the post text once. Short posts are readable in the feed; **Show more**
expands long notes. Identity links open the account's collected posts, with
initials when an avatar is unavailable.

Captured links retain their destinations and preview cards. Photos, native
videos and GIFs appear as attachments, preferring local files when available.
Portrait videos keep their natural proportions within the available space.
The raw video reference is hidden while its player is shown and returns if
playback fails. Raw URLs represented by clickable preview cards, including
YouTube cards, are also hidden. Descriptive links and links without previews
remain available. YouTube previews play inline when selected, with a separate
**Watch on YouTube** link for videos that cannot be embedded. Closing the video
or navigating to another view stops playback. X videos and YouTube players pause
when scrolled fully out of view or when another video starts. Their position is
preserved; scrolling back does not automatically resume them.

Remote media is labelled and video plays only on demand. Select the post text
or timestamp to open it; inline links and media controls work independently.
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

Reddit videos play on demand, including audio when provided by the source.
Downloaded videos play locally; remote videos stream from Reddit and carry a
small label. If a stream becomes unavailable, use **Watch on Reddit**.

Settings can hide captured comments or remote video labels in this browser.
Collection options, including video downloads and comment limits/sort, remain
in the collector configuration. Community icons use initials when a cached
image is unavailable.

The reader orders stored posts by date or search relevance. Reddit collection
defaults to Top for the past day; source configuration can change the sort
and time window before scraping. Refreshing this view reloads collected posts.

## Structure

React + TypeScript + Vite, with native CSS, Lucide icons, TanStack Query for
request caching, DOMPurify for the article boundary, and Biome for checks.
Fonts are served locally. No separate frontend application server is required
for production.

`src/components` owns presentation; `src/lib` owns API contracts, validation,
URL filters and persistence; `src/hooks` bridges browser state. The additive
Python API is in `src/dank/web/reader_api.py` and `reader_query.py` in DANK.

The article adapter sanitizes imported HTML and creates hosts for React portals.
Image toggles and code-copy controls own their state in React; image analysis is
isolated in `src/lib/imageAnalysis.ts` with sources for its contrast formulas.

Keep source identity as `(domain, post id)`. Treat all collected HTML and URLs
as untrusted. Keep source selection distinct from collector configuration.
