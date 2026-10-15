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

## Reddit

When Reddit communities are configured, a Reddit section appears below All
stories. Expand its community list to browse each subreddit, including those
with no collected posts yet. Reddit stays separate from the main story feed.
Community icons are downloaded with collection and appear beside community names.
If an icon is unavailable, the community initial is shown. **Filters → Flair**
shows flairs found in collected posts for the selected community, or across all
communities in the Reddit view. Select multiple flairs to match any of them;
flair choices combine with search, author and date filters. Shared flair labels
match across communities. Click a flair badge on a post or article to open
that community’s feed filtered to that flair.

Search, author and date filters apply within the current Reddit view; article
links and browser navigation preserve that view. Scores, comment counts and
flair reflect the collected post. Comments themselves are not collected.
Community icons are cached during scraping and appear in the sidebar, cards
and article headers, with initials when unavailable. Filters offer the flairs
observed in collected posts: community-specific choices in community views,
and combined matching labels across the Reddit collection. Selected flairs
match any selected label and remain in the URL through search and navigation.

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
