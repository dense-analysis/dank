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
- Article titles support opening new tabs. Source names filter to that source;
  unlinked article images open at full size in a new tab.
- Reopening an article resumes where you left off.
- Code blocks use their declared language for syntax highlighting, with copy
  controls and horizontal scrolling. Unsupported or unlabelled code stays plain.
- Tables retain merged cells, captions and row/column groups, with horizontal
  scrolling when they are wider than the reading column.
- Settings offer DM Sans, Newsreader and Georgia, plus article text size
  and a live preview. Preferences save in this browser and also apply to the
  article toolbar. The interface uses a charcoal dark theme with mint accents.

## Structure

React + TypeScript + Vite, with native CSS, Lucide icons, TanStack Query for
request caching, DOMPurify for the article boundary, and Biome for checks.
Fonts are served locally. No separate frontend application server is required
for production.

`src/components` owns presentation; `src/lib` owns API contracts, validation,
URL filters and persistence; `src/hooks` bridges browser state. The additive
Python API is in `src/dank/web/reader_api.py` and `reader_query.py` in DANK.

Keep source identity as `(domain, post id)`. Treat all collected HTML and URLs
as untrusted. Keep source selection distinct from collector configuration.
