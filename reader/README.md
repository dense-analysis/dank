# DANK reader

DANK's new reader frontend: compact stories, focused articles, Words / Meaning
search, source filters, bookmarks and named feeds. The original viewer remains
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

For a single server, run `npm run build`, then start DANK. The same web process
serves the built reader at `/reader/`. The existing Docker image does not build
the reader; build it locally and mount `reader/dist` at `/app/reader/dist` if
using a container.

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
- **Words** matches every whitespace-separated term literally, ignoring case,
  in article titles or text. **Meaning** uses DANK's embedding model.
- Date bounds include the entire selected UTC calendar day. Newest and oldest
  orders paginate; relevance shows up to 30 best matches and labels the cap.
- Feeds save definitions, so reopening or refreshing them fetches matching
  collected posts. They do not add sources for collection.
- Feeds, bookmarks and read markers live in this browser's local storage.
  Bookmarks retain collected article content. They are not synced across devices;
  the app reports storage failures. Search and filter state lives in the URL.
- Articles open above the existing timeline. Closing, Escape and browser Back
  preserve filters, loaded pages and scroll position.
- Briefings, scheduling and delivery are future work. They have no placeholder
  controls in this reader.

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
