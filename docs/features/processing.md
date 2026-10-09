# Processing and embeddings

DANK separates collecting source material from preparing it for browsing and
search. Scraping writes raw posts and asset references; processing reads those
records and writes normalised posts, local-file metadata and text embeddings.

## Run

**Native**

```sh
uv run process
uv run process --age 48h
uv run process --age 30m
```

**Docker**

```sh
docker compose run --rm dank process
docker compose run --rm dank process --age 48h
docker compose run --rm dank process --age 30m
```

The default window is `24h`. The age applies to collection time, and processing
uses the domains listed in `sources`. Durations accept seconds, minutes or
hours, including forms such as `30s`, `10minutes` and `6hours`; use `48h` for
two days.

For each post, processing selects the newest raw capture inside the window
and skips it if its collection time is not newer than the stored processed
version. Increasing `--age` includes older captures but does not force
unchanged records through an updated processor.

Use `--reprocess` to rebuild saved posts after a processing fix, without
scraping again. Limit the run with `--domains` (a regex over configured
sources) and `--age`, for example:

```sh
uv run process --reprocess --domains '^www\.theregister\.com$' --age 168h
```

The same flags work with `docker compose run --rm dank process`. Reprocessing
rebuilds post HTML and embeddings; asset processing remains incremental.
Original captures, post identities and source timestamps are preserved.
Known authors are retained when a newer capture omits the byline.

## Produced data

- RSS/Atom entries become posts using feed metadata and available article HTML.
  Entries retained after an article-fetch failure use their feed content.
- X payloads become posts with text, author, title and timestamps.
- Downloaded assets gain file size and an inferred content type. Missing local
  files are omitted from processed assets.
- Posts receive separate title and body embeddings for semantic search.

Raw payloads remain in the [database](database.md) alongside the processed
records. A post's identity is its domain and source post ID. Where a publication
time is absent, processors use a fallback timestamp.

## Embedding tools

The default model is `sentence-transformers/paraphrase-MiniLM-L3-v2`.
It loads on demand and runs locally on CPU. The model may be downloaded on first
use; it can also be cached in advance:

**Native**

```sh
uv run download-embedding-model
uv run embed-text "Example text to represent as a vector"
```

**Docker**

```sh
docker compose run --rm dank download-embedding-model
docker compose run --rm dank embed-text "Example text to represent as a vector"
```

`embed-text` prints a numeric vector. `download-embedding-model` accepts
`--model` and `--device` to initialise another model; downloading one does not
change the model used by processing or search.

The current processor embeds the first 512 characters of the title and the
first 8,192 characters of the body HTML. Model token limits also apply. These
are whole-post representations; the processor does not create passage-level
indexes or analyse the contents of downloaded images, audio or video.

Use the [web viewer](search.md) to browse or search processed posts. See the
[README](../../README.md) for setup.

## Terminal progress

Processing reports source and record counts to stdout. Embedding and database
writes also report elapsed time every five seconds while pending. Warnings
and errors go to stderr; both streams are retained in the configured log file.
The same output is available through `make process` and
`make process MODE=native`.


RSS processing preserves full feed bodies, including comment text. When a
feed supplies only a summary, it extracts the article body from the fetched
page when possible. Page headers, scripts and styles are removed from
processed content and previews. Embeddings use readable text rather than
HTML markup. Original feed XML and page HTML remain in raw storage.

When a full feed body is plain text, processing can recover paragraphs,
headings, lists and quotations from the saved article page. Only complete
blocks matching the entire feed text in order (ignoring whitespace) are
accepted. Unrelated page content is excluded. Formatted feeds and bodies
without a complete match retain their original content.

Figures and standalone images between matching text blocks in the same
article container are retained in place, including their source captions
and credits. Media in other containers or outside the matched passage is
excluded. The reader uses downloaded copies when available.

Links and media in extracted page HTML resolve against `page_final_url`,
including a valid HTML `<base href>`, so relative URLs work in the viewer.
Older captures without this metadata fall back to the original article URL;
redirected articles need a new scrape and processing run to use the final URL.
