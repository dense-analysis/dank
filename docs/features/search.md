# Browsing and semantic search

DANK's local web viewer displays processed posts, original source links and
available downloaded media. Collect and [process](processing.md) content before
opening the viewer.

## Start the viewer

```sh
make up
```

Open [localhost:8080](http://127.0.0.1:8080). Compose publishes the viewer on
`127.0.0.1:8080` and starts it after ClickHouse is ready. It shows 50 posts per
page by default; the `limit` URL parameter accepts up to 200 results.

Configuration lives in the `config.toml` generated beside `compose.yaml`.
After editing it, recreate the viewer:

```sh
docker compose up -d --force-recreate dank
```

The image starts the web server without development hot reloading. See
[setup](../setup.md) for storage and port settings. The viewer has no built-in
user authentication and is published only on the local host by default.

## Browse and filter

With the search box empty, posts appear newest first, with Previous and Next
links for navigation. A post's detail page displays its stored content and
locally available assets. The Source link opens the original URL.

Filters work for both browsing and search:

| Filter | Behaviour |
| --- | --- |
| Domain | Exact domain match, such as `example.com` |
| Account / author | Case-insensitive substring match on the author field |
| Days back | Restrict by post creation time; `0` means all dates |

The date slider covers up to 365 days. Displayed timestamps use UTC. The text
preview on each card is an excerpt of the stored content.

## Search by meaning

Enter a phrase to find posts with similar text embeddings. DANK compares the
query with each post's title and body vectors, weighting the title at 65% and
the body at 35%. Newer posts receive an additional ranking boost.

Search returns the best matches up to the selected result limit. Previous and
Next navigation applies to chronological browsing, not ranked search results.
Query embeddings are cached in ClickHouse with a six-hour expiry policy.

Ranking reflects text similarity and freshness. It does not measure whether
a source is reliable or a statement is correct; use the original source links
to inspect the material.

See [processing](processing.md) for model behaviour and the
[README](../../README.md) for setup.
