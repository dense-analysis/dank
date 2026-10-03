# Browsing and semantic search

DANK's local web viewer displays processed posts, original source links and
available downloaded media. Collect and [process](processing.md) content before
opening the viewer.

## Start the viewer

**Native**

```sh
uv run web --no-reload
```

**Docker**

```sh
make up
```

Complete [setup](../setup.md) for your chosen mode first. Both viewers are
available at [localhost:8080](http://127.0.0.1:8080) by default. Native runs use
your configured database; Compose starts its database before the viewer. The
default page size is 50; the `limit` URL parameter accepts up to 200 results.

Both modes read the checkout's `config.toml`. After editing it, stop the
native viewer with Ctrl+C and restart it, or recreate the Docker service:

**Native**

```sh
uv run web --no-reload
```

**Docker**

```sh
docker compose up -d --force-recreate dank
```

The native example disables development hot reload, which uses Linux inotify.
The Docker image also disables reload. Use `--port 8081` on native `web` to
change its port; for Docker, change the host port mapping as described in
[setup](../setup.md). The viewer has no built-in authentication and defaults
to local access in both modes.

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
