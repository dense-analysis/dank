# Browsing and semantic search

DANK's reader displays processed posts, original source links and available
downloaded media. Collect and [process](processing.md) content before opening
it.

## Start the reader

**Native**

```sh
make web MODE=native
```

**Docker**

```sh
make up
```

Complete [setup](../setup.md) for your chosen mode first. Both modes serve the
reader at [localhost:8080](http://127.0.0.1:8080), which opens
[`/reader/`](http://127.0.0.1:8080/reader/). Native runs use your configured
database; Compose starts its databases before the web app.

Both modes read the checkout's `config.toml`. After editing it, stop the
native web app with Ctrl+C and start it again, or recreate the Docker service:

**Native**

```sh
make web MODE=native
```

**Docker**

```sh
make web ARGS='--force-recreate dank'
```

Native `make web` and the Docker image both run without hot reload. Pass
`ARGS='--port 8081'` to native `make web` to change its port; for Docker, change
the host port mapping as described in [setup](../setup.md). The web app has no
built-in authentication and defaults to local access in both modes.

## Browse and filter

Stories appear newest first and load as you scroll. Source, tag, category,
author and date filters apply to browsing and search alike, and stay in the
URL. Opening a story shows its stored content and locally available media; its
source link opens the original URL. Displayed timestamps use UTC. The
[reader's behaviour](../../reader/README.md#behaviour) covers every filter and
view.

## Explore source connections

Open **Connections** on a saved article or post to switch between the evidence
list and graph. Incoming links show which saved posts or comments cited this
item; outgoing links show the destinations it cites. Select a node
to inspect the saved excerpt, author and capture date, then **Open link in DANK**
or **Open comment in DANK** to reveal the match. Collected neighbours can become the centre
of the next graph; uncollected destinations retain their external URL.

Connections come from explicit saved links, not inferred similarity. Each view
shows up to 100 incoming and 100 outgoing occurrences, with the full totals.
Repeated captures of the same post/comment link count once, using its latest
saved evidence. A link from an older capture can remain useful after the current
post changes: DANK labels that evidence with its capture date and can show the
archived excerpt if the current reader content no longer contains the match.
Missing comments, uncaptured pages and unsupported page layouts limit coverage.

## Search by meaning

Search puts stories containing every search term first, then posts with similar
text embeddings. For those, DANK compares the query with each post's title and
body vectors, weighting the title at 65% and the body at 35%. Newer posts
receive an additional ranking boost.

Ordered by relevance, search shows up to 30 best matches and labels the cap;
newest and oldest orders keep loading as you scroll. Query embeddings are cached
in ClickHouse with a six-hour expiry policy.

Ranking reflects text similarity and freshness. It does not measure whether
a source is reliable or a statement is correct; use the original source links
to inspect the material.

See [processing](processing.md) for model behaviour and the
[README](../../README.md) for setup.
