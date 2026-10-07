# Media downloads

DANK discovers media while collecting [RSS articles](rss.md) and [X
posts](x.md). Downloads happen during scraping; a later processing run records
metadata for files that are available locally.

## Supported assets

| Asset | Handling |
| --- | --- |
| Images and video posters | Downloaded over HTTP |
| Direct audio and video files | Downloaded over HTTP |
| Recognised YouTube embeds | Downloaded with yt-dlp |
| Other iframes and external links | Retained as references |

The YouTube downloader prefers an MP4 format and falls back to the best
available combined format. It also requests a thumbnail and metadata JSON.
When available, it uses the saved Chromium profile's cookies and detected
Node or Deno runtimes.

Downloads share the run's media-job limit and direct HTTP limits with RSS
collection. A direct media 429 also pauses new requests to that host; the
failed media download itself is not retried in that run. See
[concurrency settings](history.md#concurrency).

## Configure storage

```toml
[storage]
data_dir = "data"
max_asset_bytes = 10485760
```

Files are stored under `<data_dir>/assets/<domain>/<post_id>/`. HTTP filenames
use a hash of the full URL, including query parameters, to avoid collisions
between CDN resources. Existing records keep their paths; older filenames
may be downloaded again once into the new layout. Native runs
use the host path in `storage.data_dir`; a relative `data` path is beneath
your working directory. Docker resolves `data` to `/app/data` in its persistent
volume. See [setup](../setup.md) for configuration and storage differences.

The example sets a 10 MiB limit for each media file. Omitting
`max_asset_bytes`, or setting it to zero or a negative value, removes the
configured size limit. It is not a total disk or run-wide download budget.

HTTP downloads check both declared size and bytes received, and remove partial
files after failure. The YouTube path passes the limit to yt-dlp and also checks
the downloaded media file. Its additional metadata and thumbnail files are not
covered by a total storage budget.

## Collect and view

**Native**

```sh
uv run scrape --headless
uv run process --age 24h
uv run web --no-reload
```

**Docker**

```sh
docker compose run --rm dank scrape --headless
docker compose run --rm dank process --age 24h
docker compose up -d dank
```

Raw asset records keep the source URL, post association and local path. A
skipped or unsuccessful download can have an empty local path. Processing
includes only assets whose local files exist, adding their size and inferred
content type.

The [web viewer](search.md) shows processed images, audio and video alongside
posts. Back up the asset directory together with the database if you want to
preserve the downloaded files and their associations.

See the [README](../../README.md) for setup and shared settings.
