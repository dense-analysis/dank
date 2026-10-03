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

## Configure storage

```toml
[storage]
data_dir = "data"
max_asset_bytes = 10485760
```

Files are stored under `<data_dir>/assets/<domain>/<post_id>/`. Relative paths
are resolved from the directory where DANK runs. The default data directory is
`data`.

The example sets a 10 MiB limit for each media file. Omitting
`max_asset_bytes`, or setting it to zero or a negative value, removes the
configured size limit. It is not a total disk or run-wide download budget.

HTTP downloads check both declared size and bytes received, and remove partial
files after failure. The YouTube path passes the limit to yt-dlp and also checks
the downloaded media file. Its additional metadata and thumbnail files are not
covered by a total storage budget.

## Collect and view

```sh
uv run scrape
uv run process --age 24h
uv run web --no-reload
```

Raw asset records keep the source URL, post association and local path. A
skipped or unsuccessful download can have an empty local path. Processing
includes only assets whose local files exist, adding their size and inferred
content type.

The [web viewer](search.md) shows processed images, audio and video alongside
posts. Back up the asset directory together with the database if you want to
preserve the downloaded files and their associations.

See the [README](../../README.md) for setup and shared settings.
