# Media downloads

DANK discovers media while collecting [RSS articles](rss.md) and [X
posts](x.md). Downloads happen during scraping; a later processing run records
metadata for files that are available locally.

Processing recognizes PNG and WebP signatures for downloaded images whose
filenames have no known type, allowing the reader to use their local copies. Other
unrecognized formats remain unclassified. Reprocessing article HTML alone
does not collect images found only inside embedded article data.

## Supported assets

| Asset | Download type | Handling |
| --- | --- | --- |
| Images, X photos and video posters | `image` | HTTP |
| Direct audio files | `audio` | HTTP |
| Direct videos and X video/animated clips | `video` | HTTP |
| Recognised YouTube embeds | `video` | yt-dlp |
| Other iframes and external links | — | References only |

YouTube downloads use H.264 video and AAC audio, merged into MP4 with FFmpeg.
The collector prefers the best complete format that fits the configured size
budget, including smaller video/audio combinations. Missing compatible formats,
unavailable videos and access restrictions remain failures; size exclusions are
skips. Failed requests are not repeated automatically with browser cookies removed.

The matching `yt-dlp-ejs` solver package is pinned alongside yt-dlp. Docker
includes Node and FFmpeg. Native downloads need FFmpeg and Node 22+ or a supported
Deno runtime on `PATH`; see the [official runtime guide](https://github.com/yt-dlp/yt-dlp/wiki/EJS).
When available, downloads use the saved Chromium profile's cookies. Metadata JSON
and, when `image` is enabled, a thumbnail are retained beside successful media.

Downloads share the run's media-job limit and direct HTTP limits with RSS
collection. A direct media 429 also pauses new requests to that host; the
failed media download itself is not retried in that run. See
[concurrency settings](history.md#concurrency).

## Choose downloads

Add this to your private `config.toml` for image downloads only. It applies to
RSS and X in both native and Docker runs:

```toml
[media]
download_types = ["image"]
```

Omitting the setting allows all three types: `["image", "audio", "video"]`.
Use `[]` to disable media downloads while still collecting posts and media
references. Values are case-insensitive; duplicates are ignored and unknown
values are rejected. `audio` covers direct audio files; it does not extract
soundtracks from videos.

Types come from discovery metadata. When restricting types, unclassified media
is retained as a reference. Disabled types do not invoke HTTP or yt-dlp, including
for cached files. Their new raw references have an empty `local_path`; existing
files and previously processed media remain available. Re-enabling a type makes
it eligible when rediscovered in a future scrape.

Terminal summaries and `media_skipped` in [run history](history.md) count these
intentional skips separately from download failures. Each run also records
`media_download_types` so comparisons can use the same download policy.

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
files after failure or a size exclusion. A known oversized estimate, HTTP header,
stream or completed media file counts as `media_skipped`, not `files_failed`.
The post and its media reference are retained, and the log states the size-limit
reason. These exclusions alone do not make the run partial. Genuine HTTP,
network and filesystem errors remain failures.

Reddit preview downloads preserve the original escaped signed URL, including
through redirects; rewriting its query can invalidate the signature and return
403. Other HTTP URLs retain the client's normal URL handling.

YouTube format selection reserves 2% of the limit for MP4 overhead. When sized
formats exist, it chooses a fitting pair or skips; it does not switch to an
unmeasured stream to avoid a known exclusion. If all available sizes are unknown,
it attempts the download with an actual-byte guard across video and audio.
The completed MP4 must also fit the limit. Size exclusions from selection,
yt-dlp's header check, streaming or the final file check all count as skips.

Media is downloaded in an isolated temporary directory and published only after
completion. Failed or size-excluded attempts remove their temporary files.
Successful videos are cached under a full-URL hash inside the post's asset folder;
other files in that folder cannot be mistaken for the requested video. Legacy
paths remain valid but may be downloaded once into the new cache layout.
A lower cap skips an oversized cached video without deleting the older file.
Metadata/thumbnail files and temporary muxing overhead are not covered by a total
disk or network-transfer budget.

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
