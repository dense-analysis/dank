# Scrubbing old captures and media

`scrub` removes old rows from `raw_posts` and `raw_assets`, and retires old
downloaded media. Processed `posts` and `assets` rows are retained, including
post HTML, authors, publication dates, embeddings and assigned categories.
Other tables, including training examples, collection evidence, scrape history,
YouTube discoveries, playlists, metadata and transcripts, are retained.

Preview the operation first. Native commands are:

```sh
make scrub MODE=native ARGS='--age 30d --dry-run'
make scrub MODE=native ARGS='--age 30d'
```

For Docker, omit `MODE=native`. `--config PATH` selects another configuration.
Specify a positive age in seconds, minutes, hours or days. Scrubbing covers
the configured database and asset directory, including sources that have since
been disabled or removed.

The dry run reports the cutoff, selected raw-row counts, file count and up to
how many GB of allocated file space can be reclaimed. It writes no database
rows, creates no tables and removes no files. Normal logging and the collection
lock still apply.

## Age and repeated posts

Raw rows expire when their `scraped_at` is strictly before the UTC cutoff.
Rows exactly at the cutoff remain.

Media expires from its earliest recorded successful capture. `asset_retention`
preserves that timestamp when raw rows are removed. Rediscovering unchanged
media on a pinned post keeps its original age.

An intentionally retired asset is remembered by source domain, post ID and
media URL. Future scrapes retain its reference and count an intentional media
skip. A different media URL remains eligible for collection. The marker
persists across changes to the retention age. DANK treats media behind the
same URL as the same file, consistent with HTTP cache reuse.

On the first scrub, existing raw and processed asset records supply the first
known capture times. Unreferenced files use their modification time. A yt-dlp
video and its sidecar files use the video's capture age together, so a source's
old Last-Modified date does not discard a newly downloaded video. Orphans and
abandoned temporary files older than the cutoff can also be removed.

## Preserved files and playback

Scrubbing applies to regular files within `data_dir/assets` and rejects a
symlinked asset directory. Source icons, saved Reddit and YouTube channel
icons, and files referenced by saved YouTube thumbnail history are preserved.

The reader omits local media whose files no longer exist. YouTube items use the
existing YouTube embed after the user presses Play. Retired YouTube attachments
in articles offer **Play from YouTube**. An already-open YouTube collection
player can report a missing file and offer remote playback. Embedding depends
on YouTube's availability and the video's permissions. Other retired media
may be unavailable.

Deleting raw captures removes their reprocessing source and raw-only reader
details, such as archived discussions and X For You appearances. Processed post
text remains available in All and source listings. Provenance and YouTube
history retain the fields they recorded. Process old captures before scrubbing
if you want to keep their processed posts.

## Locking and recovery

Scrubbing takes the same exclusive collection lock as a full scrape, and refuses
to run while a CLI or reader collection owns it. Retirement records are saved
before files or raw rows are removed. Rerun an interrupted scrub to finish it.
Saved retirement decisions prevent immediate download and deletion loops.

Raw-table deletion waits for the local ClickHouse mutation to finish. Obsolete
ClickHouse parts can remain until the server's background cleanup, so database
disk space may not fall immediately. Hardlinked files release space after the
last link is removed. Open file handles can also delay reclamation.

Total storage depends on how much media arrives within the retention window.
Per-file [media limits](media.md) still apply to new downloads.
