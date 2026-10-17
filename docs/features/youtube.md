# YouTube collections

DANK saves the feeds YouTube presents: videos, Shorts and community posts,
together with their discovery context. Video downloads are off by default.
No account or API key is needed for Hype, News or public channel feeds.

## Configure

Add a YouTube entry to the existing `sources` array in your private
`config.toml`:

```toml
sources = [
  { domain = "youtube.com", accounts = ["Channel4News"] },
]

[youtube]
scrape_videos = false
include_shorts = true
include_posts = true
authenticated = false
homepage = false
hype = true
news_tabs = ["top_stories", "business", "world"]
region = "GB"
max_posts = 200
max_scrolls = 20
scroll_pause_seconds = 1.5
stall_scrolls = 3
known_overlap = 5
```

Keep your other source entries. A bare `"youtube.com"` enables the default
Hype and News feeds without choosing any channels. `accounts` accepts channel
handles; `feed_urls` can specify channel video pages or playlists, for example
`https://www.youtube.com/@Channel4News/videos`.

News tabs can include `top_stories`, `sports`, `entertainment`, `business` and
`world`. These are requested feed destinations; availability and headings can
change on YouTube. Set `hype = false` or `news_tabs = []` to omit those feeds.

To collect an additional country's Hype playlist, configure its explicit URL.
The current region still uses the discovered **View all** playlist; an explicit
entry for that same region replaces discovery. `hype = false` disables both.

```toml
[youtube.hype_playlists]
US = "https://www.youtube.com/playlist?list=OLAK5uy_lSbPjCHn-9LSQeRc6T1LWfz-DN8gOqNdw"
```

[YouTube says](https://support.google.com/youtube/answer/15334500?hl=en) Hype
rankings update about every few minutes and are shared within each country.
Its normal interface only exposes the viewer's country. An explicit playlist
lets DANK collect a separately discovered destination; changing `region` alone
is not a verified way to choose another country's Hype. Playlist URL longevity
is unverified. Saved ranks, IDs and capture times support comparison across
runs; see the [follow-up](../known-issues.md#youtube-feed-discovery).

## Collect and read

```sh
make scrape ARGS='--domains "^youtube\.com$"'
```

Add `MODE=native` for native execution. Open
[YouTube in the reader](http://127.0.0.1:8080/reader/?view=youtube).
Feed discoveries are readable immediately; normal processing also prepares
them for the combined collection and semantic search.

Hype follows the **Top 100 → View all** playlist and saves its regional context,
playlist identity and captured rank. News expands available **Show more**
sections. Each appearance retains its feed, tab, shelf heading, position,
region, observation time and original source renderer. The same video can
therefore have several discoveries without becoming several video identities.

A selected recommendation feed shows its latest captured snapshot in source
order. Pagination stays on that snapshot if another collection finishes while
you read. Channel views retain earlier items as their collection advances;
opening an item exposes its saved discovery history.

Channel collection also saves the channel icon and YouTube's displayed video
total. Icons are cached locally when image downloads are enabled. Channel
counts include distinct collected videos and Shorts, excluding community
posts. YouTube may round the available total (for example, `26K videos`); the
reader preserves that display, and an unavailable total remains unknown.

Videos use landscape cards, Shorts have a separate portrait section, and
community posts retain their text and images. Opening an item shows its saved
discovery context. Playback starts only when selected. Feed-only captures do
not claim to contain transcripts, descriptions or comments.

**From your feeds** gathers YouTube links from collected blog, X and Reddit
posts. Equivalent watch, short-link, embed and Shorts URLs share one video
card, with links back to the collected posts that referenced it. A video does
not need to be downloaded or directly collected from YouTube to appear here.

## Limits and downloads

Limits apply per feed. Collection stops at the configured item/scroll budget,
after repeated scrolls without new content, or when a channel's latest-video
listing reaches a consecutive overlap with already saved items. The overlap
rule applies only to channels, because known videos can still reveal a new
recommendation order elsewhere. Set `known_overlap` to adjust that threshold.

Livestreams and scheduled/upcoming broadcasts are excluded. Shorts and
community-post switches apply globally. Individual watch pages and comment
threads are not visited during feed collection.

`scrape_videos = true` enables the existing yt-dlp media downloader for collected
videos, subject to `[media].download_types` and the asset size limit. Thumbnail
downloads follow the image setting independently. See [media controls](media.md).

Collection is bounded, not a promise that every item on an infinite feed was
captured. A changed layout, consent page or unavailable feed is reported in
scrape logs/history. See [known issues](../known-issues.md).

## Optional account session

```sh
make youtube-login
# Or: make youtube-login MODE=native
```

Complete sign-in and verification yourself in the browser. Docker prints a
local sign-in link on port 6081. The command saves YouTube's separate profile;
no password belongs in the configuration. Native and Docker sessions are
separate, so sign in in the mode you will use for collection.

Then set `[youtube].authenticated = true`. Enable `homepage = true` to also
capture your homepage recommendations. A requested authenticated collection
requires a valid saved session rather than silently collecting a signed-out
page. The default remains signed out, using a temporary profile on every run.

The saved profile normally lives at
`<storage.data_dir>/youtube-browser-profile`; `profile_dir` can override it.
YouTube has its own browser and collection lock, so it can run concurrently
with X and Reddit. Keep profiles private; they contain session credentials.
