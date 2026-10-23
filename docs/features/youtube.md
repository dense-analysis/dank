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

Signed-in **Recommendations** collects through Chromium and yt-dlp in parallel
with a shared 30-second listing budget. The verified homepage is reused when
available. yt-dlp's original response renderers retain shelf ancestry, Shorts
and posts before its flat extraction removes that context. Both paths exclude
news shelves, Playables, ads, live and upcoming videos. Duplicate video/post IDs
are saved once, with the accepted method-specific renderers and shelf/rank
observations retained. Browser order comes first, followed by additional
items from yt-dlp. A timeout or failed method preserves merged results and
marks the collection partial. Startup, media, captions and indexing add time
outside the listing budget.

**Recommendations** shows all saved homepage captures, with one card per item,
ordered by when it was most recently recommended. Pagination keeps the original
capture cutoff while you browse, so a new refresh does not replace or duplicate
items on later pages. **Channels** groups this same history by distinct item
count. Hype and News remain scoped to their latest snapshots; channel feeds keep
their incremental history. Opening an item exposes its saved discovery history. In **All**, Videos appear
before Shorts and Posts; each type loads up to 500 items and has its own **Show
more** control. Individual format tabs retain infinite scrolling. **Channels**
groups the selected feed's captured items, showing the most represented channels
first and up to three recent examples per channel. Its **Configured channels
only** filter matches current channel feeds, including resolved handles/IDs,
and also applies to the other tabs. Items without a saved channel identity are
reported separately. These are counts of saved recommendations, not impressions
or a live subscription list.

Channel collection also saves the channel icon and YouTube's displayed video
total. Icons are cached locally when image downloads are enabled. Channel
counts include distinct collected videos and Shorts, excluding community
posts. YouTube may round the available total (for example, `26K videos`); the
reader preserves that display, and an unavailable total remains unknown.

Videos use landscape cards, Shorts have a separate portrait section, and
community posts retain their text and images. Opening an item shows its saved
discovery context. Playback starts only when selected. Feed snapshots are separate from saved transcripts; descriptions and comments
are not collected.

**From your feeds** gathers verified YouTube links from collected posts and
comments. Equivalent watch, short-link, embed and Shorts URLs share one video
identity, with excerpts and links back to the matching source.

After collection, DANK retrieves public YouTube previews for up to 100 linked
videos that need metadata. This saves the real video title, channel and thumbnail
without downloading video, visiting watch pages, following channels, or inventing
feed appearances. Opening a linked video can also collect its preview on demand.
Preview metadata is separate from full details such as descriptions or transcripts.

To backfill saved links without collecting feeds or starting browsers:

```sh
make scrape ARGS='--youtube-metadata'
# Collect just one known video:
make scrape ARGS='--youtube-metadata --youtube-video-id NnV_cWeoo5Q'
```

Add `MODE=native` for native execution. Repeat the backfill for more than 100
pending videos. Successful previews are refreshed after seven days; failed
requests wait one hour, and unavailable previews wait one day. Original provider
responses and previous successful metadata are retained. A failed refresh leaves
the last saved title available. Selected feed snapshots keep their captured titles;
source post headlines are never used as video titles.

## Transcripts

After scraping, DANK checks up to 25 videos or Shorts encountered in that run
for existing captions. Video downloads can stay off. It prefers English captions,
then the original language, using creator captions before automatic captions
within each language. Machine translations and live chat are excluded.

Opening a video's details shows its saved transcript. If none has been checked,
the local DANK server checks that video automatically and saves the result.
Older videos are checked individually when opened; there is no bulk backfill.
Timestamp links open the corresponding moment on YouTube.

Saved transcripts are reused without refreshing. Missing captions are cached for
one day; failed requests for one hour. Reopening after that delay checks again,
or use **Try transcript again**. The reader distinguishes unavailable captions
from failed retrieval. Caption collection is bounded to 40 seconds per video,
and a failed request stops the remaining transcript checks for that scrape.
It does not download audio, generate speech-to-text, or collect comments.

## Thumbnail and title history

Videos keep the same identity when their title or thumbnail changes. Raw
captures preserve observed titles; discovery details show their first and last
capture dates. A card retains the title from its selected feed snapshot.

Thumbnail details list each title captured with that image, with first/last dates
for the combination. A reused image can therefore have several separately dated
titles. New captures save the title alongside the image observation. Older
matches require an exact thumbnail URL and a unique title in the same completed
scrape before the image was fetched; they are labelled as matched. Ambiguous
matches, unlinked captures and fallback images do not claim a pairing. Dates
reflect observed captures, not continuous use of that title/image combination.

With image downloads enabled, thumbnails are checked again when a video is
encountered. Changed bytes are saved separately, including changes at an unchanged
URL. Identical images reuse storage, and visually similar resolutions or encodings
are grouped for display. Original files and capture provenance remain available.
Hovering or keyboard-focusing a preview crossfades its saved images without
playing video; reduced-motion mode stays still. Details also show the images
for touch and manual inspection.

A 404 thumbnail triggers alternate thumbnail URLs for the same video. Recovery
renditions are not additional discoveries. To retry saved missing thumbnails
without crawling channels again:

```sh
make scrape ARGS='--retry-youtube-thumbnails'
```

Add `--youtube-thumbnail-id VIDEO_ID` for each video to limit repair and adopt
its older cached images into the history. Add `MODE=native` for native execution.
Other HTTP failures and configured media limits remain failures or skips.

History represents what DANK observed, not every version YouTube might serve.
[YouTube supports creator-run title and thumbnail tests](https://support.google.com/youtube/answer/16391400?hl=en),
but a changed capture does not establish its cause. Incremental channel limits
still apply: older videos gain new versions only when encountered again.

## Limits and downloads

Channel, News and Hype limits apply per feed. Collection stops at the configured
item/scroll budget, after repeated scrolls without new content, or when a
channel's latest-video listing reaches a consecutive overlap with already saved items. The overlap
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

## Compare collection methods

After saving the YouTube session, run:

```sh
make youtube-compare
# Optional shorter comparison:
make youtube-compare ARGS='--timeout 30'
```

Add `MODE=native` for native execution. This diagnostic compares the browser
and flat yt-dlp baseline; normal Recommendations uses raw renderers from both.
The browser and metadata-only yt-dlp `:ytrec` collector start together with the same authenticated cookie snapshot
and region. Each has a four-minute ceiling by default. The report separates
browser warmup from collection time and records start skew, stop reasons,
unique Videos/Shorts/Posts counts, overlap, and IDs found by only one method.
Raw listing data is saved under `<storage.data_dir>/comparisons/`; credentials
and cookies are excluded. Comparison does not write to the reader library or
download media, captions or individual video details.

The browser applies the Recommendations exclusions. yt-dlp's flat output loses
shelf headings such as **Top News**, omits text-only community posts, and may
omit Shorts shelves. Its totals remain raw for an honest comparison; a larger
video count does not establish more relevant or complete capture. YouTube may
vary results between requests even with matching cookies and close start times.

## Google account and 1Password

Personal recommendations need a saved Google session. DANK keeps YouTube's
profile separate from X and Reddit. Chromium browser-account sign-in is disabled
for that profile so it does not reconcile away the saved Google website session.
Configure references to your Google login
in private `config.toml`; passwords and authenticator secrets stay in 1Password:

```toml
[youtube]
authenticated = true
homepage = true

[youtube.onepassword]
account = "YOUR_1PASSWORD_ACCOUNT_ID"
username = "op://VAULT_ID/GOOGLE_ITEM_ID/username"
password = "op://VAULT_ID/GOOGLE_ITEM_ID/password"
otp = "op://VAULT_ID/GOOGLE_ITEM_ID/one-time password?attribute=otp"
```

Use your actual field references. The `otp` field is optional; it generates a
fresh authenticator code when Google requests one. Use item IDs when several
logins share the name Google. `op account list` identifies the account to use.
The host needs [1Password CLI with desktop integration](https://developer.1password.com/docs/cli/app-integration/).
Docker's launcher also needs Python 3 on the host.

```sh
make youtube-login
# Native collection:
make youtube-login MODE=native
```

A valid saved session is reused without requesting vault access. Otherwise,
authorize 1Password once when prompted. DANK reads the configured Google item
once per login attempt and keeps its needed fields only in memory. It submits
the Google username, password
and, when offered, authenticator code automatically. It only fills the Google
sign-in origin, then closes and reopens the saved profile to verify that
YouTube remains signed in. Docker resolves credentials on the host and passes
them privately to the login process; values are not stored in configuration,
arguments, environment variables or logs.

Google may require a CAPTCHA, device approval, passkey or other challenge that
an authenticator code cannot answer. DANK reports that requirement instead of
retrying passwords. Manual browser sign-in remains an explicit option:

```sh
make youtube-login ARGS='--manual'
```

Docker manual mode prints a local browser link on port 6081; native manual
mode opens a local browser. Native and Docker profiles are separate, so use
the mode that collects your feeds. The saved profile normally lives at
`<storage.data_dir>/youtube-browser-profile`; `profile_dir` can override it.
DANK also keeps an owner-only (0600) youtube-session.json beside that
profile. Native profile authentication takes precedence. Only a completed,
signed-out homepage permits one cache restore per browser session, followed
by verification in a fresh document. Loading or uncertain pages trigger neither
restoration nor vault access. DANK refreshes the cache after verified collection,
with page scripts stopped. Keep the profile and cookie cache private; Google
passwords and authenticator seeds remain in 1Password.

Authenticated collection requires a valid saved session; it never silently
collects a signed-out homepage. Public feeds remain signed out by default.
