# X.com Link and Title Fixes

## Problem

Niche Gamer X posts were missing expected links in rendered content, and post titles were showing raw URLs.

The main issue for `post_id=2025214408972996647` was in X post processing:

- Tweet text had two trailing `t.co` URLs.
- One expanded to the article URL (wanted to keep).
- One was a media/self URL (wanted to remove).
- Previous logic removed all trailing `t.co` URLs, so both disappeared.

## What changed

### 1) Keep meaningful links in body HTML, remove only self/media trailing links

Updated `src/dank/process/x.py`:

- Replaced the old blanket trailing `t.co` stripper with structured cleanup:
  - `_clean_post_text(...)`
  - `_extract_url_map(...)`
  - `_replace_tco_urls(...)`
  - `_removable_trailing_urls(...)`
  - `_strip_trailing_urls(...)`
  - helpers for entity/media extraction and URL comparison
- Behavior now:
  - Expand `t.co` URLs to `expanded_url` where available.
  - Remove only trailing URLs that are:
    - media URLs, or
    - self-post links (same post URL, including `/photo/...`).
  - Keep non-self external links in post body.

### 2) Strip links from titles while keeping links in body

Updated `src/dank/process/x.py` title creation:

- `title` now comes from `_title_from_text(text)`.
- `_title_from_text(...)`:
  - takes first line,
  - unescapes HTML entities,
  - removes all `http://` and `https://` URLs,
  - normalizes whitespace.

Result:

- `post.html` keeps useful links.
- `post.title` is clean text without URL clutter.

### 3) Web rendering improvement (already applied)

Updated `src/dank/web/app.py`:

- `_sanitize_html(...)` now runs `bleach.clean(...)` and then `bleach.linkify(...)`.
- This makes plain URLs clickable in rendered post detail pages.

## Tests added/updated

Updated `tests/process/test_process_x.py`:

- Unknown trailing `t.co` remains in body (not blindly removed).
- URL expansion + trailing media stripping works.
- Trailing self-link stripping works.
- Title strips links while body keeps expanded link.

Updated `tests/web/test_web_app.py`:

- Sanitized HTML linkifies plain `https://x.com/...` URLs.

## Validation run

- `uv run pytest tests/process/test_process_x.py` -> passed
- `uv run pytest tests/web/test_web_app.py` -> passed
- `uv run ruff check src/dank/process/x.py tests/process/test_process_x.py` -> passed

## Note on existing rows

Already-processed rows do not automatically update unless reprocessed with newer raw data.

For existing posts, scrape/process again so updated logic is applied to stored rows.
