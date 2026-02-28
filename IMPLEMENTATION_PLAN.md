# DANK Implementation Plan

This document captures the implementation plan and completion details for:

- Web view screenshot command with locked local-only headless browsing.
- Hybrid full-text + embedding search ranking in the web view.
- Faster X account scraping via in-app account switching.

## 1) Web View Screenshot Command

### Goal

Add a command that launches the web view on a specific local port, captures screenshots with headless zendriver, and allows loading those images in OpenCode sessions.

### Plan

- Add new CLI entry point: `uv run web-screenshot`.
- Start an internal temporary `aiohttp` web server on configurable host/port.
- Use existing zendriver integration to launch a headless browser and capture PNG.
- Lock navigation so browser cannot leave local loopback URLs.
- Keep normal scraper browser behavior unchanged.

### Implemented

- Added script entry: `web-screenshot = "dank.tools.web_screenshot:main"` in `pyproject.toml`.
- Added command implementation in `src/dank/tools/web_screenshot.py`.
- Added request-level lock via CDP Fetch interception.
- Added DOM-level lock for links/forms/`window.open`.
- Added host validation so only loopback bind hosts are allowed.
- Added README usage notes.
- Verified command output and image capture:
  - `uv run web-screenshot --port 8899 --route "/" --output "data/web-view-8899.png"`
  - Screenshot saved at `data/web-view-8899.png`.

## 2) Hybrid Search Ranking (Full Text + Embeddings)

### Goal

Upgrade web search so ranking combines semantic similarity with full-text relevance using ClickHouse capabilities.

### Plan

- Keep embedding-based search as the semantic backbone.
- Tokenize query into normalized search terms.
- Score title/body full-text token matches.
- Combine embedding similarity + full-text score with configured weights.
- Retain freshness boost in final ranking.

### Implemented

- Updated search scoring in `src/dank/web/app.py`.
- Added query term extraction helper `_search_terms`.
- Added weighted hybrid score:
  - embedding similarity
  - full-text token match score (title + html)
  - freshness boost
- Updated filtering to keep embedding-near matches OR full-text matches.
- Added token bloom indexes to `schema.sql` for new environments:
  - `idx_posts_title_tokens`
  - `idx_posts_html_tokens`

## 3) Faster X Account Scraping via In-Page Navigation

### Goal

Speed up scraping by reusing existing `x.com` tab state and using in-app account jump navigation instead of full page loads when possible.

### Plan

- If already on an `x.com` page and not on login flow, attempt keyboard-driven account switch:
  - `/`
  - paste/type `@account`
  - `ArrowUp`
  - `Enter`
- Use minimal waits to reduce latency.
- Confirm navigation by observing History API changes and pathname updates.
- Wait briefly for account content container.
- Fallback to direct URL navigation when in-page jump fails.

### Implemented

- Added in-page account open path in `src/dank/scrape/x/__init__.py`.
- Added history tracker injection around `pushState`/`replaceState`/`popstate`.
- Added path confirmation wait logic for `/<account>`.
- Added lightweight content readiness wait.
- Preserved fallback to direct navigation for reliability.

## 4) Browser Config Extension

### Goal

Allow specialized browser args without affecting normal scraping defaults.

### Implemented

- Extended `BrowserConfig` in `src/dank/scrape/zendriver.py` with:
  - `browser_args: tuple[str, ...] = ()`
- Used only by screenshot flow to harden/quiet headless behavior.

## 5) Tests and Validation

### Added/Updated Tests

- `tests/tools/test_web_screenshot.py`
- `tests/web/test_web_app.py`
- `tests/scrape/test_scrape_x.py`

### Validation Commands

- `./run-linters.sh` passed.
- `uv run pytest` passed (`88 passed, 31 deselected`).
