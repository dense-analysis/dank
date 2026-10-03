# X account collection

DANK collects posts from configured X accounts using a Chromium-based browser
controlled by zendriver. It reads timeline data from the browser's network
responses while visiting and scrolling account pages.

## Configure

Add account handles and login details to `config.toml` in the checkout root.
Create it using the [native or Docker setup](../setup.md); these source
settings apply to both modes:

```toml
sources = [{ domain = "x.com", accounts = ["example", "another_account"] }]

[x]
username = "your-username"
email = "you@example.com"
password = "your-password"
max_posts = 200
max_scrolls = 20
scroll_pause_seconds = 1.5
```

`email` is used if X asks for account confirmation during login. The browser
profile is kept under `<storage.data_dir>/browser-profile`, allowing browser
session data to persist between runs.

The scraper visits accounts in order. `max_posts` is a per-account stopping
threshold, so the final batch can exceed it. `max_scrolls` bounds scrolling;
collection can also stop after repeated idle scrolls. These settings control
the collection attempt, not a guarantee of complete account history.

## Run

**Native**

```sh
uv run scrape --headless --domains '^x\.com$'
uv run process --age 24h
```

**Docker**

```sh
docker compose run --rm dank scrape --headless --domains '^x\.com$'
docker compose run --rm dank process --age 24h
```

Both examples use `--headless`. Native runs can omit it for a visible browser;
install a Chromium-based browser and use its host path if setting
`browser.executable_path`. Docker includes Chromium and its own launcher.
`browser.connection_timeout` and `browser.connection_max_tries` can help with
slow browser startup.

## Email confirmation codes

DANK includes optional IMAP polling for one-time codes:

```toml
[email]
host = "imap.example.com"
username = "you@example.com"
password = "your-imap-password"
port = 993
```

When X requests a code, the poller looks for recent messages from `x.com`.
If a required code is unavailable or email is unconfigured, the scraper stops
with a login-required message. This integration still needs live validation
against an actual confirmation email.

## Collected data

Raw posts retain their X post ID, URL, timestamps, request URL and extracted
Tweet JSON. DANK also discovers media and links from the captured payloads.
[Media downloads](media.md) save supported files locally; external links are
recorded as references.

[Processing](processing.md) produces post text, author, timestamps and
embeddings for the [web viewer](search.md). Check `dank.log`, or your configured
log file, for account progress and login failures.

See the [README](../../README.md) for shared configuration.
