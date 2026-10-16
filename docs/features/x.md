# X account collection

DANK collects posts from configured X accounts using a Chromium-based browser
controlled by zendriver. It reads timeline data from the browser's network
responses while visiting and scrolling account pages.

## Configure

Add account handles to `config.toml` in the checkout root.
Create it using the [native or Docker setup](../setup.md); these source
settings apply to both modes:

```toml
sources = [{ domain = "x.com", accounts = ["example", "another_account"] }]

[x]
max_posts = 200
max_scrolls = 20
scroll_pause_seconds = 1.5
```

Sign in once using the command below. DANK reuses the saved browser session;
X passwords and email credentials are not required in configuration.

The scraper visits accounts in order. `max_posts` is a per-account stopping
threshold, so the final batch can exceed it. `max_scrolls` bounds scrolling;
collection can also stop after repeated idle scrolls. These settings control
the collection attempt, not a guarantee of complete account history.

## Sign in

**Docker**

```sh
make x-login
```

Open [the sign-in browser](http://127.0.0.1:6080/vnc.html?autoconnect=1&resize=scale).
Sign in to X there and complete any verification. The command confirms
"X session saved" and closes the temporary browser when your signed-in home
page is detected. Docker needs no browser or Python installation on the host.
The viewer can keep running while you sign in.
Use the clipboard panel in the sign-in view to paste into the remote browser.

**Native**

```sh
make x-login MODE=native
```

This opens a local Chromium window. Complete sign-in and verification there;
the command confirms success and closes the window automatically.

Sign-in waits up to ten minutes. Use `ARGS='--timeout 1200'` for more time,
or Ctrl+C to cancel. Native users can also run `uv run x-login --config PATH`.
No database connection or configured source is needed to sign in. The window
fits the available display. DANK keeps Chromium's normal site isolation and
WebRTC behavior, and uses the browser's own user agent and language.

The profile lives under `<storage.data_dir>/browser-profile`. Docker sign-in
and scraping use the same `dank_data` volume, so the session survives container
removal and restarts. Use the same Compose project and configuration for both.
Native and Docker profiles are separate: sign in in the mode you will scrape.
Only one sign-in or browser scrape may use a profile at a time.

Keep the profile private: it contains session credentials. The Docker sign-in
view is available only on localhost and only while the command is running.
Do not expose port 6080 to a network. An existing valid session is confirmed
and reused automatically.

Older `[x].username`, `[x].email`, `[x].password` and `[email]` settings are
ignored. Remove them from existing configuration; DANK no longer submits
passwords or polls your mailbox for codes.

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

## Temporary login limits

If X reports that it has temporarily limited login, DANK stops sign-in and
closes the browser. Wait for X's restriction to clear before trying again.
Changing browser settings or restarting DANK does not clear that restriction.
DANK does not submit credentials or retry login for you.

Docker still needs Chromium's `--no-sandbox` launcher under the supplied
container permissions. Native Chrome uses its normal sandbox. The dedicated
profile and its existing cookie storage are preserved across upgrades.

## Expired sessions

Collection checks the saved sign-in before visiting accounts. If X requires
sign-in or verification again, collection records a login-required failure and
asks you to run `make x-login` (or `make x-login MODE=native`). Complete that
step, then rerun collection. Signing in does not automatically start a scrape.

## Collected data

Raw posts retain their X post ID, URL, timestamps, request URL and extracted
Tweet JSON. DANK also discovers media and links from the captured payloads.
[Media downloads](media.md) save supported files locally; external links are
recorded as references.

[Processing](processing.md) produces post text, author, timestamps and
embeddings for the [web viewer](search.md). Check `dank.log`, or your configured
log file, for account progress and login failures.

See the [README](../../README.md) for shared configuration.
