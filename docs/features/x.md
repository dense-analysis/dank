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

The command first checks the saved X session. If it is still valid, it reports
"Already signed in to X" and finishes without opening the sign-in view or
requesting 1Password access.

When sign-in is required, follow the printed link to
[the sign-in browser](http://127.0.0.1:6080/vnc.html?autoconnect=1&resize=scale).
Complete sign-in and any verification there. The command confirms
"X sign-in complete. Session saved" and closes the temporary browser. Docker needs no browser or Python installation on the host.
The viewer can keep running while you sign in.
Use the clipboard panel in the sign-in view to paste into the remote browser.

**Native**

```sh
make x-login MODE=native
```

This opens a local Chromium window to check the saved session. If it is valid,
the command reports that it is reusing the session and closes the window.
Otherwise, complete sign-in and verification when prompted.

Sign-in waits up to ten minutes. Use `ARGS='--timeout 1200'` for more time,
or Ctrl+C to cancel. Native users can also run `uv run x-login --config PATH`.
No database connection or configured source is needed to sign in. The window
fits the available display. DANK keeps Chromium's normal site isolation and
WebRTC behavior, and uses the browser's own user agent and language.

The profile lives under `<storage.data_dir>/browser-profile`. Docker sign-in
and scraping use the same `dank_data` volume, so the session survives container
removal and restarts. Use the same Compose project and configuration for both.
Native and Docker profiles are separate: sign in in the mode you will scrape.
Only one X sign-in or scrape may use that profile at a time. Reddit uses a
separate browser and profile, so it can collect while X is waiting.

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

## Optional 1Password autofill

If you have [1Password CLI](https://developer.1password.com/docs/cli/), enable
its [desktop app integration](https://developer.1password.com/docs/cli/app-integration/)
and copy the secret references for your X login into configuration:

```toml
[x.onepassword]
username = "op://Personal/X/username"
password = "op://Personal/X/password"
```

Use your actual vault, item and field references. Run `make x-login` as usual,
or add `MODE=native`. 1Password is requested only when sign-in is needed.
Authorize it when prompted. It fills each visible,
empty X username/password field once; you click Continue or Sign in yourself.
Existing input is preserved, and verification codes remain manual.

Docker uses `op` on the host and sends the values through standard input to the
one-off container. No 1Password installation or vault access is needed inside
Docker. Use the Make command for this integration; direct Compose commands
continue to support manual sign-in.

Without references, without `op`, or when access is denied, sign-in remains
manual. Use `ARGS='--manual'` to skip 1Password for a run. Resolved credentials
are kept in memory and are not written to config, environment variables,
command-line arguments or logs. This does not clear an X login restriction.

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
