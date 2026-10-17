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

[x.conversation]
enabled = true
max_posts_per_account = 5
max_replies = 50
max_scrolls = 3
timeout_seconds = 30
```

Sign in once using the command below. DANK reuses the saved browser session;
X passwords and email credentials are not required in configuration.

The scraper visits accounts in order. `max_posts` is a per-account stopping
threshold, so the final batch can exceed it. `max_scrolls` bounds scrolling;
collection can also stop after repeated idle scrolls. These settings control
the collection attempt, not a guarantee of complete account history.

Normal scrapes collect timelines only. Refresh discussions separately with
`--x-details`; timeline-only captures retain earlier conversation archives in the
reader instead of replacing them with empty discussions.

Browser startup/navigation, response drains and each scroll step have a
30-second response budget. A timeout or lost browser connection closes the
owned browser and retries that account once with the same saved profile.
Already-saved posts and asset references are filtered from the retry. A second
failure records a failed/partial account and continues with the next one.
Account progress and outcomes are logged explicitly; a zero-post response is
reported as unavailable, not silently treated as a successful empty timeline.

Sign-in requirements and a busy profile stop X without automatic login retries.
Browser cleanup has a 20-second budget; if it cannot close safely, X stops
rather than opening another browser on the same profile. Saved posts remain
available in every case; other sources can continue.

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
Tweet JSON. ID-only objects containing only `__typename: Tweet` and `rest_id`
are preserved as raw references and counted separately in logs and run history.
They do not consume the post limit or suppress a later full capture of the same
ID. `raw_posts.is_reference` is a computed flag that also classifies older rows
without rewriting their payloads. Processing and discussion refreshes select
content snapshots before choosing the latest capture, so a newer reference
cannot hide an earlier full post. Other sparse posts keep their existing handling.

DANK also discovers media and links from the captured payloads.
[Media downloads](media.md) save supported files locally; external links are
recorded as references.

[Processing](processing.md) produces post text, author, timestamps and
embeddings for the [web viewer](search.md). Check `dank.log`, or your configured
log file, for account progress and login failures.

## Reading saved posts

X cards and opened posts show the account identity and one post body. Processing
prefers full long-form text, preserves paragraphs and expands captured links.
Photos, playable video/GIF files and external link cards use saved metadata;
available local files are preferred, with captured X media URLs as a fallback.
Remote media may expire or become unavailable. A link card remains a usable
external link when its image cannot load; previews never embed third-party pages.

Reply, repost, like and view counts describe the same saved capture as the
post body. Missing counts remain unknown, not zero.

## Conversation snapshots

Discussion refreshes are a separate, explicit pass:

```sh
make scrape ARGS='--x-details'
make process ARGS='--domains "^x\.com$" --age 24h'
```

Add `MODE=native` for native execution. `--x-details` selects configured X
accounts, uses saved captures from the last 24 hours, and refreshes up to
`max_posts_per_account` newest pending owned posts per account. Latest snapshots
already marked captured or partial are left alone; use `--x-post` below to
explicitly revisit one. No account timelines are visited during this pass.
`--x-details` cannot be combined with other detail/retry modes.

The collector captures replies and parent context from X's detail responses.
The reader shows saved context before the selected post and replies below it.
Conversation posts retain their identity, text, links and media references;
reply media uses remote URLs rather than starting extra downloads. A discussion
timeout saves its failure/partial snapshot and restarts the browser before the
next post. A sign-in requirement stops further refreshes. Earlier useful
archives remain available after failed attempts and normal timeline scrapes.

Each conversation has a `timeout_seconds` budget and at most `max_scrolls`
scrolls. `max_replies` bounds the total archived replies **and parent context**.
The accepted ranges are 1–100 posts per account, 1–200 related posts,
0–20 scrolls and 1–120 seconds. `enabled = false` stops new conversation
capture without deleting existing snapshots.

The reader distinguishes a captured snapshot, a partial snapshot, a failed
attempt and replies not yet collected. Partial means X still supplied a
pagination cursor or unexpanded branch, a limit or timeout was reached, parent
context was missing, or reported replies exceeded the replies saved. Reported
counts and capture times describe that snapshot, not the current conversation.
A missing or mismatched detail response is a failure, never an empty thread.
Newer failed attempts retain the earlier saved conversation in the reader.

DANK uses the existing signed-in browser and read-only navigation; it does not
post, like, follow, or expand hidden branches. X's relevance ordering, restricted
or deleted posts and unexpanded branches can leave gaps. Open the original
conversation on X for current or missing replies.

To revisit a particular **already saved** post without crawling account feeds:

```sh
uv run scrape --headless --x-post https://x.com/example/status/123456789
uv run process --domains '^x\.com$' --age 1h
```

Repeat `--x-post URL` for several saved posts. X and Twitter status URLs are
accepted; `--domains` and `--retry-failed-pages` cannot be combined with it.
Docker users can pass the same options to `docker compose run --rm dank scrape`
and `docker compose run --rm dank process`. No new source or login is needed
when the saved X session remains valid.

## Rebuild saved posts

Previously saved X posts can be rebuilt without collecting them again:

```sh
uv run process --domains '^x\.com$' --age all --reprocess
```

For Docker, use `docker compose run --rm dank process` with the same options.
Reprocessing rebuilds text and embeddings from saved captures; it does not
fetch missing media, replies or newer content. Unknown source dates retain the
capture date so replaying a post does not move it to the top of the timeline.

See the [README](../../README.md) for shared configuration.
