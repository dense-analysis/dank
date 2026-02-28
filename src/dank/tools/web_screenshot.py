from __future__ import annotations

import argparse
import asyncio
import json
import logging
import pathlib
from typing import Any, NamedTuple
from urllib.parse import urlsplit

import zendriver
from aiohttp import web
from zendriver import cdp

from dank.config import BrowserSettings, Settings, load_settings
from dank.scrape.zendriver import BrowserConfig, BrowserSession
from dank.web.app import (
    DEFAULT_HOST,
    DEFAULT_PAGE_SIZE,
    MAX_PAGE_SIZE,
    create_app,
)

logger = logging.getLogger(__name__)

DEFAULT_WEB_SCREENSHOT_PORT = 8799
DEFAULT_SCREENSHOT_WIDTH = 1440
DEFAULT_SCREENSHOT_HEIGHT = 1800
DEFAULT_WAIT_SECONDS = 0.35
ALLOWED_BIND_HOSTS = frozenset(
    {
        "127.0.0.1",
        "localhost",
        "::1",
        "0.0.0.0",
        "::",
    },
)


class WebScreenshotConfig(NamedTuple):
    config_path: str
    bind_host: str
    navigate_host: str
    port: int
    route: str
    output_path: pathlib.Path
    width: int
    height: int
    wait_seconds: float
    page_size: int
    full_page: bool


class _LocalOnlyRequestGuard:
    def __init__(
        self,
        tab: zendriver.Tab,
        *,
        allowed_hosts: frozenset[str],
        allowed_port: int,
    ) -> None:
        self._tab = tab
        self._allowed_hosts = allowed_hosts
        self._allowed_port = allowed_port
        self._started = False

    async def start(self) -> None:
        if self._started:
            return

        tab: Any = self._tab
        tab.add_handler(cdp.fetch.RequestPaused, self._on_request_paused)
        await self._tab.send(
            cdp.fetch.enable(
                patterns=[
                    cdp.fetch.RequestPattern(
                        url_pattern="*",
                        request_stage=cdp.fetch.RequestStage.REQUEST,
                    ),
                ],
            ),
        )
        self._started = True

    async def stop(self) -> None:
        if not self._started:
            return

        tab: Any = self._tab
        tab.remove_handlers(cdp.fetch.RequestPaused, self._on_request_paused)

        try:
            await self._tab.send(cdp.fetch.disable())
        except Exception:
            logger.debug("Unable to disable fetch interception", exc_info=True)

        self._started = False

    async def _on_request_paused(self, event: cdp.fetch.RequestPaused) -> None:
        request_url = event.request.url

        if _is_allowed_request_url(
            request_url,
            allowed_hosts=self._allowed_hosts,
            allowed_port=self._allowed_port,
        ):
            await self._tab.send(
                cdp.fetch.continue_request(request_id=event.request_id),
            )
            return

        logger.info("Blocked non-local screenshot request url=%s", request_url)
        await self._tab.send(
            cdp.fetch.fail_request(
                request_id=event.request_id,
                error_reason=cdp.network.ErrorReason.BLOCKED_BY_CLIENT,
            ),
        )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="dank.tools.web_screenshot")
    parser.add_argument(
        "--config",
        default="config.toml",
        help="Path to config.toml",
    )
    parser.add_argument(
        "--host",
        default=DEFAULT_HOST,
        help="Bind host (loopback hosts only)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_WEB_SCREENSHOT_PORT,
        help="Port to run the temporary web view server on",
    )
    parser.add_argument(
        "--route",
        default="/",
        help="Route path to render, for example '/?q=dank'",
    )
    parser.add_argument(
        "--output",
        default="data/web-view.png",
        help="Path to write the PNG screenshot",
    )
    parser.add_argument(
        "--width",
        type=int,
        default=DEFAULT_SCREENSHOT_WIDTH,
        help="Browser viewport width",
    )
    parser.add_argument(
        "--height",
        type=int,
        default=DEFAULT_SCREENSHOT_HEIGHT,
        help="Browser viewport height",
    )
    parser.add_argument(
        "--wait",
        type=float,
        default=DEFAULT_WAIT_SECONDS,
        help="Seconds to wait after initial load",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help="Post page size for the temporary web app",
    )
    parser.add_argument(
        "--full-page",
        action="store_true",
        help="Capture the full scrollable page",
    )
    args = parser.parse_args(argv)

    try:
        config = _build_config(
            config_path=args.config,
            host=args.host,
            port=args.port,
            route=args.route,
            output=args.output,
            width=args.width,
            height=args.height,
            wait=args.wait,
            page_size=args.limit,
            full_page=args.full_page,
        )
    except ValueError as error:
        parser.error(str(error))

    screenshot_path = run_web_view_screenshot(config)
    print(f"Saved screenshot: {screenshot_path}")


def run_web_view_screenshot(config: WebScreenshotConfig) -> pathlib.Path:
    return asyncio.run(_run_web_view_screenshot(config))


async def _run_web_view_screenshot(
    config: WebScreenshotConfig,
) -> pathlib.Path:
    settings = load_settings(config.config_path)
    target_url = _target_url(
        host=config.navigate_host,
        port=config.port,
        route=config.route,
    )
    output_path = config.output_path.expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    app = create_app(settings, page_size=config.page_size)
    runner = web.AppRunner(app)
    allowed_hosts = frozenset(
        {
            config.navigate_host,
            "localhost",
            "127.0.0.1",
            "::1",
        },
    )

    await runner.setup()

    try:
        site = web.TCPSite(runner, host=config.bind_host, port=config.port)
        await site.start()
        await _capture_with_locked_browser(
            settings,
            target_url=target_url,
            output_path=output_path,
            width=config.width,
            height=config.height,
            wait_seconds=config.wait_seconds,
            full_page=config.full_page,
            allowed_hosts=allowed_hosts,
            allowed_port=config.port,
        )
    finally:
        await runner.cleanup()

    return output_path


async def _capture_with_locked_browser(
    settings: Settings,
    *,
    target_url: str,
    output_path: pathlib.Path,
    width: int,
    height: int,
    wait_seconds: float,
    full_page: bool,
    allowed_hosts: frozenset[str],
    allowed_port: int,
) -> None:
    browser_config = _browser_config(settings.browser, settings.data_dir)

    async with BrowserSession(browser_config) as session:
        browser = await session.get_browser()

        try:
            page = browser.main_tab
        except Exception:
            page = await browser.get("about:blank")

        await page.set_window_size(width=width, height=height)
        await _install_navigation_guard(
            page,
            allowed_hosts=allowed_hosts,
            allowed_port=allowed_port,
        )
        request_guard = _LocalOnlyRequestGuard(
            page,
            allowed_hosts=allowed_hosts,
            allowed_port=allowed_port,
        )
        await request_guard.start()

        try:
            page = await page.get(target_url)
            await _wait_for_ready_state(page)

            if wait_seconds > 0:
                await page.sleep(wait_seconds)

            await page.save_screenshot(
                filename=str(output_path),
                format="png",
                full_page=full_page,
            )
        finally:
            await request_guard.stop()


def _browser_config(
    browser_settings: BrowserSettings,
    data_dir: pathlib.Path,
) -> BrowserConfig:
    return BrowserConfig(
        headless=True,
        browser_executable_path=(
            str(browser_settings.executable_path)
            if browser_settings.executable_path
            else None
        ),
        connection_timeout=browser_settings.connection_timeout,
        connection_max_tries=browser_settings.connection_max_tries,
        keep_open=False,
        profile_dir=data_dir / "web-screenshot-profile",
        browser_args=(
            "--disable-background-networking",
            "--disable-default-apps",
            "--disable-sync",
            "--metrics-recording-only",
            "--no-first-run",
            "--no-default-browser-check",
        ),
    )


async def _install_navigation_guard(
    page: zendriver.Tab,
    *,
    allowed_hosts: frozenset[str],
    allowed_port: int,
) -> None:
    source = _navigation_guard_script(
        allowed_hosts=allowed_hosts,
        allowed_port=allowed_port,
    )
    await page.send(cdp.page.add_script_to_evaluate_on_new_document(source))

    try:
        await page.evaluate(source)
    except Exception:
        # This runs before initial navigation and can fail on empty pages.
        logger.debug("Unable to evaluate guard script before navigation")


def _navigation_guard_script(
    *,
    allowed_hosts: frozenset[str],
    allowed_port: int,
) -> str:
    hosts_json = json.dumps(sorted(allowed_hosts))

    return (
        "(() => {"
        "if (window.__dankWebViewGuardInstalled) { return true; }"
        "window.__dankWebViewGuardInstalled = true;"
        f"const allowedHosts = new Set({hosts_json});"
        f"const allowedPort = {int(allowed_port)};"
        "const normalizedPort = (url) => {"
        "if (url.port) { return Number.parseInt(url.port, 10); }"
        "if (url.protocol === 'https:' || url.protocol === 'wss:') {"
        "return 443;"
        "}"
        "if (url.protocol === 'http:' || url.protocol === 'ws:') {"
        "return 80;"
        "}"
        "return -1;"
        "};"
        "const isAllowed = (target) => {"
        "let parsed;"
        "try {"
        "parsed = new URL(target, window.location.href);"
        "} catch (_error) {"
        "return false;"
        "}"
        "if (parsed.protocol === 'about:' || parsed.protocol === 'data:' "
        "|| parsed.protocol === 'blob:') {"
        "return true;"
        "}"
        "if (!allowedHosts.has(parsed.hostname.toLowerCase())) {"
        "return false;"
        "}"
        "return normalizedPort(parsed) === allowedPort;"
        "};"
        "document.addEventListener('click', (event) => {"
        "if (!(event.target instanceof Element)) { return; }"
        "const anchor = event.target.closest('a[href]');"
        "if (!anchor) { return; }"
        "const href = anchor.getAttribute('href') || '';"
        "if (!isAllowed(href)) {"
        "event.preventDefault();"
        "event.stopPropagation();"
        "}"
        "}, true);"
        "document.addEventListener('submit', (event) => {"
        "if (!(event.target instanceof HTMLFormElement)) { return; }"
        "const action = event.target.getAttribute('action') "
        "|| window.location.href;"
        "if (!isAllowed(action)) {"
        "event.preventDefault();"
        "event.stopPropagation();"
        "}"
        "}, true);"
        "const originalOpen = window.open.bind(window);"
        "window.open = (...args) => {"
        "const href = args.length > 0"
        " ? String(args[0])"
        " : window.location.href;"
        "if (!isAllowed(href)) {"
        "return null;"
        "}"
        "return originalOpen(...args);"
        "};"
        "return true;"
        "})()"
    )


def _is_allowed_request_url(
    url: str,
    *,
    allowed_hosts: frozenset[str],
    allowed_port: int,
) -> bool:
    parsed = urlsplit(url)
    scheme = parsed.scheme.lower()

    if scheme in {"about", "data", "blob"}:
        return True

    if scheme not in {"http", "https", "ws", "wss"}:
        return False

    host = (parsed.hostname or "").lower()

    if host not in allowed_hosts:
        return False

    if parsed.port is not None:
        return parsed.port == allowed_port

    default_port = 443 if scheme in {"https", "wss"} else 80

    return default_port == allowed_port


async def _wait_for_ready_state(page: zendriver.Tab) -> None:
    try:
        await page.wait_for_ready_state(timeout=2)
    except TimeoutError:
        # The web app can keep network connections open for assets.
        return


def _build_config(
    *,
    config_path: str,
    host: str,
    port: int,
    route: str,
    output: str,
    width: int,
    height: int,
    wait: float,
    page_size: int,
    full_page: bool,
) -> WebScreenshotConfig:
    bind_host = host.strip().lower()

    if bind_host not in ALLOWED_BIND_HOSTS:
        allowed = ", ".join(sorted(ALLOWED_BIND_HOSTS))
        raise ValueError(f"--host must be one of: {allowed}")

    if port <= 0:
        raise ValueError("--port must be greater than zero")

    normalized_route = route.strip()

    if normalized_route.startswith("http://") or normalized_route.startswith(
        "https://",
    ):
        raise ValueError("--route must be a local path, not an absolute URL")

    if not normalized_route:
        normalized_route = "/"

    if not normalized_route.startswith("/"):
        normalized_route = "/" + normalized_route

    if width <= 0 or height <= 0:
        raise ValueError("--width and --height must both be greater than zero")

    if wait < 0:
        raise ValueError("--wait cannot be negative")

    if page_size <= 0:
        page_size = DEFAULT_PAGE_SIZE
    elif page_size > MAX_PAGE_SIZE:
        page_size = MAX_PAGE_SIZE

    navigate_host = _navigate_host(bind_host)

    return WebScreenshotConfig(
        config_path=config_path,
        bind_host=bind_host,
        navigate_host=navigate_host,
        port=port,
        route=normalized_route,
        output_path=pathlib.Path(output),
        width=width,
        height=height,
        wait_seconds=wait,
        page_size=page_size,
        full_page=full_page,
    )


def _navigate_host(bind_host: str) -> str:
    if bind_host in {"0.0.0.0", "::"}:
        return "127.0.0.1"

    return bind_host


def _target_url(*, host: str, port: int, route: str) -> str:
    host_part = f"[{host}]" if ":" in host else host

    return f"http://{host_part}:{port}{route}"


if __name__ == "__main__":
    main()
