"""Bounded public-page recovery independent of social browsers."""

from __future__ import annotations

import asyncio
import logging
import re
from contextvars import ContextVar
from typing import NamedTuple
from urllib.parse import urlsplit

from zendriver import cdp

from dank.html_utils import is_page_challenge
from dank.scrape.metrics import count
from dank.scrape.zendriver import BrowserSession, NetworkCapture

logger = logging.getLogger(__name__)
# These public layouts were verified to load in ordinary signed-out Chromium.
# Keep other denials (including subscriber pages) as explicit failures.
RECOVERY_TIMEOUT_SECONDS = 20
RECOVERY_POLL_SECONDS = 0.25
RECOVERY_ATTEMPTS = 2


class RecoveredPage(NamedTuple):
    text: str
    url: str


def supports_browser_recovery(url: str) -> bool:
    try:
        parsed = urlsplit(url)

        if (parsed.username or parsed.password
                or parsed.port not in {None, 443}
                or parsed.scheme != "https"):
            return False
    except ValueError:
        return False

    return (
        parsed.hostname in {"www.theregister.com", "netflixtechblog.com"}
        or (parsed.hostname == "www.etsy.com"
            and parsed.path.startswith("/codeascraft/"))
    )


class PageRecovery:
    def __init__(self, session: BrowserSession) -> None:
        self.session = session
        self.lock = asyncio.Lock()
        self.unavailable_hosts: set[str] = set()

    async def fetch(self, url: str) -> RecoveredPage | None:
        if not supports_browser_recovery(url):
            return None

        host = urlsplit(url).netloc

        async with self.lock:
            if host in self.unavailable_hosts:
                return None

            for attempt in range(1, RECOVERY_ATTEMPTS + 1):
                count("retries")
                logger.info(
                    "Trying independent browser recovery for %s (attempt %d)",
                    url, attempt,
                )

                try:
                    async with asyncio.timeout(RECOVERY_TIMEOUT_SECONDS):
                        result = await self._fetch_document(url)
                except Exception as error:
                    logger.warning(
                        "Browser recovery attempt %d failed for %s (%s)",
                        attempt, url, type(error).__name__,
                    )
                    continue

                logger.info(
                    "Recovered public page through browser: %s", result.url,
                )

                return result

            self.unavailable_hosts.add(host)
            logger.warning(
                "Browser recovery unavailable for %s; later denials on "
                "this host will retain feed content", host,
            )

            return None

    async def _fetch_document(self, url: str) -> RecoveredPage:
        browser = await self.session.get_browser()
        page = await browser.get("about:blank", new_tab=True)
        target = urlsplit(url)
        capture = NetworkCapture(
            page, [rf"^https://{re.escape(target.netloc)}/"],
            resource_types=(cdp.network.ResourceType.DOCUMENT,),
        )

        try:
            await capture.start()
            await page.get(url)

            while True:
                for response in await capture.drain(
                    timeout_seconds=RECOVERY_POLL_SECONDS,
                ):
                    actual = urlsplit(response.url)

                    if (
                        response.status == 200 and response.body.strip()
                        and actual.hostname == target.hostname
                        and actual.path.rstrip("/") == target.path.rstrip("/")
                        and not is_page_challenge(response.body)
                        and response.mime_type in {
                            "text/html", "application/xhtml+xml",
                            "application/rss+xml", "application/atom+xml",
                            "text/xml", "application/xml",
                        }
                    ):
                        return RecoveredPage(response.body, response.url)

                await asyncio.sleep(RECOVERY_POLL_SECONDS)
        finally:
            await capture.stop()

            try:
                async with asyncio.timeout(3):
                    await page.close()
            except Exception:
                logger.debug(
                    "Recovered-page tab cleanup failed", exc_info=True,
                )


current_page_recovery: ContextVar[PageRecovery | None] = ContextVar(
    "current_page_recovery", default=None,
)
