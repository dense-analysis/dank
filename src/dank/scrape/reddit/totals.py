"""Optional community-size enrichment from Arctic Shift's archive."""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
from collections.abc import AsyncIterator
from typing import cast
from urllib.parse import urlencode

import aiohttp

from dank.model import RedditCommunityTotal
from dank.scrape.http import HostCooldownError, http_response
from dank.scrape.metrics import count

logger = logging.getLogger(__name__)
ARCTIC_SHIFT_URL = "https://arctic-shift.photon-reddit.com/api/subreddits/search"
# Optional metadata should not hold up listing collection during an outage.
LOOKUP_TIMEOUT_SECONDS = 10


def _parse_total(
    payload: str, community: str, request_url: str,
) -> RedditCommunityTotal:
    body: object = json.loads(payload)
    entries = (
        cast(dict[str, object], body).get("data")
        if isinstance(body, dict) else None
    )

    if not isinstance(entries, list) or len(cast(list[object], entries)) != 1:
        raise ValueError("Expected one matching archived community")

    entry = cast(list[object], entries)[0]
    fields = cast(dict[str, object], entry) if isinstance(entry, dict) else {}
    name = fields.get("display_name")
    metadata = fields.get("_meta")

    if (
        not isinstance(name, str) or name.casefold() != community.casefold()
        or not isinstance(metadata, dict)
    ):
        raise ValueError("Archive response does not match the community")

    metadata = cast(dict[str, object], metadata)
    total = metadata.get("num_posts")
    updated = metadata.get("num_posts_updated_at")
    captured = dt.datetime.now(dt.UTC)

    if type(total) is not int or not 0 <= total <= 2**53 - 1:
        raise ValueError("Missing or invalid archived post count")

    if type(updated) is not int or not 0 < updated <= captured.timestamp():
        raise ValueError("Missing or invalid archive count update date")

    return RedditCommunityTotal(
        community=community.lower(), total_posts=total,
        updated_at=dt.datetime.fromtimestamp(updated, dt.UTC),
        scraped_at=captured, request_url=request_url, payload=payload,
    )


async def scrape_community_totals(
    client: aiohttp.ClientSession, communities: tuple[str, ...],
) -> AsyncIterator[RedditCommunityTotal]:
    # _meta counts the archive's records, not Reddit's current public posts.
    # https://github.com/ArthurHeitmann/arctic_shift/tree/master/api#subreddits
    for community in dict.fromkeys(name.lower() for name in communities):
        url = ARCTIC_SHIFT_URL + "?" + urlencode({
            "subreddit": community, "limit": 1,
        })

        try:
            async with asyncio.timeout(LOOKUP_TIMEOUT_SECONDS):
                async with http_response(client, url) as response:
                    response.raise_for_status()
                    payload = await response.text()

            total = _parse_total(payload, community, url)
        except (aiohttp.ClientError, TimeoutError, HostCooldownError) as error:
            count("fetch_failures")
            logger.warning(
                "Arctic Shift r/%s total unavailable; keeping any saved "
                "estimate: %s", community, error,
            )
            continue
        except ValueError as error:
            count("parse_failures")
            logger.warning(
                "Arctic Shift r/%s total invalid; keeping any saved "
                "estimate: %s", community, error,
            )
            continue

        logger.info(
            "Arctic Shift r/%s: %d archived posts; source updated %s",
            community, total.total_posts, total.updated_at.isoformat(),
        )
        yield total
