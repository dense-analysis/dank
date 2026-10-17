"""X account identities from canonical post URLs and captured profiles."""

from __future__ import annotations

import json
import re
from typing import Any, TypedDict, cast
from urllib.parse import urlsplit

from dank.storage.clickhouse import ClickHouseClient

X_DOMAINS = ("x.com", "twitter.com")
# Keep counts and account filtering tied to the same post owner, independent
# of the editable author search or the timeline a post was discovered on.
X_ACCOUNT_PATTERN = (
    "^https?://(?:www[.])?(?:x[.]com|twitter[.]com)/"
    "([a-z0-9_]{1,15})/status/"
)
X_ACCOUNT_SQL = f"extract(lowerUTF8(url), '{X_ACCOUNT_PATTERN}')"


class AccountIdentity(TypedDict):
    handle: str
    name: str
    icon_url: str | None
    total_posts: int | None


class Account(AccountIdentity):
    count: int


def source_kind(domain: str) -> str:
    if domain in X_DOMAINS:
        return "x"

    if domain == "youtube.com":
        return "youtube"

    if domain == "news.ycombinator.com":
        return "hacker_news"

    return "reddit" if domain == "reddit.com" else "rss"


async def source_accounts(
    client: ClickHouseClient, domain: str, configured: tuple[str, ...],
) -> list[Account]:
    result = await client.fetch_json(
        f"SELECT {X_ACCOUNT_SQL} AS handle, count() AS count "
        "FROM posts FINAL WHERE domain = %(domain)s "
        "AND handle != '' GROUP BY handle",
        {"domain": domain},
    )
    counts = {str(row["handle"]): int(row["count"]) for row in result.rows}
    handles = {
        account.strip().removeprefix("@").lower() for account in configured
    } if configured else set(counts)
    handles = {
        handle for handle in handles
        if re.fullmatch(r"[a-z0-9_]{1,15}", handle)
    }

    if not handles:
        return []

    profiles = await _account_profiles(
        client, {(domain, handle) for handle in handles},
    )

    return [
        {**profiles[domain, handle], "count": counts.get(handle, 0)}
        for handle in sorted(handles)
    ]


async def _account_profiles(
    client: ClickHouseClient, accounts: set[tuple[str, str]],
) -> dict[tuple[str, str], AccountIdentity]:
    if not accounts:
        return {}

    result = await client.fetch_json(
        f"SELECT domain, {X_ACCOUNT_SQL} AS handle, "
        "argMax(JSONExtractRaw(payload, 'core', 'user_results', 'result'), "
        "scraped_at) AS profile FROM raw_posts "
        "WHERE (domain, handle) IN %(accounts)s GROUP BY domain, handle",
        {"accounts": sorted(accounts)},
    )
    profiles = {
        (str(row["domain"]), str(row["handle"])):
        _account_profile(row.get("profile"), str(row["handle"]))
        for row in result.rows
    }

    return {
        (domain, handle): profiles.get(
            (domain, handle), _account_profile(None, handle),
        )
        for domain, handle in accounts
    }


def profile_identity(value: object, handle: str) -> tuple[str, str | None]:
    profile = _account_profile(value, handle)

    return profile["name"], profile["icon_url"]


def _account_profile(value: object, handle: str) -> AccountIdentity:
    fallback: AccountIdentity = {
        "handle": handle, "name": handle, "icon_url": None,
        "total_posts": None,
    }

    try:
        profile = _object(
            json.loads(value) if isinstance(value, str) else value,
        )
    except ValueError:
        return fallback

    core = _object(profile.get("core"))
    legacy = _object(profile.get("legacy"))
    identity = core if core.get("screen_name") else legacy
    captured_handle = identity.get("screen_name")

    if (
        not isinstance(captured_handle, str)
        or captured_handle.lower() != handle
    ):
        return fallback

    name = identity.get("name")
    avatar = _object(profile.get("avatar"))
    icon = avatar.get("image_url") or legacy.get("profile_image_url_https")
    total = _object(profile.get("tweet_counts")).get("tweets")

    if total is None:
        total = legacy.get("statuses_count")

    return {
        "handle": handle,
        "name": (
            name.strip() if isinstance(name, str) and name.strip() else handle
        ),
        "icon_url": _profile_icon(icon),
        "total_posts": total if (
            type(total) is int and 0 <= total <= 2**53 - 1
        ) else None,
    }


def _object(value: object) -> dict[str, Any]:
    return cast(dict[str, Any], value) if isinstance(value, dict) else {}


def _profile_icon(value: object) -> str | None:
    if not isinstance(value, str) or any(
        character.isspace() or ord(character) < 32 for character in value
    ):
        return None

    try:
        url = urlsplit(value)

        if (
            url.scheme == "https" and url.hostname == "pbs.twimg.com"
            and url.username is None and url.password is None
            and url.port in {None, 443}
            and url.path.startswith((
                "/profile_images/", "/sticky/default_profile_images/",
            ))
        ):
            return value
    except ValueError:
        pass

    return None
