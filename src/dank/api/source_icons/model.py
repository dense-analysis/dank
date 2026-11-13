"""Which domains may have a source icon, and where it is served.

The route and the capture share these rules; the capture checks every URL
it fetches against them.
"""

import ipaddress
import re
from urllib.parse import urlencode

SOCIAL_DOMAINS = {"x.com", "twitter.com", "reddit.com", "youtube.com"}
DOMAIN_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")


def domain_valid(domain: str) -> bool:
    if len(domain) > 253 or "." not in domain:
        return False

    try:
        ipaddress.ip_address(domain)
    except ValueError:
        return all(
            DOMAIN_LABEL.fullmatch(label) for label in domain.split(".")
        )

    return False


def source_icon_url(domain: str) -> str | None:
    if (
        not domain_valid(domain)
        or domain.removeprefix("www.") in SOCIAL_DOMAINS
    ):
        return None

    return "/api/reader/source-icon?" + urlencode({"domain": domain})


def icon_domain(raw: str) -> str | None:
    """The domain a request asks for, if it may have an icon at all."""
    domain = raw.strip().lower()

    return domain if source_icon_url(domain) is not None else None
