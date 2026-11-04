"""Narrow compatibility fixes for Zendriver's generated CDP bindings."""

from typing import Any

from zendriver import cdp

_parse_request_extra_info = cdp.network.RequestWillBeSentExtraInfo.from_json


def _request_extra_info(
    json: dict[str, Any],
) -> cdp.network.RequestWillBeSentExtraInfo:
    state = json.get("clientSecurityState")

    if (
        isinstance(state, dict)
        and "privateNetworkRequestPolicy" in state
        and "localNetworkAccessRequestPolicy" not in state
    ):
        # Chromium 138 reports the older Private Network Access policy.
        # It has no exact equivalent in the newer permission-based schema:
        # https://chromedevtools.github.io/devtools-protocol/tot/Network/#type-ClientSecurityState
        # Omit this optional, unused metadata rather than invent a policy.
        json = {**json, "clientSecurityState": None}

    return _parse_request_extra_info(json)


def install_cdp_compatibility() -> None:
    """Keep legacy request events parseable without changing event types."""
    cdp.network.RequestWillBeSentExtraInfo.from_json = staticmethod(
        _request_extra_info,
    )
