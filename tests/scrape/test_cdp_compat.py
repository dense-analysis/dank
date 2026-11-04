from copy import deepcopy
from typing import Any

import pytest
from zendriver import cdp
from zendriver.cdp.util import parse_json_event

from dank.scrape.cdp_compat import install_cdp_compatibility


@pytest.fixture(autouse=True)
def compatibility(monkeypatch: pytest.MonkeyPatch) -> None:
    # Restore the class descriptor after each test, including failed tests.
    event_type = cdp.network.RequestWillBeSentExtraInfo
    monkeypatch.setattr(
        event_type, "from_json", event_type.__dict__["from_json"],
    )
    install_cdp_compatibility()


def _event(state: dict[str, Any] | None) -> dict[str, Any]:
    return {
        "method": "Network.requestWillBeSentExtraInfo",
        "params": {
            "requestId": "123.4",
            "associatedCookies": [{
                "cookie": {
                    "name": "fixture", "value": "example",
                    "domain": "example.test", "path": "/", "expires": -1,
                    "size": 14, "httpOnly": True, "secure": True,
                    "session": True, "priority": "Medium",
                    "sourceScheme": "Secure", "sourcePort": 443,
                },
                "blockedReasons": [],
            }],
            "headers": {"accept": "application/json"},
            "connectTiming": {"requestTime": 123.0},
            "clientSecurityState": state,
            "siteHasCookieInOtherPartition": False,
        },
    }


@pytest.mark.parametrize("same_party", [None, False])
@pytest.mark.parametrize("policy", ["PreflightWarn", "PreflightBlock"])
def test_legacy_events_preserve_request_data(
    policy: str, *, same_party: bool | None,
) -> None:
    message = _event({
        "initiatorIsSecureContext": True,
        "initiatorIPAddressSpace": "Public",
        "privateNetworkRequestPolicy": policy,
    })

    if same_party is not None:
        message["params"]["associatedCookies"][0]["cookie"][
            "sameParty"
        ] = same_party

    original = deepcopy(message)
    event = parse_json_event(message)

    assert type(event) is cdp.network.RequestWillBeSentExtraInfo
    assert event.client_security_state is None
    assert event.request_id == "123.4"
    assert event.headers == {"accept": "application/json"}
    assert event.connect_timing.request_time == 123.0
    assert event.associated_cookies[0].cookie.name == "fixture"
    assert event.associated_cookies[0].cookie.value == "example"
    assert event.site_has_cookie_in_other_partition is False
    assert message == original


@pytest.mark.parametrize("legacy_policy", [None, "PreflightWarn"])
def test_modern_security_state_is_preserved(
    legacy_policy: str | None,
) -> None:
    state = {
        "initiatorIsSecureContext": True,
        "initiatorIPAddressSpace": "Public",
        "localNetworkAccessRequestPolicy": "PermissionBlock",
    }

    if legacy_policy is not None:
        state["privateNetworkRequestPolicy"] = legacy_policy

    event = parse_json_event(_event(state))

    assert event.client_security_state.to_json() == {
        key: value for key, value in state.items()
        if key != "privateNetworkRequestPolicy"
    }


def test_absent_optional_security_state() -> None:
    message = _event(None)
    del message["params"]["clientSecurityState"]
    event = parse_json_event(message)

    assert event.client_security_state is None


def test_unrelated_schema_errors_are_not_hidden() -> None:
    message = _event(None)
    del message["params"]["requestId"]

    with pytest.raises(KeyError, match="requestId"):
        parse_json_event(message)


def test_repeated_installation_is_safe() -> None:
    install_cdp_compatibility()
    install_cdp_compatibility()
    event = parse_json_event(_event({
        "initiatorIsSecureContext": True,
        "initiatorIPAddressSpace": "Public",
        "privateNetworkRequestPolicy": "PreflightWarn",
    }))

    assert event.request_id == "123.4"
