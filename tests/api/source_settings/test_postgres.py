"""Source settings round trips through the route and repository port."""

from httpx import AsyncClient

from tests.api.client import BASE_URL

PATH = "/api/reader/source-settings"
SAME_ORIGIN = {"origin": BASE_URL}
BODY = {
    "enabled": True,
    "domain": "x.com",
    "accounts": [{"value": "someone", "enabled": True}],
    "feeds": [],
    "tags": [],
    "adapters": None,
    "reddit": None,
    "hacker_news": None,
}


async def test_sources_are_saved_listed_and_edited(
    client: AsyncClient,
) -> None:
    added = await client.post(PATH, json=BODY, headers=SAME_ORIGIN)
    again = await client.post(PATH, json=BODY, headers=SAME_ORIGIN)
    source_id = added.json()["id"]
    edited = await client.put(
        f"{PATH}/{source_id}", json={**BODY, "enabled": False},
        headers=SAME_ORIGIN,
    )

    assert added.status_code == 201
    assert (again.status_code, again.json()["code"]) == (
        409, "duplicate_source",
    )
    assert edited.json() == {**added.json(), "enabled": False}
    assert (await client.get(PATH)).json()["sources"] == [edited.json()]
    assert (await client.get(f"{PATH}/{source_id}")).json() == edited.json()
