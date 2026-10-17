"""YouTube evidence adapter over shared source-owned link extraction."""

from typing import Any

from dank.references import reference_rows as link_reference_rows


def reference_rows(row: dict[str, Any], *, raw: bool) -> list[dict[str, Any]]:
    results = link_reference_rows(row, raw=raw, video=True)

    for evidence in results:
        evidence["video_id"] = evidence.pop("target_url")
        evidence["format_hint"] = (
            "short" if "/shorts/" in evidence["matched_url"] else "video"
        )

    return results
