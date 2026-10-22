"""Shared resource identities for reader collection scheduling and locking."""

from __future__ import annotations

from dank.config import Settings


def collection_resources(
    settings: Settings, target_post: tuple[str, str] | None = None,
) -> frozenset[str]:
    domains = {
        source.domain for source in settings.sources
        if target_post is None or source.domain == target_post[0]
    }
    resources = {f"domain:{domain}" for domain in domains}
    profiles = {
        "x.com": settings.data_dir / "browser-profile",
        "reddit.com": settings.data_dir / "reddit-browser-profile",
    }

    if settings.youtube.authenticated and target_post is None:
        profiles["youtube.com"] = (
            settings.youtube.profile_dir
            or settings.data_dir / "youtube-browser-profile"
        )

    resources.update(
        f"profile:{profile.expanduser().resolve()}"
        for domain, profile in profiles.items() if domain in domains
    )

    return frozenset(resources)
