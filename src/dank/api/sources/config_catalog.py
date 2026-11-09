"""SourceCatalog read from config.toml, while `sources_in_database` is off."""

from dank.config import Settings, SourceConfig


class ConfigSourceCatalog:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    async def sources(self) -> tuple[SourceConfig, ...]:
        return self._settings.sources

    async def collectable(self) -> tuple[SourceConfig, ...]:
        # config.toml has no way to disable a source.
        return self._settings.sources
