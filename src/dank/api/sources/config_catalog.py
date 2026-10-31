"""SourceCatalog read from config.toml, until DEN-22 moves it to Postgres."""

from dank.config import Settings, SourceConfig


class ConfigSourceCatalog:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    async def sources(self) -> tuple[SourceConfig, ...]:
        return self._settings.sources
