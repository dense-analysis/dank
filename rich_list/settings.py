import tomllib
from typing import Any
from typing import cast
from typing import NotRequired, TypedDict


from typing import Type
from .sources import available_sources, SourceSettings


class Settings(TypedDict):
    sources: dict[str, SourceSettings]


def process_source_settings(
    settings_type: Type[SourceSettings],
) -> None:
    pass


def load_settings(filename: str) -> Settings:
    with open(filename, 'rb') as file:
        data = tomllib.load(file)

    # TODO: Use type introspection to process settings
    data.get('sources', {})

    for source in available_sources.values():
        source.name

        source.settings_type

    return Settings(
        x=XSettings(
            (data.get('x') or {}).get('username')
            username="",
        )
    )
