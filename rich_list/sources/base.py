from typing import AsyncIterator, NamedTuple, TypedDict
from typing import TypeVar
from typing import Type
from typing import Any
from typing import Callable


class Post(NamedTuple):
    title: str


class SourceSettings(TypedDict):
    enabled: bool
    """Enable/disable a source"""


class Source(NamedTuple):
    name: str
    settings_type: Type[SourceSettings]
    load_posts: Callable[[Any, dict[str, Any]], AsyncIterator[Post]]


SettingsType = TypeVar('SettingsType', bound=SourceSettings, default=SourceSettings)

available_sources: dict[str, Source] = {}


def register_source(
    source_name: str,
    settings_type: Type[SettingsType],
    load_func: Callable[[SettingsType, dict[str, Any]], AsyncIterator[Post]],
) -> None:
    """
    Register a source for fetching posts so it can be used to retrieve posts.
    """
    available_sources[source_name] = Source(
        source_name,
        settings_type,
        load_func,
    )
