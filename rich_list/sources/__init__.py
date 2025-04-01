import concurrent.futures

from typing import Any, NamedTuple, Protocol

from .base import available_sources, SourceSettings, Source, Post
import rich_list.sources.x  # noqa


class SourceTaskDefinition(NamedTuple):
    source_persistent_data: dict[str, Any]
    settings: SourceSettings
    source: Source


class ProcessedPost(NamedTuple):
    source_name: str
    post: Post


class PostProcessor(Protocol):
    """
    A PosterProcessor is an async callable that processes posts.
    """
    async def __call__(self, source: str, post: Post) -> None:
        ...


async def load_posts_group(
    source_task_definitions: list[SourceTaskDefinition],
    post_processor: PostProcessor,
) -> None:
    """
    Load posts group takes a series of tasks and processes them in an async
    loop, sending post data to a processor.
    """
    for task_def in source_task_definitions:
        source_name = task_def.source.name

        async for post in task_def.source.load_posts(
            task_def.settings,
            task_def.source_persistent_data
        ):
            await post_processor(source_name, post)


def load_posts_from_all_sources(
    *,
    source_settings: dict[str, SourceSettings],
    persistent_data: dict[str, dict[str, Any]],
    post_processor: PostProcessor,
    max_workers: int = 4,
):
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max_workers,
    ) as executor:
        source_task_definitions: list[SourceTaskDefinition] = []

        # Load sources for all settings,
        # and create tasks defintions for all enabled sources.
        for source_name, settings in source_settings.items():
            source = available_sources.get(source_name)

            if source is not None and settings['enabled']:
                source_persistent_data: dict[str, Any] = (
                    persistent_data
                    .setdefault(source_name, {})
                )

                source_task_definitions.append(SourceTaskDefinition(
                    source_persistent_data=source_persistent_data,
                    settings=settings,
                    source=source,
                ))

        groups = [source_task_definitions[i::max_workers] for i in range(max_workers)]
        futures = [
            executor.submit(load_posts_group, group, post_processor)
            for group in groups
            if len(group) > 0
        ]

        concurrent.futures.wait(futures)


__all__ = [
    'available_sources',
    'load_posts_from_all_sources',
]
