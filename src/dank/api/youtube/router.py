"""HTTP routes for YouTube: parse the request, call the service, shape."""

from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query

from dank.api.deps import single_values
from dank.api.youtube.deps import VideoCaptureServiceDep, YouTubeServiceDep
from dank.api.youtube.model import PLAYLIST_ID
from dank.api.youtube.schemas import (
    ChannelsQuery,
    CollectBody,
    PlaylistQuery,
    PlaylistResponse,
    PlaylistsQuery,
    PlaylistsResponse,
    VideoMetadataOut,
    VideoTranscriptOut,
    YouTubeChannelsResponse,
    YouTubeQuery,
    YouTubeResponse,
    YouTubeSourcesResponse,
)
from dank.youtube import VIDEO_ID

router = APIRouter(prefix="/api/reader/youtube", tags=["youtube"])

VideoId = Annotated[str, Path(pattern=f"^{VIDEO_ID.pattern}$")]
PlaylistId = Annotated[str, Path(pattern=f"^{PLAYLIST_ID.pattern}$")]


@router.get("", dependencies=[Depends(single_values(YouTubeQuery))])
async def list_youtube_items(
    query: Annotated[YouTubeQuery, Query()], service: YouTubeServiceDep,
) -> YouTubeResponse:
    """One page of saved videos, Shorts and posts."""
    page = await service.page(query.filters())

    return YouTubeResponse.model_validate(page)


@router.get("/sources")
async def list_youtube_sources(
    service: YouTubeServiceDep,
) -> YouTubeSourcesResponse:
    """Configured feeds and channels, with what each has collected."""
    overview = await service.overview()

    return YouTubeSourcesResponse.model_validate(overview)


@router.get("/channels", dependencies=[Depends(single_values(ChannelsQuery))])
async def list_youtube_channels(
    query: Annotated[ChannelsQuery, Query()], service: YouTubeServiceDep,
) -> YouTubeChannelsResponse:
    """Channels in the selected items, most items first, with examples."""
    page = await service.channels(query.filters(), query.cursor)

    return YouTubeChannelsResponse.model_validate(page)


@router.get(
    "/playlists", dependencies=[Depends(single_values(PlaylistsQuery))],
)
async def list_youtube_playlists(
    query: Annotated[PlaylistsQuery, Query()], service: YouTubeServiceDep,
) -> PlaylistsResponse:
    """The saved playlist library."""
    page = await service.playlists(query.library())

    return PlaylistsResponse.model_validate(page)


@router.get(
    "/playlists/{playlist_id}",
    dependencies=[Depends(single_values(PlaylistQuery))],
)
async def read_youtube_playlist(
    playlist_id: PlaylistId,
    query: Annotated[PlaylistQuery, Query()],
    service: YouTubeServiceDep,
) -> PlaylistResponse:
    """One saved playlist and a page of its entries, duplicates included."""
    detail = await service.playlist(playlist_id, query.page(playlist_id))

    return PlaylistResponse.model_validate(detail)


@router.post("/metadata/{video_id}")
async def collect_video_metadata(
    video_id: VideoId, _body: CollectBody, service: VideoCaptureServiceDep,
) -> VideoMetadataOut:
    """Fetch a known video's preview, unless a saved one is still valid."""
    capture = await service.collect_metadata(video_id)

    return VideoMetadataOut.model_validate(capture)


@router.get("/transcript/{video_id}")
async def read_video_transcript(
    video_id: VideoId, service: VideoCaptureServiceDep,
) -> VideoTranscriptOut:
    """A known video's saved captions. This never contacts YouTube."""
    transcript = await service.transcript(video_id)

    return VideoTranscriptOut.model_validate(transcript)


@router.post("/transcript/{video_id}")
async def collect_video_transcript(
    video_id: VideoId, _body: CollectBody, service: VideoCaptureServiceDep,
) -> VideoTranscriptOut:
    """Fetch a known video's captions, unless saved ones can be reused."""
    transcript = await service.collect_transcript(video_id)

    return VideoTranscriptOut.model_validate(transcript)
