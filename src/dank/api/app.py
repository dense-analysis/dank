"""The FastAPI application: the one place routers and policies are mounted."""

from fastapi import Depends, FastAPI

from dank.api.connections.router import router as connections_router
from dank.api.errors import EXCEPTION_HANDLERS
from dank.api.files.router import router as files_router
from dank.api.files.static import DEFAULT_ROOTS, FileRoots, mount_files
from dank.api.guards import same_origin_json
from dank.api.headers import ApiHeaders
from dank.api.lifespan import lifespan
from dank.api.posts.router import router as posts_router
from dank.api.refresh.router import router as refresh_router
from dank.api.source_icons.router import router as source_icons_router
from dank.api.source_settings.router import router as source_settings_router
from dank.api.sources.router import router as sources_router
from dank.api.youtube.router import router as youtube_router
from dank.config import Settings


def create_app(
    settings: Settings, *, roots: FileRoots = DEFAULT_ROOTS,
) -> FastAPI:
    app = FastAPI(
        title="DANK",
        lifespan=lifespan(settings),
        # Runs before every route; it only acts on unsafe methods.
        dependencies=[Depends(same_origin_json)],
        exception_handlers=EXCEPTION_HANDLERS,
        openapi_url="/api/openapi.json",
        docs_url="/api/docs",
        # No OAuth flows, so Swagger UI needs no redirect page.
        swagger_ui_oauth2_redirect_url=None,
        redoc_url=None,
        # FastAPI's redirect would name this server's scheme, which behind
        # the HTTPS proxy is plain HTTP.
        redirect_slashes=False,
    )
    app.add_middleware(ApiHeaders)
    app.include_router(sources_router)
    app.include_router(source_settings_router)
    app.include_router(posts_router)
    app.include_router(connections_router)
    app.include_router(refresh_router)
    app.include_router(youtube_router)
    app.include_router(source_icons_router)
    app.include_router(files_router)
    mount_files(app, settings, roots)

    return app
