"""The FastAPI application: the one place routers and policies are mounted."""

from fastapi import Depends, FastAPI

from dank.api.connections.router import router as connections_router
from dank.api.errors import EXCEPTION_HANDLERS
from dank.api.guards import same_origin_json
from dank.api.headers import ApiHeaders
from dank.api.lifespan import lifespan
from dank.api.posts.router import router as posts_router
from dank.api.sources.router import router as sources_router
from dank.config import Settings


def create_app(settings: Settings, *, page_size: int) -> FastAPI:
    app = FastAPI(
        title="DANK",
        lifespan=lifespan(settings, page_size=page_size),
        # Runs before every route; it only acts on unsafe methods.
        dependencies=[Depends(same_origin_json)],
        exception_handlers=EXCEPTION_HANDLERS,
        openapi_url="/api/openapi.json",
        docs_url="/api/docs",
        # No OAuth flows, so Swagger UI needs no redirect page.
        swagger_ui_oauth2_redirect_url=None,
        redoc_url=None,
    )
    app.add_middleware(ApiHeaders)
    app.include_router(sources_router)
    app.include_router(posts_router)
    app.include_router(connections_router)

    return app
