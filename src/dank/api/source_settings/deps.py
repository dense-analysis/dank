"""Wires SourceService to its repository. Tests override it here."""

from typing import Annotated

from fastapi import Depends

from dank.api.deps import ResourcesDep
from dank.db.sources import PostgresSourceRepository
from dank.sources.service import SourceRepository, SourceService


def source_repository(resources: ResourcesDep) -> SourceRepository:
    # It opens a transaction per call, so it takes the session factory
    # rather than a session per request.
    return PostgresSourceRepository(resources.postgres)


def source_service(
    repository: Annotated[SourceRepository, Depends(source_repository)],
) -> SourceService:
    return SourceService(repository)


SourceServiceDep = Annotated[SourceService, Depends(source_service)]
