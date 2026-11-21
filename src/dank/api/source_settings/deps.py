"""Provides the repository for saved source settings."""

from typing import Annotated

from fastapi import Depends

from dank.api.deps import ResourcesDep
from dank.db.sources import PostgresSourceRepository
from dank.sources.service import SourceRepository


def source_repository(resources: ResourcesDep) -> SourceRepository:
    # It opens a transaction per call, so it takes the session factory
    # rather than a session per request.
    return PostgresSourceRepository(resources.postgres)


SourceRepositoryDep = Annotated[SourceRepository, Depends(source_repository)]
