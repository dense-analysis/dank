"""Sources people manage in the app, one row each. See dank.sources."""

import uuid
from typing import Any

from sqlalchemy import BigInteger, Identity, Index, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from dank.db.models import Base

# The index that refuses two sources with identical settings.
DUPLICATE_INDEX = "uq_sources_config"


class SourceRow(Base):
    __tablename__ = "sources"
    __table_args__ = (
        # A hash, because a btree entry can't hold more than about 2.7 KB,
        # and a source with many accounts is longer. jsonb's text is
        # canonical, so key order can't hide a duplicate. MD5 is only a
        # fingerprint here, not a security measure.
        Index(DUPLICATE_INDEX, text("md5(config::text)"), unique=True),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    # The order sources were added in, which the import takes from
    # config.toml. Postgres assigns it and nothing changes it.
    position: Mapped[int] = mapped_column(BigInteger, Identity(always=True))
    # A column of its own so that it isn't part of the duplicate check.
    enabled: Mapped[bool]
    # Everything else, from dank.sources.model.encode_config.
    config: Mapped[dict[str, Any]] = mapped_column(JSONB)
