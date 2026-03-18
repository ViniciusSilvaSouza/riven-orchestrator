from datetime import datetime
from enum import Enum

import sqlalchemy
from sqlalchemy.orm import Mapped, mapped_column

from program.db.base_model import Base


class DebridCacheStatus(str, Enum):
    CACHED = "cached"
    NOT_FOUND = "not_found"
    ERROR = "error"


class ProviderHealthState(str, Enum):
    HEALTHY = "healthy"
    RATE_LIMITED = "rate_limited"
    DOWN = "down"


class DebridResolutionCache(Base):
    __tablename__ = "DebridResolutionCache"

    id: Mapped[int] = mapped_column(sqlalchemy.Integer, primary_key=True)
    infohash: Mapped[str] = mapped_column(sqlalchemy.String(64), index=True)
    provider: Mapped[str] = mapped_column(sqlalchemy.String(32), index=True)
    status: Mapped[DebridCacheStatus] = mapped_column(
        sqlalchemy.Enum(DebridCacheStatus), nullable=False
    )
    resolved_at: Mapped[datetime | None] = mapped_column(
        sqlalchemy.DateTime, nullable=True
    )
    last_checked: Mapped[datetime] = mapped_column(sqlalchemy.DateTime, nullable=False)

    __table_args__ = (
        sqlalchemy.UniqueConstraint("infohash", "provider", name="uq_debrid_cache_hash_provider"),
    )
