from datetime import datetime
from enum import Enum
from typing import cast

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


class DebridTaskPriority(str, Enum):
    HIGH = "high"
    NORMAL = "normal"
    LOW = "low"


class DebridTaskStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class DebridTaskTrigger(str, Enum):
    PIPELINE = "pipeline"
    PLAY = "play"
    RETRY = "retry"
    MANUAL = "manual"
    SCHEDULER = "scheduler"


class DebridResolutionCache(Base):
    __tablename__ = "DebridResolutionCache"

    id: Mapped[int] = mapped_column(sqlalchemy.Integer, primary_key=True)
    infohash: Mapped[str] = mapped_column(sqlalchemy.String(64), index=True)
    provider: Mapped[str] = mapped_column(sqlalchemy.String(32), index=True)
    status: Mapped[DebridCacheStatus] = mapped_column(
        sqlalchemy.Enum(
            DebridCacheStatus,
            name="debridcachestatus",
            values_callable=lambda enum: [
                e.value for e in cast(list[DebridCacheStatus], enum)
            ],
        ),
        nullable=False,
    )
    resolved_at: Mapped[datetime | None] = mapped_column(
        sqlalchemy.DateTime, nullable=True
    )
    last_checked: Mapped[datetime] = mapped_column(sqlalchemy.DateTime, nullable=False)

    __table_args__ = (
        sqlalchemy.UniqueConstraint("infohash", "provider", name="uq_debrid_cache_hash_provider"),
    )


class DebridResolutionTask(Base):
    __tablename__ = "DebridResolutionTask"

    id: Mapped[int] = mapped_column(sqlalchemy.Integer, primary_key=True)
    item_id: Mapped[int] = mapped_column(sqlalchemy.Integer, index=True)
    infohash: Mapped[str] = mapped_column(sqlalchemy.String(64), index=True)
    provider: Mapped[str | None] = mapped_column(sqlalchemy.String(32), index=True)
    stream_title: Mapped[str | None] = mapped_column(sqlalchemy.String(), nullable=True)
    trigger: Mapped[DebridTaskTrigger] = mapped_column(
        sqlalchemy.Enum(
            DebridTaskTrigger,
            name="debridtasktrigger",
            values_callable=lambda enum: [
                e.value for e in cast(list[DebridTaskTrigger], enum)
            ],
        ),
        nullable=False,
    )
    priority: Mapped[DebridTaskPriority] = mapped_column(
        sqlalchemy.Enum(
            DebridTaskPriority,
            name="debridtaskpriority",
            values_callable=lambda enum: [
                e.value for e in cast(list[DebridTaskPriority], enum)
            ],
        ),
        nullable=False,
    )
    status: Mapped[DebridTaskStatus] = mapped_column(
        sqlalchemy.Enum(
            DebridTaskStatus,
            name="debridtaskstatus",
            values_callable=lambda enum: [
                e.value for e in cast(list[DebridTaskStatus], enum)
            ],
        ),
        nullable=False,
    )
    attempts: Mapped[int] = mapped_column(sqlalchemy.Integer, default=0)
    max_attempts: Mapped[int] = mapped_column(sqlalchemy.Integer, default=3)
    available_at: Mapped[datetime] = mapped_column(sqlalchemy.DateTime, nullable=False)
    locked_at: Mapped[datetime | None] = mapped_column(sqlalchemy.DateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(sqlalchemy.DateTime, nullable=True)
    last_error: Mapped[str | None] = mapped_column(sqlalchemy.String(), nullable=True)
    created_at: Mapped[datetime] = mapped_column(sqlalchemy.DateTime, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(sqlalchemy.DateTime, nullable=False)

    __table_args__ = (
        sqlalchemy.Index("ix_debridtask_status_available_at", "status", "available_at"),
        sqlalchemy.Index("ix_debridtask_item_status", "item_id", "status"),
    )
