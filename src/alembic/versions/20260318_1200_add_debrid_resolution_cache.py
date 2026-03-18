"""add debrid resolution cache

Revision ID: 20260318_1200
Revises: b1345f835923
Create Date: 2026-03-18 12:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "20260318_1200"
down_revision: Union[str, None] = "b1345f835923"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


debrid_cache_status = sa.Enum(
    "CACHED",
    "NOT_FOUND",
    "ERROR",
    name="debridcachestatus",
)


def upgrade() -> None:
    debrid_cache_status.create(op.get_bind(), checkfirst=True)
    op.create_table(
        "DebridResolutionCache",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("infohash", sa.String(length=64), nullable=False),
        sa.Column("provider", sa.String(length=32), nullable=False),
        sa.Column("status", debrid_cache_status, nullable=False),
        sa.Column("resolved_at", sa.DateTime(), nullable=True),
        sa.Column("last_checked", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("infohash", "provider", name="uq_debrid_cache_hash_provider"),
    )
    op.create_index(
        op.f("ix_DebridResolutionCache_infohash"),
        "DebridResolutionCache",
        ["infohash"],
        unique=False,
    )
    op.create_index(
        op.f("ix_DebridResolutionCache_provider"),
        "DebridResolutionCache",
        ["provider"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_DebridResolutionCache_provider"), table_name="DebridResolutionCache")
    op.drop_index(op.f("ix_DebridResolutionCache_infohash"), table_name="DebridResolutionCache")
    op.drop_table("DebridResolutionCache")
    debrid_cache_status.drop(op.get_bind(), checkfirst=True)
