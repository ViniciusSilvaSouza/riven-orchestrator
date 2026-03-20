"""add provider torrent fields to debrid resolution task

Revision ID: 20260320_1900
Revises: 20260320_0900
Create Date: 2026-03-20 19:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "20260320_1900"
down_revision: Union[str, None] = "20260320_0900"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "DebridResolutionTask",
        sa.Column("provider_torrent_id", sa.String(length=128), nullable=True),
    )
    op.add_column(
        "DebridResolutionTask",
        sa.Column("provider_torrent_status", sa.String(length=64), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("DebridResolutionTask", "provider_torrent_status")
    op.drop_column("DebridResolutionTask", "provider_torrent_id")
