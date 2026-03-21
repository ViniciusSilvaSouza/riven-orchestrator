"""add acquiring started at to debrid resolution task

Revision ID: 20260321_1000
Revises: 20260320_1900
Create Date: 2026-03-21 10:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "20260321_1000"
down_revision: Union[str, None] = "20260320_1900"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "DebridResolutionTask",
        sa.Column("acquiring_started_at", sa.DateTime(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("DebridResolutionTask", "acquiring_started_at")
