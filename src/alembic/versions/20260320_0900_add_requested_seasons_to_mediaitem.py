"""add requested seasons to mediaitem

Revision ID: 20260320_0900
Revises: 20260318_2300
Create Date: 2026-03-20 09:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "20260320_0900"
down_revision: Union[str, None] = "20260318_2300"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("MediaItem", sa.Column("requested_seasons", sa.JSON(), nullable=True))


def downgrade() -> None:
    op.drop_column("MediaItem", "requested_seasons")
