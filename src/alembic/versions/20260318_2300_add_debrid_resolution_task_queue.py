"""add debrid resolution task queue

Revision ID: 20260318_2300
Revises: 20260318_1200
Create Date: 2026-03-18 23:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "20260318_2300"
down_revision: Union[str, None] = "20260318_1200"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "DebridResolutionTask",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("item_id", sa.Integer(), nullable=False),
        sa.Column("infohash", sa.String(length=64), nullable=False),
        sa.Column("provider", sa.String(length=32), nullable=True),
        sa.Column("stream_title", sa.String(), nullable=True),
        sa.Column(
            "trigger",
            sa.Enum(
                "pipeline",
                "play",
                "retry",
                "manual",
                "scheduler",
                name="debridtasktrigger",
            ),
            nullable=False,
        ),
        sa.Column(
            "priority",
            sa.Enum("high", "normal", "low", name="debridtaskpriority"),
            nullable=False,
        ),
        sa.Column(
            "status",
            sa.Enum(
                "pending",
                "processing",
                "completed",
                "failed",
                "cancelled",
                name="debridtaskstatus",
            ),
            nullable=False,
        ),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("max_attempts", sa.Integer(), nullable=False),
        sa.Column("available_at", sa.DateTime(), nullable=False),
        sa.Column("locked_at", sa.DateTime(), nullable=True),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.Column("last_error", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_debridtask_status_available_at",
        "DebridResolutionTask",
        ["status", "available_at"],
        unique=False,
    )
    op.create_index(
        "ix_debridtask_item_status",
        "DebridResolutionTask",
        ["item_id", "status"],
        unique=False,
    )
    op.create_index(
        op.f("ix_DebridResolutionTask_item_id"),
        "DebridResolutionTask",
        ["item_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_DebridResolutionTask_infohash"),
        "DebridResolutionTask",
        ["infohash"],
        unique=False,
    )
    op.create_index(
        op.f("ix_DebridResolutionTask_provider"),
        "DebridResolutionTask",
        ["provider"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_DebridResolutionTask_provider"), table_name="DebridResolutionTask")
    op.drop_index(op.f("ix_DebridResolutionTask_infohash"), table_name="DebridResolutionTask")
    op.drop_index(op.f("ix_DebridResolutionTask_item_id"), table_name="DebridResolutionTask")
    op.drop_index("ix_debridtask_item_status", table_name="DebridResolutionTask")
    op.drop_index("ix_debridtask_status_available_at", table_name="DebridResolutionTask")
    op.drop_table("DebridResolutionTask")
