"""crawl queue performance indexes

Revision ID: 0002_queue_perf_indexes
Revises: 0001_initial_schema
Create Date: 2026-02-21 00:10:00
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "0002_queue_perf_indexes"
down_revision = "0001_initial_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_crawl_queue_ready_order
        ON crawl_queue(status, last_attempt, attempt_count)
        WHERE status = 'queued';
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_crawl_queue_ready_order")
