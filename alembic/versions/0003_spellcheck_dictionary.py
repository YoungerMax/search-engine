"""spellcheck dictionary table

Revision ID: 0003_spellcheck_dictionary
Revises: 0002_queue_perf_indexes
Create Date: 2026-02-21 00:00:00
"""

from alembic import op

revision = "0003_spellcheck_dictionary"
down_revision = "0002_queue_perf_indexes"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS spellcheck_dictionary (
            word TEXT PRIMARY KEY,
            doc_frequency BIGINT NOT NULL,
            total_frequency BIGINT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_spellcheck_doc_freq ON spellcheck_dictionary(doc_frequency DESC);
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS spellcheck_dictionary")