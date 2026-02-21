"""spellcheck v2 dictionary and delete index

Revision ID: 0004_spellcheck_v2
Revises: 0003_spellcheck_dictionary
Create Date: 2026-02-21 00:20:00
"""

from alembic import op

revision = "0004_spellcheck_v2"
down_revision = "0003_spellcheck_dictionary"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE spellcheck_dictionary
            ADD COLUMN IF NOT EXISTS external_frequency BIGINT NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS popularity_score DOUBLE PRECISION NOT NULL DEFAULT 0;

        CREATE INDEX IF NOT EXISTS idx_spellcheck_popularity
        ON spellcheck_dictionary(popularity_score DESC);

        CREATE TABLE IF NOT EXISTS spellcheck_deletes (
            delete_key TEXT NOT NULL,
            word TEXT NOT NULL REFERENCES spellcheck_dictionary(word) ON DELETE CASCADE,
            PRIMARY KEY (delete_key, word)
        );

        CREATE INDEX IF NOT EXISTS idx_spellcheck_deletes_key
        ON spellcheck_deletes(delete_key);
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP TABLE IF EXISTS spellcheck_deletes;
        DROP INDEX IF EXISTS idx_spellcheck_popularity;

        ALTER TABLE spellcheck_dictionary
            DROP COLUMN IF EXISTS popularity_score,
            DROP COLUMN IF EXISTS external_frequency;
        """
    )
