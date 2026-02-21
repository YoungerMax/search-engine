"""switch spellcheck to trigram lookup

Revision ID: 0005_spellcheck_trgm_lookup
Revises: 0004_spellcheck_v2
Create Date: 2026-02-21 00:35:00
"""

from alembic import op

revision = "0005_spellcheck_trgm_lookup"
down_revision = "0004_spellcheck_v2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE EXTENSION IF NOT EXISTS pg_trgm;

        CREATE INDEX IF NOT EXISTS idx_spellcheck_word_trgm
        ON spellcheck_dictionary
        USING gin (word gin_trgm_ops);

        DROP TABLE IF EXISTS spellcheck_deletes;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS spellcheck_deletes (
            delete_key TEXT NOT NULL,
            word TEXT NOT NULL REFERENCES spellcheck_dictionary(word) ON DELETE CASCADE,
            PRIMARY KEY (delete_key, word)
        );

        CREATE INDEX IF NOT EXISTS idx_spellcheck_deletes_key
        ON spellcheck_deletes(delete_key);

        DROP INDEX IF EXISTS idx_spellcheck_word_trgm;
        """
    )
