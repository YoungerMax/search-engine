"""add words table for spellcheck source terms

Revision ID: 0008_spellcheck_words_table
Revises: 0007_unify_news_tokens
Create Date: 2026-02-23 00:00:00
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "0008_spellcheck_words_table"
down_revision = "0007_unify_news_tokens"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS words (
          word TEXT PRIMARY KEY,
          total_frequency BIGINT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_words_total_frequency ON words(total_frequency DESC);
        """
    )

    op.execute(
        """
        INSERT INTO words(word, total_frequency)
        SELECT word, SUM(freq) AS total_frequency
        FROM (
            SELECT m.word AS word, COUNT(*)::bigint AS freq
            FROM documents d
            JOIN LATERAL regexp_matches(lower(
                concat_ws(' ', d.title, d.description, d.content)
            ), '[a-z]{2,32}', 'g') AS m(word) ON TRUE
            WHERE d.status = 'done'
            GROUP BY m.word

            UNION ALL

            SELECT m.word AS word, COUNT(*)::bigint AS freq
            FROM news_articles na
            JOIN LATERAL regexp_matches(lower(
                concat_ws(' ', na.title, na.description, na.content)
            ), '[a-z]{2,32}', 'g') AS m(word) ON TRUE
            GROUP BY m.word
        ) all_words
        GROUP BY word
        ON CONFLICT (word) DO UPDATE
        SET total_frequency = EXCLUDED.total_frequency;
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS words")

