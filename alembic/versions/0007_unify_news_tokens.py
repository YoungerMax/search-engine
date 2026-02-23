"""unify news and web token storage

Revision ID: 0007_unify_news_tokens
Revises: 0006_news_integration
Create Date: 2026-02-22 00:30:00
"""

from alembic import op

revision = "0007_unify_news_tokens"
down_revision = "0006_news_integration"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE tokens
          ALTER COLUMN doc_id DROP NOT NULL;

        ALTER TABLE tokens
          ADD COLUMN IF NOT EXISTS article_url TEXT REFERENCES news_articles(url) ON DELETE CASCADE,
          ADD COLUMN IF NOT EXISTS source_type SMALLINT NOT NULL DEFAULT 1;

        CREATE INDEX IF NOT EXISTS idx_tokens_article_url ON tokens(article_url);
        CREATE INDEX IF NOT EXISTS idx_tokens_source_type ON tokens(source_type);

        INSERT INTO tokens(doc_id, article_url, source_type, term, field, frequency, positions)
        SELECT NULL, nt.article_url, 2, nt.term, 4, nt.frequency, '{}'
        FROM news_tokens nt
        ON CONFLICT DO NOTHING;

        DROP TABLE IF EXISTS news_tokens;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS news_tokens (
          article_url TEXT NOT NULL REFERENCES news_articles(url) ON DELETE CASCADE,
          term TEXT NOT NULL,
          frequency INT NOT NULL,
          PRIMARY KEY(article_url, term)
        );
        CREATE INDEX IF NOT EXISTS idx_news_tokens_term ON news_tokens(term);

        INSERT INTO news_tokens(article_url, term, frequency)
        SELECT t.article_url, t.term, t.frequency
        FROM tokens t
        WHERE t.source_type = 2
          AND t.article_url IS NOT NULL
        ON CONFLICT (article_url, term) DO UPDATE SET frequency = EXCLUDED.frequency;

        DELETE FROM tokens WHERE source_type = 2;

        DROP INDEX IF EXISTS idx_tokens_source_type;
        DROP INDEX IF EXISTS idx_tokens_article_url;
        ALTER TABLE tokens DROP COLUMN IF EXISTS source_type;
        ALTER TABLE tokens DROP COLUMN IF EXISTS article_url;
        ALTER TABLE tokens ALTER COLUMN doc_id SET NOT NULL;
        """
    )
