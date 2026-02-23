"""news integration schema

Revision ID: 0006_news_integration
Revises: 0005_spellcheck_trgm_lookup
Create Date: 2026-02-22 00:00:00
"""

from alembic import op

revision = "0006_news_integration"
down_revision = "0005_spellcheck_trgm_lookup"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS news_feeds (
          feed_url TEXT PRIMARY KEY,
          home_url TEXT,
          name TEXT,
          link TEXT,
          image TEXT,
          discovered_by_url TEXT,
          last_published TIMESTAMPTZ,
          last_fetched TIMESTAMPTZ,
          next_fetch_at TIMESTAMPTZ,
          publish_rate_per_hour REAL
        );
        CREATE INDEX IF NOT EXISTS idx_news_feeds_next_fetch ON news_feeds(next_fetch_at);

        CREATE TABLE IF NOT EXISTS news_articles (
          url TEXT PRIMARY KEY,
          feed_url TEXT NOT NULL REFERENCES news_feeds(feed_url) ON DELETE CASCADE,
          title TEXT,
          description TEXT,
          image TEXT,
          content TEXT,
          author TEXT,
          published_at TIMESTAMPTZ,
          updated_at TIMESTAMPTZ DEFAULT now()
        );
        CREATE INDEX IF NOT EXISTS idx_news_articles_published ON news_articles(published_at DESC);

        CREATE TABLE IF NOT EXISTS news_tokens (
          article_url TEXT NOT NULL REFERENCES news_articles(url) ON DELETE CASCADE,
          term TEXT NOT NULL,
          frequency INT NOT NULL,
          PRIMARY KEY(article_url, term)
        );
        CREATE INDEX IF NOT EXISTS idx_news_tokens_term ON news_tokens(term);
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS news_tokens")
    op.execute("DROP TABLE IF EXISTS news_articles")
    op.execute("DROP TABLE IF EXISTS news_feeds")
