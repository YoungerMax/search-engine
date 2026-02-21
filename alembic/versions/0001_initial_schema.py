"""initial schema

Revision ID: 0001_initial_schema
Revises: 
Create Date: 2026-02-21 00:00:00
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "0001_initial_schema"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TYPE crawl_status AS ENUM (
          'queued',
          'in_progress',
          'done',
          'validation_error',
          'non_success_status_error',
          'processing_error'
        );
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS documents (
          id BIGSERIAL PRIMARY KEY,
          url TEXT UNIQUE NOT NULL,
          canonical_url TEXT,
          title TEXT,
          description TEXT,
          content TEXT,
          published_at TIMESTAMPTZ,
          updated_at TIMESTAMPTZ,
          word_count INT,
          quality_score FLOAT,
          freshness_score FLOAT,
          status crawl_status DEFAULT 'done',
          created_at TIMESTAMPTZ DEFAULT now()
        );
        CREATE INDEX IF NOT EXISTS idx_documents_status ON documents(status);
        CREATE INDEX IF NOT EXISTS idx_documents_updated_at ON documents(updated_at);
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS links_outgoing (
          source_doc_id BIGINT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
          target_url TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_links_outgoing_source ON links_outgoing(source_doc_id);
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS tokens (
          doc_id BIGINT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
          term TEXT NOT NULL,
          field SMALLINT NOT NULL,
          frequency INT NOT NULL,
          positions INT[] NOT NULL DEFAULT '{}'
        );
        CREATE INDEX IF NOT EXISTS idx_tokens_term ON tokens(term);
        CREATE INDEX IF NOT EXISTS idx_tokens_doc_id ON tokens(doc_id);
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS crawl_queue (
          url TEXT PRIMARY KEY,
          status crawl_status NOT NULL DEFAULT 'queued',
          domain TEXT NOT NULL,
          last_attempt TIMESTAMPTZ,
          attempt_count INT NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_crawl_queue_status ON crawl_queue(status);
        CREATE INDEX IF NOT EXISTS idx_crawl_queue_domain ON crawl_queue(domain);
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS document_fingerprints (
          doc_id BIGINT PRIMARY KEY REFERENCES documents(id) ON DELETE CASCADE,
          fingerprint BIGINT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS links_resolved (
          source_doc_id BIGINT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
          target_doc_id BIGINT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
          PRIMARY KEY(source_doc_id, target_doc_id)
        );

        CREATE TABLE IF NOT EXISTS document_authority (
          doc_id BIGINT PRIMARY KEY REFERENCES documents(id) ON DELETE CASCADE,
          pagerank DOUBLE PRECISION NOT NULL,
          inlink_count INT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS term_statistics (
          term TEXT PRIMARY KEY,
          doc_frequency BIGINT NOT NULL,
          idf DOUBLE PRECISION NOT NULL,
          avg_doc_len DOUBLE PRECISION NOT NULL
        );
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS term_statistics")
    op.execute("DROP TABLE IF EXISTS document_authority")
    op.execute("DROP TABLE IF EXISTS links_resolved")
    op.execute("DROP TABLE IF EXISTS document_fingerprints")
    op.execute("DROP TABLE IF EXISTS crawl_queue")
    op.execute("DROP TABLE IF EXISTS tokens")
    op.execute("DROP TABLE IF EXISTS links_outgoing")
    op.execute("DROP TABLE IF EXISTS documents")
    op.execute("DROP TYPE IF EXISTS crawl_status")
