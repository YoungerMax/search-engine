from app.common.db import get_conn


def run() -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT AVG(word_count)::float FROM documents WHERE status='done'")
            avg_doc_len = cur.fetchone()[0] or 0.0

            cur.execute("SELECT COUNT(*) FROM documents WHERE status='done'")
            doc_total = cur.fetchone()[0] or 1

            cur.execute("TRUNCATE term_statistics")
            cur.execute(
                """
                INSERT INTO term_statistics(term, doc_frequency, idf, avg_doc_len)
                SELECT t.term,
                       COUNT(DISTINCT t.doc_id) AS df,
                       LN((%s - COUNT(DISTINCT t.doc_id) + 0.5) / (COUNT(DISTINCT t.doc_id) + 0.5) + 1),
                       %s
                FROM tokens t
                GROUP BY t.term
                """,
                (doc_total, avg_doc_len),
            )


if __name__ == "__main__":
    run()
