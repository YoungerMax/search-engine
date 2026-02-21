from fastapi import FastAPI, Query

from app.common.db import get_conn
from app.crawler.tokenizer import tokenize

app = FastAPI(title="Search API")


@app.get("/search")
def search(
    q: str = Query(..., min_length=1),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
) -> dict[str, list[dict[str, str | float]]]:
    query_terms = list(tokenize(q).keys())
    if not query_terms:
        return {"results": []}

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH scored AS (
                  SELECT d.id,
                         d.title,
                         d.description,
                         d.url,
                         SUM(
                           t.frequency
                           * COALESCE(ts.idf, 1.0)
                           * CASE t.field
                               WHEN 1 THEN 2.0
                               WHEN 2 THEN 1.5
                               ELSE 1.0
                             END
                         ) AS score
                  FROM tokens t
                  JOIN documents d ON d.id = t.doc_id
                  LEFT JOIN term_statistics ts ON ts.term = t.term
                  WHERE d.status = 'done'
                    AND t.term = ANY(%s)
                  GROUP BY d.id, d.title, d.description, d.url
                )
                SELECT title, description, url, score
                FROM scored
                ORDER BY score DESC, url ASC
                LIMIT %s OFFSET %s
                """,
                (query_terms, limit, offset),
            )
            rows = cur.fetchall()

    return {
        "results": [
            {
                "title": r[0] or "",
                "description": r[1] or "",
                "url": r[2],
                "score": float(r[3] or 0.0),
            }
            for r in rows
        ]
    }
