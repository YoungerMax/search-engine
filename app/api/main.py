import os
import re
from collections import defaultdict
import json
from pathlib import Path

from fastapi import FastAPI, Query
from fastapi.responses import FileResponse
from psycopg.errors import UndefinedFunction, UndefinedObject

from app.common.db import get_conn
from app.crawler.tokenizer import STOPWORDS
from app.api.search_service import perform_news_search, perform_web_search
from app.spellcheck.engine import (
    Candidate,
    LexiconEntry,
    WORD_RE as SPELLCHECK_WORD_RE,
    apply_case,
    choose_correction,
    normalize_word,
)

app = FastAPI(title="Search API")



@app.get('/')
def index():
    return FileResponse(path=Path(os.path.dirname(__file__)) / 'search.html')


@app.get("/search")
def search(
    q: str = Query(..., min_length=1),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
) -> dict[str, object]:
    return perform_web_search(q=q, limit=limit, offset=offset)


@app.get("/search/web")
def search_web(
    q: str = Query(..., min_length=1),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
) -> dict[str, object]:
    return perform_web_search(q=q, limit=limit, offset=offset)


@app.get("/search/news")
def search_news(
    q: str = Query(..., min_length=1),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
) -> dict[str, object]:
    return perform_news_search(q=q, limit=limit, offset=offset)


SPELLCHECK_KNOWN_SQL = """
SELECT word, doc_frequency, total_frequency, external_frequency, popularity_score
FROM spellcheck_dictionary
WHERE word = ANY(%s)
"""

SPELLCHECK_CANDIDATE_SQL = """
WITH input_words AS (
  SELECT DISTINCT unnest(%s::text[]) AS input_word
)
SELECT i.input_word,
       s.word,
       s.doc_frequency,
       s.total_frequency,
       s.external_frequency,
       s.popularity_score
FROM input_words i
JOIN LATERAL (
  SELECT word, doc_frequency, total_frequency, external_frequency, popularity_score
  FROM spellcheck_dictionary
  WHERE length(word) BETWEEN GREATEST(2, length(i.input_word) - 2) AND length(i.input_word) + 2
    AND word %% i.input_word
    AND popularity_score >= %s
  ORDER BY similarity(word, i.input_word) DESC, popularity_score DESC
  LIMIT %s
) s ON TRUE
"""
SPELLCHECK_FALLBACK_SQL = """
SELECT word, doc_frequency, total_frequency, external_frequency, popularity_score
FROM spellcheck_dictionary
WHERE length(word) BETWEEN GREATEST(2, length(%s) - 2) AND length(%s) + 2
  AND left(word, 1) = left(%s, 1)
  AND popularity_score >= %s
ORDER BY popularity_score DESC
LIMIT %s
"""
SPELLCHECK_MIN_CANDIDATE_POPULARITY = 2.0
SPELLCHECK_MAX_CANDIDATES_PER_WORD = 120
SPELLCHECK_META_PATH = Path(os.environ.get("SPELLCHECK_META_PATH", "/tmp/spellcheck_meta.json"))
_spell_meta_mtime: float | None = None
_spell_meta_cache: dict[str, LexiconEntry] = {}


def _load_spell_meta() -> dict[str, LexiconEntry]:
    global _spell_meta_mtime, _spell_meta_cache
    if not SPELLCHECK_META_PATH.exists():
        return {}
    mtime = SPELLCHECK_META_PATH.stat().st_mtime
    if _spell_meta_mtime == mtime and _spell_meta_cache:
        return _spell_meta_cache
    payload = json.loads(SPELLCHECK_META_PATH.read_text())
    _spell_meta_cache = {
        row["word"]: LexiconEntry(
            word=row["word"],
            doc_frequency=int(row.get("doc_frequency", 0)),
            total_frequency=int(row.get("total_frequency", 0)),
            external_frequency=int(row.get("external_frequency", 0)),
            popularity_score=float(row.get("popularity_score", 0.0)),
        )
        for row in payload.get("words", [])
    }
    _spell_meta_mtime = mtime
    return _spell_meta_cache


@app.get("/spellcheck")
def spellcheck(
    q: str = Query(..., min_length=1),
) -> dict[str, str | None]:
    words = [normalize_word(w) for w in SPELLCHECK_WORD_RE.findall(q)]
    words = [w for w in words if w and w not in STOPWORDS]
    if not words:
        return {"suggestion": None}

    cached_lexicon = _load_spell_meta()
    known: dict[str, LexiconEntry] = {word: cached_lexicon[word] for word in words if word in cached_lexicon}

    with get_conn() as conn:
        with conn.cursor() as cur:
            missing_words = [word for word in words if word not in known]
            if missing_words:
                cur.execute(SPELLCHECK_KNOWN_SQL, (missing_words,))
                for row in cur.fetchall():
                    known[row[0]] = LexiconEntry(
                        word=row[0],
                        doc_frequency=int(row[1] or 0),
                        total_frequency=int(row[2] or 0),
                        external_frequency=int(row[3] or 0),
                        popularity_score=float(row[4] or 0.0),
                    )

            suspect: list[str] = []
            for word in words:
                entry = known.get(word)
                if entry and entry.popularity_score >= 3.0:
                    continue

                suspect.append(word)

            if not suspect:
                return {"suggestion": None}

            candidates_by_word: dict[str, dict[str, Candidate]] = defaultdict(dict)
            try:
                cur.execute(
                    SPELLCHECK_CANDIDATE_SQL,
                    (
                        suspect,
                        SPELLCHECK_MIN_CANDIDATE_POPULARITY,
                        SPELLCHECK_MAX_CANDIDATES_PER_WORD,
                    ),
                )
                for row in cur.fetchall():
                    candidate = Candidate(
                        word=row[1],
                        doc_frequency=int(row[2] or 0),
                        total_frequency=int(row[3] or 0),
                        external_frequency=int(row[4] or 0),
                        popularity_score=float(row[5] or 0.0),
                    )
                    candidates_by_word[row[0]][candidate.word] = candidate
            except (UndefinedFunction, UndefinedObject):
                conn.rollback()
                with conn.cursor() as fallback_cur:
                    for word in set(suspect):
                        fallback_cur.execute(
                            SPELLCHECK_FALLBACK_SQL,
                            (
                                word,
                                word,
                                word,
                                SPELLCHECK_MIN_CANDIDATE_POPULARITY,
                                SPELLCHECK_MAX_CANDIDATES_PER_WORD,
                            ),
                        )
                        for row in fallback_cur.fetchall():
                            candidate = Candidate(
                                word=row[0],
                                doc_frequency=int(row[1] or 0),
                                total_frequency=int(row[2] or 0),
                                external_frequency=int(row[3] or 0),
                                popularity_score=float(row[4] or 0.0),
                            )
                            candidates_by_word[word][candidate.word] = candidate

    corrected: dict[str, str] = {}
    for word in suspect:
        best = choose_correction(
            word=word,
            known=known.get(word),
            candidates=candidates_by_word.get(word, {}).values(),
        )
        if best:
            corrected[word] = best

    if not corrected:
        return {"suggestion": None}

    def _replace(match: re.Match[str]) -> str:
        token = match.group(0)
        replacement = corrected.get(token.lower())
        if not replacement:
            return token
        return apply_case(token, replacement)

    suggestion = SPELLCHECK_WORD_RE.sub(_replace, q)
    if suggestion == q:
        return {"suggestion": None}

    return {"suggestion": suggestion}
