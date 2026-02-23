import json
import os
import re
from collections import defaultdict
from pathlib import Path

from fastapi import FastAPI, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel
from psycopg.errors import UndefinedFunction, UndefinedObject

from app.api.search_service import SearchResponse, perform_news_search, perform_web_search, search_service
from app.common.db import get_conn_async
from app.crawler.tokenizer import STOPWORDS
from app.spellcheck.engine import (
    Candidate,
    LexiconEntry,
    SpellCheckerEngine,
    WORD_RE as SPELLCHECK_WORD_RE,
)

app = FastAPI(title="Search API")


class SpellcheckResponse(BaseModel):
    suggestion: str | None


class SpellcheckService:
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

    def __init__(self, *, meta_path: Path | None = None, engine: SpellCheckerEngine | None = None) -> None:
        self.meta_path = meta_path or Path(os.environ.get("SPELLCHECK_META_PATH", "/tmp/spellcheck_meta.json"))
        self.engine = engine or SpellCheckerEngine()
        self._spell_meta_mtime: float | None = None
        self._spell_meta_cache: dict[str, LexiconEntry] = {}

    def load_spell_meta(self) -> dict[str, LexiconEntry]:
        if not self.meta_path.exists():
            return {}
        mtime = self.meta_path.stat().st_mtime
        if self._spell_meta_mtime == mtime and self._spell_meta_cache:
            return self._spell_meta_cache
        payload = json.loads(self.meta_path.read_text())
        self._spell_meta_cache = {
            row["word"]: LexiconEntry(
                word=row["word"],
                doc_frequency=int(row.get("doc_frequency", 0)),
                total_frequency=int(row.get("total_frequency", 0)),
                external_frequency=int(row.get("external_frequency", 0)),
                popularity_score=float(row.get("popularity_score", 0.0)),
            )
            for row in payload.get("words", [])
        }
        self._spell_meta_mtime = mtime
        return self._spell_meta_cache

    async def suggest(self, q: str) -> SpellcheckResponse:
        words = [self.engine.normalize_word(w) for w in SPELLCHECK_WORD_RE.findall(q)]
        words = [w for w in words if w and w not in STOPWORDS]
        if not words:
            return SpellcheckResponse(suggestion=None)

        cached_lexicon = self.load_spell_meta()
        known: dict[str, LexiconEntry] = {word: cached_lexicon[word] for word in words if word in cached_lexicon}

        async with get_conn_async() as conn:
            async with conn.cursor() as cur:
                missing_words = [word for word in words if word not in known]
                if missing_words:
                    await cur.execute(self.SPELLCHECK_KNOWN_SQL, (missing_words,))
                    for row in await cur.fetchall():
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
                    return SpellcheckResponse(suggestion=None)

                candidates_by_word: dict[str, dict[str, Candidate]] = defaultdict(dict)
                try:
                    await cur.execute(
                        self.SPELLCHECK_CANDIDATE_SQL,
                        (
                            suspect,
                            self.SPELLCHECK_MIN_CANDIDATE_POPULARITY,
                            self.SPELLCHECK_MAX_CANDIDATES_PER_WORD,
                        ),
                    )
                    for row in await cur.fetchall():
                        candidate = Candidate(
                            word=row[1],
                            doc_frequency=int(row[2] or 0),
                            total_frequency=int(row[3] or 0),
                            external_frequency=int(row[4] or 0),
                            popularity_score=float(row[5] or 0.0),
                        )
                        candidates_by_word[row[0]][candidate.word] = candidate
                except (UndefinedFunction, UndefinedObject):
                    await conn.rollback()
                    async with conn.cursor() as fallback_cur:
                        for word in set(suspect):
                            await fallback_cur.execute(
                                self.SPELLCHECK_FALLBACK_SQL,
                                (
                                    word,
                                    word,
                                    word,
                                    self.SPELLCHECK_MIN_CANDIDATE_POPULARITY,
                                    self.SPELLCHECK_MAX_CANDIDATES_PER_WORD,
                                ),
                            )
                            for row in await fallback_cur.fetchall():
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
            best = self.engine.choose_correction(
                word=word,
                known=known.get(word),
                candidates=candidates_by_word.get(word, {}).values(),
            )
            if best:
                corrected[word] = best

        if not corrected:
            return SpellcheckResponse(suggestion=None)

        def _replace(match: re.Match[str]) -> str:
            token = match.group(0)
            replacement = corrected.get(token.lower())
            if not replacement:
                return token
            return self.engine.apply_case(token, replacement)

        suggestion = SPELLCHECK_WORD_RE.sub(_replace, q)
        if suggestion == q:
            return SpellcheckResponse(suggestion=None)

        return SpellcheckResponse(suggestion=suggestion)


spellcheck_service = SpellcheckService()


@app.get('/')
def index():
    return FileResponse(path=Path(os.path.dirname(__file__)) / 'search.html')


@app.get("/search", response_model=SearchResponse)
async def search_web(
    q: str = Query(..., min_length=1),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
) -> SearchResponse:
    return await perform_web_search(q=q, limit=limit, offset=offset)


@app.get("/search/news", response_model=SearchResponse)
async def search_news(
    q: str = Query(..., min_length=1),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
) -> SearchResponse:
    return await perform_news_search(q=q, limit=limit, offset=offset)


@app.get("/spellcheck", response_model=SpellcheckResponse)
async def spellcheck(
    q: str = Query(..., min_length=1),
) -> SpellcheckResponse:
    return await spellcheck_service.suggest(q)


SPELLCHECK_META_PATH = spellcheck_service.meta_path
