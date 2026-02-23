import math
import re
from dataclasses import dataclass
from typing import Iterable

WORD_RE = re.compile(r"\b[a-zA-Z]{2,32}\b")
MAX_EDIT_DISTANCE = 2


@dataclass(frozen=True)
class LexiconEntry:
    word: str
    doc_frequency: int = 0
    total_frequency: int = 0
    external_frequency: int = 0
    popularity_score: float = 0.0


@dataclass(frozen=True)
class Candidate:
    word: str
    doc_frequency: int
    total_frequency: int
    external_frequency: int
    popularity_score: float


class SpellCheckerEngine:
    def normalize_word(self, word: str) -> str:
        return (word or "").strip().lower()

    def iter_words(self, text: str) -> Iterable[str]:
        for token in WORD_RE.findall((text or "").lower()):
            if token:
                yield token

    def generate_deletes(self, word: str, max_distance: int = MAX_EDIT_DISTANCE) -> set[str]:
        deletes: set[str] = set()
        frontier: set[str] = {word}
        for _ in range(max_distance):
            next_frontier: set[str] = set()
            for item in frontier:
                if len(item) < 2:
                    continue
                for idx in range(len(item)):
                    delete = item[:idx] + item[idx + 1 :]
                    if delete in deletes:
                        continue
                    deletes.add(delete)
                    next_frontier.add(delete)
            frontier = next_frontier
        return deletes

    def osa_distance(self, source: str, target: str, max_distance: int = MAX_EDIT_DISTANCE) -> int | None:
        source = self.normalize_word(source)
        target = self.normalize_word(target)

        if source == target:
            return 0
        if not source or not target:
            distance = max(len(source), len(target))
            return distance if distance <= max_distance else None
        if abs(len(source) - len(target)) > max_distance:
            return None

        rows = len(source) + 1
        cols = len(target) + 1
        dp = [[0] * cols for _ in range(rows)]

        for i in range(rows):
            dp[i][0] = i
        for j in range(cols):
            dp[0][j] = j

        for i in range(1, rows):
            row_min = max_distance + 1
            for j in range(1, cols):
                cost = 0 if source[i - 1] == target[j - 1] else 1
                value = min(
                    dp[i - 1][j] + 1,
                    dp[i][j - 1] + 1,
                    dp[i - 1][j - 1] + cost,
                )

                if (
                    i > 1
                    and j > 1
                    and source[i - 1] == target[j - 2]
                    and source[i - 2] == target[j - 1]
                ):
                    value = min(value, dp[i - 2][j - 2] + 1)

                dp[i][j] = value
                if value < row_min:
                    row_min = value

            if row_min > max_distance:
                return None

        distance = dp[-1][-1]
        return distance if distance <= max_distance else None

    def popularity_score(self, doc_frequency: int, total_frequency: int, external_frequency: int) -> float:
        return (
            math.log1p(max(doc_frequency, 0)) * 4.0
            + math.log1p(max(total_frequency, 0)) * 2.0
            + math.log1p(max(external_frequency, 0)) * 3.0
        )

    def choose_correction(
        self,
        *,
        word: str,
        known: LexiconEntry | None,
        candidates: Iterable[Candidate],
        max_distance: int = MAX_EDIT_DISTANCE,
    ) -> str | None:
        normalized_word = self.normalize_word(word)
        if not normalized_word:
            return None

        known_popularity = known.popularity_score if known else 0.0

        best: Candidate | None = None
        best_distance: int | None = None
        best_rank: tuple[int, float, int, int, str] | None = None

        for candidate in candidates:
            if candidate.word == normalized_word:
                continue

            distance = self.osa_distance(normalized_word, candidate.word, max_distance=max_distance)
            if distance is None:
                continue
            if len(normalized_word) <= 3 and distance > 1:
                continue

            rank = (
                distance,
                -candidate.popularity_score,
                -candidate.doc_frequency,
                -candidate.total_frequency,
                candidate.word,
            )
            if best_rank is None or rank < best_rank:
                best_rank = rank
                best = candidate
                best_distance = distance

        if best is None or best_distance is None:
            return None

        if known_popularity > 0.0:
            required_multiplier = 1.8 if best_distance == 1 else 4.0
            if best.popularity_score < (known_popularity * required_multiplier):
                return None
        else:
            minimum = 0.5 if best_distance == 1 else 2.5
            if best.popularity_score < minimum:
                return None

        return best.word

    def apply_case(self, original: str, replacement: str) -> str:
        if original.isupper():
            return replacement.upper()
        if original[:1].isupper() and original[1:].islower():
            return replacement.capitalize()
        return replacement


spellchecker_engine = SpellCheckerEngine()


def normalize_word(word: str) -> str:
    return spellchecker_engine.normalize_word(word)


def iter_words(text: str) -> Iterable[str]:
    return spellchecker_engine.iter_words(text)


def generate_deletes(word: str, max_distance: int = MAX_EDIT_DISTANCE) -> set[str]:
    return spellchecker_engine.generate_deletes(word, max_distance=max_distance)


def osa_distance(source: str, target: str, max_distance: int = MAX_EDIT_DISTANCE) -> int | None:
    return spellchecker_engine.osa_distance(source, target, max_distance=max_distance)


def popularity_score(doc_frequency: int, total_frequency: int, external_frequency: int) -> float:
    return spellchecker_engine.popularity_score(doc_frequency, total_frequency, external_frequency)


def choose_correction(
    *,
    word: str,
    known: LexiconEntry | None,
    candidates: Iterable[Candidate],
    max_distance: int = MAX_EDIT_DISTANCE,
) -> str | None:
    return spellchecker_engine.choose_correction(
        word=word,
        known=known,
        candidates=candidates,
        max_distance=max_distance,
    )


def apply_case(original: str, replacement: str) -> str:
    return spellchecker_engine.apply_case(original, replacement)
