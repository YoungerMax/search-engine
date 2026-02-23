import re
from collections import Counter

import nltk
from nltk.stem import PorterStemmer
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

DEFAULT_STOPWORDS = {
    "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into",
    "is", "it", "no", "not", "of", "on", "or", "such", "that", "the", "their", "then",
    "there", "these", "they", "this", "to", "was", "will", "with",
}


def _load_stopwords() -> set[str]:
    try:
        nltk.data.find("corpora/stopwords")
    except LookupError:
        try:
            nltk.download("stopwords", quiet=True)
        except Exception:
            return DEFAULT_STOPWORDS

    try:
        return set(stopwords.words("english"))
    except LookupError:
        return DEFAULT_STOPWORDS


def _safe_word_tokenize(text: str) -> list[str]:
    try:
        return word_tokenize(text.lower())
    except LookupError:
        return TOKEN_RE.findall(text.lower())


STOPWORDS = _load_stopwords()
TOKEN_RE = re.compile(r"\b[a-zA-Z0-9]{2,}\b")
stemmer = PorterStemmer()


def tokenize(text: str) -> Counter[str]:
    tokens = _safe_word_tokenize(text)
    filtered = [t for t in tokens if t not in STOPWORDS]
    stemmed = [stemmer.stem(t) for t in filtered]
    return Counter(stemmed)
