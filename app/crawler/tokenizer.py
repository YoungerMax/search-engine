import re
from collections import Counter

from nltk.stem import PorterStemmer


TOKEN_RE = re.compile(r"\b[a-zA-Z0-9]{2,}\b")
stemmer = PorterStemmer()

_DEFAULT_STOPWORDS = {
    "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is",
    "it", "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there", "these",
    "they", "this", "to", "was", "will", "with",
}


def _load_stopwords() -> set[str]:
    try:
        from nltk.corpus import stopwords

        return set(stopwords.words("english"))
    except LookupError:
        return set(_DEFAULT_STOPWORDS)


STOPWORDS = _load_stopwords()


def tokenize(text: str) -> Counter[str]:
    tokens = TOKEN_RE.findall((text or "").lower())
    filtered = [t for t in tokens if t not in STOPWORDS]
    stemmed = [stemmer.stem(t) for t in filtered]
    return Counter(stemmed)
