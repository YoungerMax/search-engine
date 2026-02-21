import re
from collections import Counter

from nltk.stem import PorterStemmer

STOPWORDS = {
    "a",
    "an",
    "the",
    "and",
    "or",
    "for",
    "to",
    "in",
    "on",
    "of",
    "with",
    "is",
    "are",
}

TOKEN_RE = re.compile(r"\b[a-zA-Z0-9]{2,}\b")
stemmer = PorterStemmer()


def tokenize(text: str) -> Counter[str]:
    lowered = text.lower()
    raw_tokens = TOKEN_RE.findall(lowered)
    filtered = [t for t in raw_tokens if t not in STOPWORDS]
    stemmed = [stemmer.stem(t) for t in filtered]
    return Counter(stemmed)
