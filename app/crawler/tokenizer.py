import re
from collections import Counter

from nltk.stem import PorterStemmer

DEFAULT_STOPWORDS = {
    "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into",
    "is", "it", "no", "not", "of", "on", "or", "such", "that", "the", "their", "then",
    "there", "these", "they", "this", "to", "was", "will", "with",
}


def _load_stopwords() -> set[str]:
    try:
        import nltk
        nltk.data.find("corpora/stopwords")
    except LookupError:
        try:
            import nltk
            nltk.download("stopwords", quiet=True)
        except Exception:
            return DEFAULT_STOPWORDS

    try:
        from nltk.corpus import stopwords
        return set(stopwords.words("english"))
    except LookupError:
        return DEFAULT_STOPWORDS


STOPWORDS = _load_stopwords()
TOKEN_RE = re.compile(r"\b[a-zA-Z0-9]{2,}\b")
stemmer = PorterStemmer()


def tokenize(text: str) -> Counter[str]:
    tokens = TOKEN_RE.findall((text or "").lower())
    filtered = [t for t in tokens if t not in STOPWORDS]
    stemmed = [stemmer.stem(t) for t in filtered]
    return Counter(stemmed)