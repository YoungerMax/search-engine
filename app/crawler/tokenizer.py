import re
from collections import Counter

import nltk
from nltk.stem import PorterStemmer
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

nltk.download('stopwords')
nltk.download('punkt')
nltk.download('punkt_tab')

STOPWORDS = set(stopwords.words('english'))

TOKEN_RE = re.compile(r"\b[a-zA-Z0-9]{2,}\b")
stemmer = PorterStemmer()


def tokenize(text: str) -> Counter[str]:
    tokens = word_tokenize(text.lower())
    filtered = [t for t in tokens if t not in STOPWORDS]
    stemmed = [stemmer.stem(t) for t in filtered]
    return Counter(stemmed)
