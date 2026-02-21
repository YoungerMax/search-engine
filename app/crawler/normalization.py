from __future__ import annotations

import re
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

TRACKING_PARAMS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "gclid",
    "fbclid",
}


def normalize_url(raw_url: str) -> str:
    parts = urlsplit(raw_url.strip())
    scheme = parts.scheme.lower() or "https"
    netloc = parts.netloc.lower()
    if not netloc and parts.path:
        netloc = parts.path.lower()
        path = ""
    else:
        path = parts.path or "/"
    path = re.sub(r"/+", "/", path)

    filtered_qs = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=False) if k.lower() not in TRACKING_PARAMS]
    query = urlencode(filtered_qs)
    return urlunsplit((scheme, netloc, path, query, ""))
