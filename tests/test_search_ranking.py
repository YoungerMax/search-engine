from app.api.main import _extract_query_words, _intent_score, _normalize_text


def test_intent_score_prefers_exact_title_phrase() -> None:
    query_words = _extract_query_words("qwen chat")
    query_phrase = _normalize_text("qwen chat")

    direct_score = _intent_score(
        token_score=28.0,
        matched_terms=2,
        total_terms=2,
        query_phrase=query_phrase,
        query_compact="qwenchat",
        query_words=query_words,
        title="Qwen Chat",
        description="Official chat client",
        url="https://chat.qwen.ai/",
    )
    generic_score = _intent_score(
        token_score=180.0,
        matched_terms=2,
        total_terms=2,
        query_phrase=query_phrase,
        query_compact="qwenchat",
        query_words=query_words,
        title="AI model update",
        description="News about Qwen",
        url="https://huggingface.co/blog/qwen-models",
    )

    assert direct_score > generic_score


def test_intent_score_boosts_compact_status_domain_match() -> None:
    query_words = _extract_query_words("cloudflare status")
    query_phrase = _normalize_text("cloudflare status")

    status_page_score = _intent_score(
        token_score=25.0,
        matched_terms=2,
        total_terms=2,
        query_phrase=query_phrase,
        query_compact="cloudflarestatus",
        query_words=query_words,
        title="System Status",
        description="Current status and incidents",
        url="https://www.cloudflarestatus.com/",
    )
    generic_blog_score = _intent_score(
        token_score=160.0,
        matched_terms=2,
        total_terms=2,
        query_phrase=query_phrase,
        query_compact="cloudflarestatus",
        query_words=query_words,
        title="Cloudflare engineering update",
        description="Some maintenance notes",
        url="https://blog.cloudflare.com/maintenance",
    )

    assert status_page_score > generic_blog_score
