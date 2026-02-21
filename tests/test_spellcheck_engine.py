from app.spellcheck.engine import (
    Candidate,
    LexiconEntry,
    apply_case,
    choose_correction,
    generate_deletes,
    osa_distance,
)


def test_generate_deletes_includes_distance_one_and_two() -> None:
    deletes = generate_deletes("chat", max_distance=2)

    assert "hat" in deletes
    assert "cat" in deletes
    assert "cht" in deletes
    assert "ha" in deletes


def test_osa_distance_handles_transposition() -> None:
    assert osa_distance("cloudfare", "cloudflare", max_distance=2) == 1
    assert osa_distance("cluodflare", "cloudflare", max_distance=2) == 1
    assert osa_distance("qwen", "qwent", max_distance=2) == 1


def test_choose_correction_prefers_high_popularity_with_same_distance() -> None:
    best = choose_correction(
        word="cloudfare",
        known=None,
        candidates=[
            Candidate("cloudflare", 200, 1200, 40, 26.0),
            Candidate("cloudware", 4, 25, 2, 7.0),
        ],
    )

    assert best == "cloudflare"


def test_choose_correction_does_not_replace_popular_known_word() -> None:
    known = LexiconEntry(
        "status",
        doc_frequency=80,
        total_frequency=1000,
        external_frequency=30,
        popularity_score=18.0,
    )
    best = choose_correction(
        word="status",
        known=known,
        candidates=[
            Candidate("statues", 15, 80, 2, 8.0),
            Candidate("states", 25, 120, 3, 10.0),
        ],
    )

    assert best is None


def test_apply_case_preserves_input_style() -> None:
    assert apply_case("Cloudfare", "cloudflare") == "Cloudflare"
    assert apply_case("API", "api") == "API"
    assert apply_case("typo", "fixed") == "fixed"
