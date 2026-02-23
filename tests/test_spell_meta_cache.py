import json
from pathlib import Path

from app.api import main


def test_load_spell_meta(tmp_path: Path) -> None:
    payload = {
        "words": [
            {
                "word": "search",
                "doc_frequency": 10,
                "total_frequency": 25,
                "external_frequency": 5,
                "popularity_score": 7.5,
            }
        ]
    }
    spell_path = tmp_path / "spellcheck_meta.json"
    spell_path.write_text(json.dumps(payload))

    main.SPELLCHECK_META_PATH = spell_path
    main._spell_meta_mtime = None
    main._spell_meta_cache = {}

    loaded = main._load_spell_meta()
    assert "search" in loaded
    assert loaded["search"].popularity_score == 7.5
