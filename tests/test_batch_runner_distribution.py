from app.batch.runner import _should_run_global_jobs


def test_run_global_jobs_default_single_node() -> None:
    assert _should_run_global_jobs(1, 0)


def test_run_global_jobs_auto_uses_node_zero(monkeypatch) -> None:
    monkeypatch.delenv("BATCH_ROLE", raising=False)
    assert _should_run_global_jobs(3, 0)
    assert not _should_run_global_jobs(3, 1)


def test_run_global_jobs_honors_explicit_role(monkeypatch) -> None:
    monkeypatch.setenv("BATCH_ROLE", "worker")
    assert not _should_run_global_jobs(1, 0)
    monkeypatch.setenv("BATCH_ROLE", "coordinator")
    assert _should_run_global_jobs(3, 2)
