import tracemalloc
from order_pipeline.generator import generate
from order_pipeline.loader import ingest_events, load_customers, chunked
from order_pipeline.reconcile import reconcile


def test_generator_is_deterministic(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    generate(a, rows=800, seed=11)
    generate(b, rows=800, seed=11)
    for name in ("order_events.csv", "customers.csv", "expected_answers.json"):
        assert (a / name).read_bytes() == (b / name).read_bytes()
    generate(tmp_path / "c", rows=800, seed=12)
    assert (a / "order_events.csv").read_bytes() != (tmp_path / "c" / "order_events.csv").read_bytes()


def test_labels_are_not_in_the_input_file(tmp_path):
    generate(tmp_path, rows=300, seed=1)
    header = (tmp_path / "order_events.csv").read_text().splitlines()[0]
    assert "expect" not in header and "label" not in header


import pytest


@pytest.mark.parametrize("fmt", ["csv", "jsonl"])
def test_end_to_end_matches_independent_labels(tmp_path, fmt):
    labels = generate(tmp_path, rows=3000, seed=5, fmt=fmt)
    db = tmp_path / "w.db"
    assert load_customers(db, tmp_path / "customers.csv").status == "SUCCESS"
    r = ingest_events(db, tmp_path / f"order_events.{fmt}", chunk_size=700)
    assert r.status == "SUCCESS" and r.rows_read == labels["input_rows"]
    rep = reconcile(db, tmp_path / "expected_answers.json")
    failed = [c for c in rep["checks"] if c["status"] == "FAIL"]
    assert not failed, failed


def test_chunking_helper_bounds_chunk_size():
    sizes = [len(c) for c in chunked(range(10), 4)]
    assert sizes == [4, 4, 2] and list(chunked([], 3)) == []


def test_python_memory_is_bounded_by_chunk_not_by_file(tmp_path):
    """Quadrupling the input must not meaningfully raise Python-level peak memory."""
    peaks = {}
    for rows in (16000, 64000):
        d = tmp_path / str(rows)
        generate(d, rows=rows, seed=3)
        db = d / "w.db"
        load_customers(db, d / "customers.csv")
        tracemalloc.start()
        ingest_events(db, d / "order_events.csv", chunk_size=500)
        peaks[rows] = tracemalloc.get_traced_memory()[1]
        tracemalloc.stop()
    assert peaks[64000] < peaks[16000] * 1.25, peaks    # 4x input -> < 1.25x memory
