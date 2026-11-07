"""Chunked, restartable, idempotent loaders for customers and order events."""
from __future__ import annotations

import hashlib
import json
import logging
import resource
import sys
import uuid
from collections import Counter
from dataclasses import dataclass, field, asdict
from functools import lru_cache
from itertools import islice
from pathlib import Path
from typing import Iterable, Iterator, Optional

from . import config as C
from .db import connect, utc_now_iso
from .readers import RecordReader, SchemaError, sha256_file
from .validation import Reject, validate_event

log = logging.getLogger("order_pipeline")


class InjectedFailure(RuntimeError):
    """Raised on purpose by fault-injection (tests / demo of retry)."""


@dataclass
class RunResult:
    run_id: str
    entity: str
    source_file: str
    started_at: str
    status: str = "RUNNING"
    finished_at: Optional[str] = None
    file_sha256: Optional[str] = None
    rows_read: int = 0
    rows_loaded: int = 0
    rows_rejected: int = 0
    rows_out_of_order: int = 0
    resumed_after_record: int = 0
    chunk_size: Optional[int] = None
    failure_reason: Optional[str] = None
    rejects_by_reason: dict = field(default_factory=dict)
    warnings: list = field(default_factory=list)
    duration_seconds: float = 0.0
    peak_rss_mb: Optional[float] = None


def new_run_id() -> str:
    return "run_" + utc_now_iso().replace("-", "").replace(":", "") + "_" + uuid.uuid4().hex[:6]


def peak_rss_mb() -> Optional[float]:
    """Peak resident memory of THIS process in MB (Linux VmHWM; ru_maxrss fallback).

    ru_maxrss can be inherited across fork, so VmHWM is preferred for accuracy.
    """
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmHWM:"):
                    return round(int(line.split()[1]) / 1024, 1)
    except OSError:
        pass
    try:
        v = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except Exception:
        return None
    return round(v / (1024 * 1024 if sys.platform == "darwin" else 1024), 1)


def chunked(it: Iterable, size: int) -> Iterator[list]:
    """Yield lists of at most `size` items; only one chunk is in memory."""
    it = iter(it)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            return
        yield chunk


def _reject_id(sha: str, record_no: int, code: str) -> str:
    return hashlib.sha1(f"{sha}|{record_no}|{code}".encode()).hexdigest()[:20]


def _insert_reject(conn, run_id, entity, source_file, sha, rec, code, detail):
    conn.execute(
        "INSERT OR REPLACE INTO rejected_records VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (_reject_id(sha, rec.record_no, code), run_id, entity, source_file, sha,
         rec.record_no, rec.line_no, code, detail, rec.raw, utc_now_iso()),
    )


def _insert_run(conn, r: RunResult):
    conn.execute(
        "INSERT OR IGNORE INTO pipeline_runs (run_id, entity, source_file, file_sha256, started_at,"
        " status, chunk_size) VALUES (?,?,?,?,?,?,?)",
        (r.run_id, r.entity, r.source_file, r.file_sha256, r.started_at, r.status, r.chunk_size),
    )


def _update_run(conn, r: RunResult):
    conn.execute(
        "UPDATE pipeline_runs SET status=?, finished_at=?, file_sha256=?, rows_read=?,"
        " rows_loaded=?, rows_rejected=?, rows_out_of_order=?, resumed_after_record=?,"
        " failure_reason=? WHERE run_id=?",
        (r.status, r.finished_at, r.file_sha256, r.rows_read, r.rows_loaded,
         r.rows_rejected, r.rows_out_of_order, r.resumed_after_record,
         r.failure_reason, r.run_id),
    )


def write_run_metrics(result: RunResult, metrics_dir) -> Path:
    Path(metrics_dir).mkdir(parents=True, exist_ok=True)
    p = Path(metrics_dir) / f"{result.run_id}.json"
    p.write_text(json.dumps(asdict(result), indent=2, sort_keys=True))
    return p


def _finish(conn, result: RunResult, t0: float, metrics_dir):
    import time
    result.finished_at = utc_now_iso()
    result.duration_seconds = round(time.perf_counter() - t0, 3)
    result.peak_rss_mb = peak_rss_mb()
    _insert_run(conn, result)     # no-op if already recorded (e.g. missing-file failure)
    _update_run(conn, result)
    conn.commit()
    if metrics_dir:
        write_run_metrics(result, metrics_dir)
    log.info(
        "run_id=%s entity=%s file=%s status=%s read=%d loaded=%d rejected=%d "
        "out_of_order=%d duration_s=%.2f reason=%s",
        result.run_id, result.entity, result.source_file, result.status,
        result.rows_read, result.rows_loaded, result.rows_rejected,
        result.rows_out_of_order, result.duration_seconds, result.failure_reason,
    )


# ---------------------------------------------------------------------------
# Customers (small reference dimension): upsert, naturally idempotent
# ---------------------------------------------------------------------------
CUSTOMER_COLUMNS = ("customer_id", "country", "segment")


def load_customers(db_path, path, chunk_size=C.DEFAULT_CHUNK_SIZE, metrics_dir=None) -> RunResult:
    import time
    t0 = time.perf_counter()
    conn = connect(db_path)
    result = RunResult(new_run_id(), "customers", Path(path).name, utc_now_iso(), chunk_size=chunk_size)
    try:
        result.file_sha256 = sha256_file(path)
        _insert_run(conn, result)
        conn.commit()
        reasons = Counter()
        with RecordReader(path, required=("customer_id",)) as reader:
            result.warnings.extend(reader.warnings)
            for chunk in chunked(reader, chunk_size):
                loaded = rejected = 0
                for rec in chunk:
                    if rec.error:
                        _insert_reject(conn, result.run_id, "customers", result.source_file,
                                       result.file_sha256, rec, C.MALFORMED_ROW, rec.error)
                        reasons[C.MALFORMED_ROW] += 1
                        rejected += 1
                        continue
                    cid = str(rec.data.get("customer_id") or "").strip()
                    if not cid:
                        _insert_reject(conn, result.run_id, "customers", result.source_file,
                                       result.file_sha256, rec, C.MISSING_REQUIRED_FIELD,
                                       "missing: customer_id")
                        reasons[C.MISSING_REQUIRED_FIELD] += 1
                        rejected += 1
                        continue
                    conn.execute(
                        "INSERT INTO dim_customers VALUES (?,?,?,?,?,?) "
                        "ON CONFLICT(customer_id) DO UPDATE SET country=excluded.country,"
                        " segment=excluded.segment, source_file=excluded.source_file,"
                        " run_id=excluded.run_id, loaded_at=excluded.loaded_at",
                        (cid, str(rec.data.get("country") or "").strip() or None,
                         str(rec.data.get("segment") or "").strip() or None,
                         result.source_file, result.run_id, utc_now_iso()),
                    )
                    loaded += 1
                conn.commit()
                result.rows_read += len(chunk)
                result.rows_loaded += loaded
                result.rows_rejected += rejected
        result.rejects_by_reason = dict(reasons)
        result.status = "SUCCESS"
    except SchemaError as e:
        conn.rollback()
        result.status, result.failure_reason = "FAILED", f"SCHEMA_MISMATCH: {e}"
    except Exception as e:  # noqa: BLE001 - operator needs the reason recorded
        conn.rollback()
        result.status, result.failure_reason = "FAILED", f"{type(e).__name__}: {e}"
    finally:
        if result.file_sha256 or result.status == "FAILED":
            try:
                _finish(conn, result, t0, metrics_dir)
            finally:
                conn.close()
    return result


# ---------------------------------------------------------------------------
# Order events (large fact): chunked transactions + checkpoint in file_ledger
# ---------------------------------------------------------------------------
def _process_chunk(conn, chunk, result: RunResult, sha: str, customer_exists):
    """Validate + load one chunk. Returns Counter of outcomes. Raises on DB error."""
    out = Counter()
    for rec in chunk:
        out["read"] += 1
        try:
            if rec.error:
                raise Reject(C.MALFORMED_ROW, rec.error)
            ev = validate_event(rec.data)
            if not customer_exists(ev.customer_id):
                raise Reject(C.INVALID_REFERENCE_CUSTOMER,
                             f"customer_id {ev.customer_id!r} not in dim_customers")
            row = conn.execute(
                "SELECT row_hash FROM fact_order_events WHERE event_id=?", (ev.event_id,)
            ).fetchone()
            if row is not None:
                if row[0] == ev.row_hash:
                    raise Reject(C.DUPLICATE_EVENT, f"event_id {ev.event_id!r} already loaded (identical)")
                raise Reject(C.CONFLICTING_DUPLICATE,
                             f"event_id {ev.event_id!r} already loaded with different content")
            late = conn.execute(
                "SELECT 1 FROM fact_order_events WHERE order_id=? AND event_ts>? LIMIT 1",
                (ev.order_id, ev.event_ts),
            ).fetchone() is not None
            conn.execute(
                "INSERT INTO fact_order_events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (ev.event_id, ev.order_id, ev.customer_id, ev.event_type, ev.amount_cents,
                 ev.currency, ev.event_ts, int(late), ev.row_hash, result.source_file,
                 rec.record_no, result.run_id, utc_now_iso()),
            )
            out["loaded"] += 1
            out["out_of_order"] += int(late)
        except Reject as r:
            _insert_reject(conn, result.run_id, "order_events", result.source_file,
                           sha, rec, r.code, r.detail)
            out["rejected"] += 1
            out["reason:" + r.code] += 1
    return out


def ingest_events(db_path, path, chunk_size=C.DEFAULT_CHUNK_SIZE, metrics_dir=None,
                  fail_after_chunks: Optional[int] = None) -> RunResult:
    """Load one events file. Safe to re-run:

    * same file already COMPLETED  -> SKIPPED (no writes)
    * earlier attempt died midway  -> resume after the last committed record
    * overlapping data in other files -> identical replays rejected as DUPLICATE_EVENT
    """
    import time
    t0 = time.perf_counter()
    conn = connect(db_path)
    result = RunResult(new_run_id(), "order_events", Path(path).name, utc_now_iso(), chunk_size=chunk_size)
    reasons = Counter()
    try:
        if not Path(path).is_file():
            raise FileNotFoundError(f"input file not found: {path}")
        sha = result.file_sha256 = sha256_file(path)
        led = conn.execute(
            "SELECT status, committed_records FROM file_ledger WHERE file_sha256=?", (sha,)
        ).fetchone()
        if led and led[0] == "COMPLETED":
            result.status = "SKIPPED"
            result.warnings.append("file already loaded (same sha256); nothing to do")
            _insert_run(conn, result)
            return result
        resume_after = led[1] if led and led[0] == "IN_PROGRESS" else 0
        result.resumed_after_record = resume_after
        _insert_run(conn, result)
        conn.execute(
            "INSERT INTO file_ledger VALUES (?,?,?,?,?,?) ON CONFLICT(file_sha256) DO UPDATE SET"
            " status='IN_PROGRESS', last_run_id=excluded.last_run_id, updated_at=excluded.updated_at",
            (sha, result.source_file, "IN_PROGRESS", resume_after, result.run_id, utc_now_iso()),
        )
        conn.commit()
        if resume_after:
            log.info("resuming %s after record %d", result.source_file, resume_after)

        @lru_cache(maxsize=20_000)           # bounded lookup cache
        def customer_exists(cid: str) -> bool:
            return conn.execute("SELECT 1 FROM dim_customers WHERE customer_id=?", (cid,)).fetchone() is not None

        with RecordReader(path) as reader:
            result.warnings.extend(reader.warnings)
            todo = (r for r in reader if r.record_no > resume_after)
            n_chunks = 0
            for chunk in chunked(todo, chunk_size):
                out = _process_chunk(conn, chunk, result, sha, customer_exists)
                last = chunk[-1].record_no
                conn.execute(
                    "UPDATE file_ledger SET committed_records=?, updated_at=? WHERE file_sha256=?",
                    (last, utc_now_iso(), sha),
                )
                conn.commit()                       # data + rejects + checkpoint atomically
                result.rows_read += out["read"]
                result.rows_loaded += out["loaded"]
                result.rows_rejected += out["rejected"]
                result.rows_out_of_order += out["out_of_order"]
                for k, v in out.items():
                    if k.startswith("reason:"):
                        reasons[k[7:]] += v
                n_chunks += 1
                log.debug("chunk %d committed (through record %d)", n_chunks, last)
                if fail_after_chunks is not None and n_chunks >= fail_after_chunks:
                    raise InjectedFailure(f"injected failure after {n_chunks} chunk(s)")
        if result.rows_read == 0 and resume_after == 0:
            result.warnings.append("EMPTY_INPUT: file contains no data records")
        conn.execute("UPDATE file_ledger SET status='COMPLETED', updated_at=? WHERE file_sha256=?",
                     (utc_now_iso(), sha))
        result.status = "SUCCESS"
    except SchemaError as e:
        conn.rollback()
        result.status, result.failure_reason = "FAILED", f"SCHEMA_MISMATCH: {e}"
        if result.file_sha256:
            conn.execute("UPDATE file_ledger SET status='FAILED_SCHEMA', updated_at=? WHERE file_sha256=?",
                         (utc_now_iso(), result.file_sha256))
    except Exception as e:  # noqa: BLE001 - recorded for the operator, then re-raised via status
        conn.rollback()
        result.status, result.failure_reason = "FAILED", f"{type(e).__name__}: {e}"
        log.error("run %s failed: %s", result.run_id, result.failure_reason)
    finally:
        result.rejects_by_reason = dict(reasons)
        try:
            if result.file_sha256 or result.status == "FAILED":
                _finish(conn, result, t0, metrics_dir)
        finally:
            conn.close()
    return result
