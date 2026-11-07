"""Simple performance measurement. Each ingest runs in a fresh subprocess so the
reported peak RSS belongs to that run alone."""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from .generator import generate


def _ingest(workdir: Path, chunk_size: int) -> dict:
    cmd = [sys.executable, "-m", "order_pipeline", "ingest",
           "--customers", str(workdir / "customers.csv"),
           "--events", str(workdir / "order_events.csv"),
           "--db", str(workdir / "bench.db"), "--chunk-size", str(chunk_size),
           "--metrics-dir", str(workdir / "runs")]
    t = time.perf_counter()
    p = subprocess.run(cmd, capture_output=True, text=True, check=True)
    wall = time.perf_counter() - t
    out = json.loads(p.stdout)
    out["wall_seconds"] = round(wall, 2)
    return out


def run(args) -> int:
    lines = ["# Benchmark results", "",
             "Single process, SQLite target, 1 CPU container. Peak RSS is `ru_maxrss` of the ingest process.", ""]
    lines += ["## A. Scaling: memory stays flat as input grows (chunk_size=10000)", "",
              "| base events | input rows | file MB | wall s | rows/s | peak RSS MB |", "|---:|---:|---:|---:|---:|---:|"]
    for n in args.sizes:
        with tempfile.TemporaryDirectory() as d:
            wd = Path(d)
            labels = generate(wd, rows=n, seed=7)
            mb = round((wd / "order_events.csv").stat().st_size / 1e6, 1)
            r = _ingest(wd, 10_000)
            rate = int(r["rows_read"] / r["duration_seconds"]) if r["duration_seconds"] else 0
            lines.append(f"| {n:,} | {labels['input_rows']:,} | {mb} | {r['duration_seconds']} | {rate:,} | {r['peak_rss_mb']} |")
            print(lines[-1], flush=True)
    lines += ["", f"## B. Optimisation: chunked transactions ({args.chunk_sweep_rows:,} base events)", "",
              "| chunk_size | wall s | rows/s | peak RSS MB |", "|---:|---:|---:|---:|"]
    with tempfile.TemporaryDirectory() as d:
        wd = Path(d)
        generate(wd, rows=args.chunk_sweep_rows, seed=7)
        for cs in args.chunk_sizes:
            (wd / "bench.db").unlink(missing_ok=True)
            for ext in ("-wal", "-shm"):
                Path(str(wd / "bench.db") + ext).unlink(missing_ok=True)
            r = _ingest(wd, cs)
            rate = int(r["rows_read"] / r["duration_seconds"]) if r["duration_seconds"] else 0
            lines.append(f"| {cs:,} | {r['duration_seconds']} | {rate:,} | {r['peak_rss_mb']} |")
            print(lines[-1], flush=True)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text("\n".join(lines) + "\n")
    return 0
