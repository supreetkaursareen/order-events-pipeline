"""Command-line interface:  python -m order_pipeline <command> ..."""
from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from pathlib import Path

from . import config as C
from .generator import generate, write_edge_case_files
from .loader import ingest_events, load_customers
from .reconcile import reconcile, write_report
from .reports import build_reports


def _setup_logging(log_file, verbose):
    handlers = [logging.StreamHandler(sys.stderr)]
    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file))
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO, handlers=handlers,
                        format="%(asctime)s %(levelname)s %(message)s", force=True)


def cmd_generate(a):
    labels = generate(a.out_dir, rows=a.rows, seed=a.seed, fmt=a.format)
    if a.edge_case_files:
        write_edge_case_files(a.out_dir)
    print(json.dumps({k: labels[k] for k in ("input_rows", "events_loaded", "events_rejected")}))
    return 0


def cmd_ingest(a):
    cust = load_customers(a.db, a.customers, chunk_size=a.chunk_size, metrics_dir=a.metrics_dir)
    if cust.status == "FAILED":
        print(json.dumps({"customers": cust.status, "failure_reason": cust.failure_reason}))
        return 2
    ev = ingest_events(a.db, a.events, chunk_size=a.chunk_size, metrics_dir=a.metrics_dir,
                       fail_after_chunks=a.inject_failure_after_chunks)
    summary = {k: getattr(ev, k) for k in (
        "run_id", "status", "source_file", "rows_read", "rows_loaded", "rows_rejected",
        "rows_out_of_order", "resumed_after_record", "duration_seconds", "peak_rss_mb",
        "failure_reason", "rejects_by_reason", "warnings")}
    print(json.dumps(summary, indent=2))
    return 2 if ev.status == "FAILED" else 0


def cmd_report(a):
    print(json.dumps(build_reports(a.db, a.out_dir), indent=2))
    return 0


def cmd_reconcile(a):
    rep = reconcile(a.db, a.expected)
    write_report(rep, a.out_dir)
    for c in rep["checks"]:
        print(f"[{c['status']}] {c['check']}")
    print(f"\nOVERALL: {rep['overall']}  ({rep['checks_total'] - rep['checks_failed']}/{rep['checks_total']} checks)")
    return 0 if rep["overall"] == "PASS" else 1


def cmd_query(a):
    conn = sqlite3.connect(a.db)
    for stmt in [s.strip() for s in Path(a.sql_file).read_text().split(";") if s.strip()]:
        title = next((l[2:].strip() for l in stmt.splitlines() if l.startswith("--")), "")
        print(f"\n== {title}")
        cur = conn.execute(stmt)
        cols = [d[0] for d in cur.description or []]
        print(" | ".join(cols))
        for row in cur.fetchmany(a.limit):
            print(" | ".join("" if v is None else str(v) for v in row))
    return 0


def build_parser():
    p = argparse.ArgumentParser(prog="order_pipeline", description=__doc__)
    p.add_argument("--log-file", default=None)
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("generate", help="create synthetic input + expected-answer labels")
    g.add_argument("--out-dir", default="sample_data")
    g.add_argument("--rows", type=int, default=500, help="approx. number of base events")
    g.add_argument("--seed", type=int, default=42)
    g.add_argument("--format", choices=["csv", "jsonl"], default="csv")
    g.add_argument("--edge-case-files", action="store_true")
    g.set_defaults(fn=cmd_generate)

    i = sub.add_parser("ingest", help="load customers then order events")
    i.add_argument("--customers", required=True)
    i.add_argument("--events", required=True)
    i.add_argument("--db", default="output/warehouse.db")
    i.add_argument("--chunk-size", type=int, default=C.DEFAULT_CHUNK_SIZE)
    i.add_argument("--metrics-dir", default="output/runs")
    i.add_argument("--inject-failure-after-chunks", type=int, default=None,
                   help="fault injection to demo retry/restart")
    i.set_defaults(fn=cmd_ingest)

    r = sub.add_parser("report", help="(re)build analytical reports + CSV exports")
    r.add_argument("--db", default="output/warehouse.db")
    r.add_argument("--out-dir", default="output/reports")
    r.set_defaults(fn=cmd_report)

    c = sub.add_parser("reconcile", help="source-to-target reconciliation")
    c.add_argument("--db", default="output/warehouse.db")
    c.add_argument("--expected", default=None, help="expected_answers.json from the generator")
    c.add_argument("--out-dir", default="output/reports")
    c.set_defaults(fn=cmd_reconcile)

    q = sub.add_parser("query", help="run the SQL statements in a file and print results")
    q.add_argument("--db", default="output/warehouse.db")
    q.add_argument("--sql-file", default="sql/sample_queries.sql")
    q.add_argument("--limit", type=int, default=10)
    q.set_defaults(fn=cmd_query)

    b = sub.add_parser("benchmark", help="throughput / memory / chunk-size measurement")
    b.add_argument("--sizes", type=int, nargs="+", default=[50_000, 200_000, 500_000])
    b.add_argument("--chunk-sizes", type=int, nargs="+", default=[1, 100, 1000, 10000])
    b.add_argument("--chunk-sweep-rows", type=int, default=50_000)
    b.add_argument("--out", default="output/benchmark_results.md")
    b.set_defaults(fn=lambda a: __import__("order_pipeline.benchmark", fromlist=["run"]).run(a))
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    _setup_logging(args.log_file, args.verbose)
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
