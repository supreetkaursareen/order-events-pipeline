"""Source-to-target reconciliation: internal consistency + (optional) labelled expectations."""
from __future__ import annotations

import json
from pathlib import Path

from .db import connect, utc_now_iso
from .reports import DAILY_REVENUE_SQL


def _q(conn, sql, params=()):
    return conn.execute(sql, params).fetchall()


def _nz(d):
    """Drop zero entries so {'USD': 0} and {} compare equal (no value either way)."""
    return {k: v for k, v in d.items() if v} if isinstance(d, dict) else d


def _check(name, expected, actual, note=""):
    expected, actual = _nz(expected), _nz(actual)
    return {"check": name, "expected": expected, "actual": actual,
            "status": "PASS" if expected == actual else "FAIL", "note": note}


def reconcile(db_path, expected_path=None) -> dict:
    conn = connect(db_path)
    checks = []
    try:
        conn.executescript(DAILY_REVENUE_SQL)     # refresh derived report (idempotent)
        conn.commit()
        # --- 1. run accounting: every record read is loaded or rejected -------
        bad = _q(conn, "SELECT COUNT(*) FROM pipeline_runs WHERE entity='order_events' "
                       "AND status IN ('SUCCESS','FAILED') AND rows_read != rows_loaded + rows_rejected")[0][0]
        checks.append(_check("every run: rows_read = rows_loaded + rows_rejected (runs violating)", 0, bad))

        # --- 2. per completed file: committed records == sum(rows_read) -------
        for sha, name, committed in _q(conn, "SELECT file_sha256, source_file, committed_records "
                                             "FROM file_ledger WHERE status='COMPLETED'"):
            total = _q(conn, "SELECT COALESCE(SUM(rows_read),0) FROM pipeline_runs "
                             "WHERE file_sha256=? AND entity='order_events'", (sha,))[0][0]
            checks.append(_check(f"file {name}: records committed = sum of rows_read over its runs",
                                 committed, total))

        # --- 3. source-to-target counts ---------------------------------------
        fact = _q(conn, "SELECT COUNT(*) FROM fact_order_events")[0][0]
        loaded = _q(conn, "SELECT COALESCE(SUM(rows_loaded),0) FROM pipeline_runs WHERE entity='order_events'")[0][0]
        checks.append(_check("fact_order_events rows = sum(rows_loaded) in run log", loaded, fact))
        rej = _q(conn, "SELECT COUNT(*) FROM rejected_records WHERE entity='order_events'")[0][0]
        rejlog = _q(conn, "SELECT COALESCE(SUM(rows_rejected),0) FROM pipeline_runs WHERE entity='order_events'")[0][0]
        checks.append(_check("rejected_records rows = sum(rows_rejected) in run log", rejlog, rej))

        # --- 4. keys / integrity ----------------------------------------------
        checks.append(_check("duplicate event_id rows", 0,
                             _q(conn, "SELECT COUNT(*) - COUNT(DISTINCT event_id) FROM fact_order_events")[0][0]))
        checks.append(_check("orphan customer_id (referential integrity)", 0,
                             _q(conn, "SELECT COUNT(*) FROM fact_order_events f LEFT JOIN dim_customers d "
                                      "ON f.customer_id=d.customer_id WHERE d.customer_id IS NULL")[0][0]))
        checks.append(_check("NULL/blank required fields in fact", 0,
                             _q(conn, "SELECT COUNT(*) FROM fact_order_events WHERE order_id='' OR event_ts='' "
                                      "OR currency='' OR customer_id=''")[0][0]))

        # --- 5. key aggregates: fact vs derived outputs -----------------------
        for ev, col in (("paid", "paid_cents"), ("refunded", "refunded_cents")):
            f = dict(_q(conn, "SELECT currency, SUM(amount_cents) FROM fact_order_events "
                              "WHERE event_type=? GROUP BY currency", (ev,)))
            r = dict(_q(conn, f"SELECT currency, SUM({col}) FROM rpt_daily_revenue GROUP BY currency"))
            checks.append(_check(f"{ev} cents by currency: fact vs rpt_daily_revenue", f, r))
        checks.append(_check("distinct orders: fact vs v_order_current",
                             _q(conn, "SELECT COUNT(DISTINCT order_id) FROM fact_order_events")[0][0],
                             _q(conn, "SELECT COUNT(*) FROM v_order_current")[0][0]))
        checks.append(_check("events: fact vs sum(event_count) in v_order_current", fact,
                             _q(conn, "SELECT COALESCE(SUM(event_count),0) FROM v_order_current")[0][0]))

        # --- 6. independently labelled expectations (generator output) --------
        if expected_path:
            exp = json.loads(Path(expected_path).read_text())
            checks.append(_check("[expected] events loaded", exp["events_loaded"], fact))
            checks.append(_check("[expected] events rejected", exp["events_rejected"], rej))
            actual_reasons = dict(_q(conn, "SELECT reason_code, COUNT(*) FROM rejected_records "
                                           "WHERE entity='order_events' GROUP BY reason_code"))
            checks.append(_check("[expected] rejects by reason", exp["rejects_by_reason"], actual_reasons))
            checks.append(_check("[expected] distinct orders", exp["distinct_orders"],
                                 _q(conn, "SELECT COUNT(DISTINCT order_id) FROM fact_order_events")[0][0]))
            checks.append(_check("[expected] out-of-order (late) events flagged", exp["out_of_order_events"],
                                 _q(conn, "SELECT COALESCE(SUM(is_out_of_order),0) FROM fact_order_events")[0][0]))
            for ev, key in (("paid", "paid_cents_by_currency"), ("refunded", "refunded_cents_by_currency")):
                actual = dict(_q(conn, "SELECT currency, SUM(amount_cents) FROM fact_order_events "
                                       "WHERE event_type=? GROUP BY currency", (ev,)))
                checks.append(_check(f"[expected] {ev} cents by currency", exp[key], actual))
    finally:
        conn.close()
    failed = [c for c in checks if c["status"] == "FAIL"]
    return {"generated_at": utc_now_iso(), "overall": "FAIL" if failed else "PASS",
            "checks_total": len(checks), "checks_failed": len(failed), "checks": checks}


def write_report(report: dict, out_dir) -> None:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / "reconciliation_report.json").write_text(json.dumps(report, indent=2, sort_keys=True))
    lines = [f"# Reconciliation report", "",
             f"Overall: **{report['overall']}** ({report['checks_total'] - report['checks_failed']}"
             f"/{report['checks_total']} checks passed)", "",
             "| status | check | expected | actual |", "|---|---|---|---|"]
    for c in report["checks"]:
        lines.append(f"| {c['status']} | {c['check']} | `{c['expected']}` | `{c['actual']}` |")
    (out / "reconciliation_report.md").write_text("\n".join(lines) + "\n")
