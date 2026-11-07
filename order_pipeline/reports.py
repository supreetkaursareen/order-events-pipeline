"""Analytical outputs built from the immutable fact table (rebuilt idempotently)."""
from __future__ import annotations

import csv
from pathlib import Path

from .db import connect

DAILY_REVENUE_SQL = """
DELETE FROM rpt_daily_revenue;
INSERT INTO rpt_daily_revenue
SELECT substr(event_ts,1,10) AS report_date, currency,
       SUM(event_type='paid'),
       COALESCE(SUM(CASE WHEN event_type='paid' THEN amount_cents END),0),
       SUM(event_type='refunded'),
       COALESCE(SUM(CASE WHEN event_type='refunded' THEN amount_cents END),0),
       COALESCE(SUM(CASE WHEN event_type='paid' THEN amount_cents END),0)
     - COALESCE(SUM(CASE WHEN event_type='refunded' THEN amount_cents END),0)
FROM fact_order_events
WHERE event_type IN ('paid','refunded')
GROUP BY report_date, currency;
"""


def export_query(conn, sql: str, out_path: Path, params=()) -> int:
    """Stream a query result to CSV; returns number of data rows written."""
    cur = conn.execute(sql, params)
    cols = [d[0] for d in cur.description]
    n = 0
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for row in cur:                      # cursor iteration: bounded memory
            w.writerow(row)
            n += 1
    return n


def build_reports(db_path, out_dir) -> dict:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    conn = connect(db_path)
    try:
        conn.executescript(DAILY_REVENUE_SQL)
        conn.commit()
        counts = {
            "daily_revenue.csv": export_query(
                conn, "SELECT * FROM rpt_daily_revenue ORDER BY report_date, currency",
                out / "daily_revenue.csv"),
            "order_current.csv": export_query(
                conn, "SELECT * FROM v_order_current ORDER BY order_id",
                out / "order_current.csv"),
            "rejected_records.csv": export_query(
                conn, "SELECT reject_id, run_id, entity, source_file, source_record_no,"
                      " source_line, reason_code, reason_detail, raw_record"
                      " FROM rejected_records ORDER BY source_file, source_record_no, reason_code",
                out / "rejected_records.csv"),
            "rejects_by_reason.csv": export_query(
                conn, "SELECT entity, source_file, reason_code, COUNT(*) AS rejected_rows"
                      " FROM rejected_records GROUP BY 1,2,3 ORDER BY 1,2,4 DESC",
                out / "rejects_by_reason.csv"),
            "pipeline_runs.csv": export_query(
                conn, "SELECT * FROM pipeline_runs ORDER BY started_at, run_id",
                out / "pipeline_runs.csv"),
        }
    finally:
        conn.close()
    return counts
