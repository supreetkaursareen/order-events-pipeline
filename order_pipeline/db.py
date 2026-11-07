"""SQLite target schema and helpers."""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id   TEXT PRIMARY KEY,
    country       TEXT,
    segment       TEXT,
    source_file   TEXT NOT NULL,
    run_id        TEXT NOT NULL,
    loaded_at     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_order_events (
    event_id          TEXT PRIMARY KEY,
    order_id          TEXT NOT NULL,
    customer_id       TEXT NOT NULL REFERENCES dim_customers(customer_id),
    event_type        TEXT NOT NULL CHECK (event_type IN
                        ('created','paid','shipped','refunded','cancelled')),
    amount_cents      INTEGER CHECK (amount_cents IS NULL OR amount_cents >= 0),
    currency          TEXT NOT NULL,
    event_ts          TEXT NOT NULL,
    is_out_of_order   INTEGER NOT NULL DEFAULT 0,
    row_hash          TEXT NOT NULL,
    source_file       TEXT NOT NULL,
    source_record_no  INTEGER NOT NULL,
    run_id            TEXT NOT NULL,
    ingested_at       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_fact_order_ts ON fact_order_events(order_id, event_ts);

CREATE TABLE IF NOT EXISTS rejected_records (
    reject_id         TEXT PRIMARY KEY,
    run_id            TEXT NOT NULL,
    entity            TEXT NOT NULL,
    source_file       TEXT NOT NULL,
    file_sha256       TEXT NOT NULL,
    source_record_no  INTEGER NOT NULL,
    source_line       INTEGER,
    reason_code       TEXT NOT NULL,
    reason_detail     TEXT,
    raw_record        TEXT,
    rejected_at       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_rejects_reason ON rejected_records(reason_code);

CREATE TABLE IF NOT EXISTS file_ledger (
    file_sha256       TEXT PRIMARY KEY,
    source_file       TEXT NOT NULL,
    status            TEXT NOT NULL CHECK (status IN
                        ('IN_PROGRESS','COMPLETED','FAILED_SCHEMA')),
    committed_records INTEGER NOT NULL DEFAULT 0,
    last_run_id       TEXT NOT NULL,
    updated_at        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id            TEXT PRIMARY KEY,
    entity            TEXT NOT NULL,
    source_file       TEXT NOT NULL,
    file_sha256       TEXT,
    started_at        TEXT NOT NULL,
    finished_at       TEXT,
    status            TEXT NOT NULL CHECK (status IN
                        ('RUNNING','SUCCESS','SKIPPED','FAILED')),
    rows_read         INTEGER NOT NULL DEFAULT 0,
    rows_loaded       INTEGER NOT NULL DEFAULT 0,
    rows_rejected     INTEGER NOT NULL DEFAULT 0,
    rows_out_of_order INTEGER NOT NULL DEFAULT 0,
    resumed_after_record INTEGER NOT NULL DEFAULT 0,
    chunk_size        INTEGER,
    failure_reason    TEXT
);

CREATE TABLE IF NOT EXISTS rpt_daily_revenue (
    report_date     TEXT NOT NULL,
    currency        TEXT NOT NULL,
    paid_events     INTEGER NOT NULL,
    paid_cents      INTEGER NOT NULL,
    refund_events   INTEGER NOT NULL,
    refunded_cents  INTEGER NOT NULL,
    net_cents       INTEGER NOT NULL,
    PRIMARY KEY (report_date, currency)
);

-- Current state per order is DERIVED from immutable history (late-arriving /
-- out-of-order events are handled by ordering on event_ts, not arrival order).
DROP VIEW IF EXISTS v_order_current;
CREATE VIEW v_order_current AS
WITH ranked AS (
    SELECT
        order_id, customer_id, event_type, currency, event_ts, event_id,
        ROW_NUMBER() OVER (PARTITION BY order_id
                           ORDER BY event_ts DESC, event_id DESC) AS rn,
        COUNT(*) OVER (PARTITION BY order_id)                      AS event_count,
        MIN(event_ts) OVER (PARTITION BY order_id)                 AS first_event_ts,
        SUM(CASE WHEN event_type='paid'     THEN amount_cents ELSE 0 END)
            OVER (PARTITION BY order_id)                           AS paid_cents,
        SUM(CASE WHEN event_type='refunded' THEN amount_cents ELSE 0 END)
            OVER (PARTITION BY order_id)                           AS refunded_cents
    FROM fact_order_events
)
SELECT order_id, customer_id, event_type AS current_status, currency,
       first_event_ts, event_ts AS last_event_ts, event_count,
       paid_cents, refunded_cents, paid_cents - refunded_cents AS net_cents
FROM ranked WHERE rn = 1;
"""


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def connect(db_path) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA)
    conn.commit()
    return conn
