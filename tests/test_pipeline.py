import json
import pytest
from order_pipeline.loader import ingest_events, load_customers
from order_pipeline.reconcile import reconcile
from order_pipeline.reports import build_reports

G = "e{n},o{n},c1,paid,2026-01-01T00:00:0{n}Z,USD,10.00\n"


def good(n):
    return G.format(n=n)


# ---------------------------------------------------------------- happy path
def test_happy_path(env):
    f = env.events("".join(good(i) for i in range(1, 6)))
    r = ingest_events(env.db, f, chunk_size=2, metrics_dir=env.metrics)
    assert (r.status, r.rows_read, r.rows_loaded, r.rows_rejected) == ("SUCCESS", 5, 5, 0)
    assert env.q("SELECT COUNT(*) FROM fact_order_events")[0][0] == 5
    # traceability fields populated
    src, rec_no, run_id = env.q("SELECT source_file, source_record_no, run_id FROM fact_order_events "
                                "WHERE event_id='e3'")[0]
    assert (src, rec_no, run_id) == ("events.csv", 3, r.run_id)
    # operational metrics persisted
    m = json.loads((env.metrics / f"{r.run_id}.json").read_text())
    assert m["rows_loaded"] == 5 and m["started_at"] and m["finished_at"]


# ----------------------------------------------------------------- empty input
def test_header_only_file_is_success_with_zero_rows(env):
    r = ingest_events(env.db, env.events(""))
    assert (r.status, r.rows_read, r.rows_loaded) == ("SUCCESS", 0, 0)
    assert any("EMPTY_INPUT" in w for w in r.warnings)


def test_zero_byte_file(env, tmp_path):
    p = tmp_path / "zero.csv"
    p.write_text("")
    r = ingest_events(env.db, p)
    assert (r.status, r.rows_read) == ("SUCCESS", 0)


def test_missing_file_fails_cleanly(env, tmp_path):
    r = ingest_events(env.db, tmp_path / "nope.csv")
    assert r.status == "FAILED" and "not found" in r.failure_reason
    assert env.q("SELECT status FROM pipeline_runs WHERE run_id=?", (r.run_id,)) == [("FAILED",)]


# ------------------------------------------------------------- malformed rows
def test_malformed_rows_are_quarantined_not_fatal(env):
    body = good(1) + "e2,o2,c1,paid,2026-01-01T00:00:00Z,USD,10.00,EXTRA\n" + "short,row\n" + good(3)
    r = ingest_events(env.db, env.events(body))
    assert (r.rows_loaded, r.rows_rejected) == (2, 2)
    rows = env.q("SELECT source_record_no, reason_code, raw_record FROM rejected_records ORDER BY 1")
    assert [(a, b) for a, b, _ in rows] == [(2, "MALFORMED_ROW"), (3, "MALFORMED_ROW")]
    assert "EXTRA" in rows[0][2]                       # raw source kept for tracing


def test_jsonl_input_with_broken_line(env, tmp_path):
    p = tmp_path / "e.jsonl"
    ok = {"event_id": "j1", "order_id": "o1", "customer_id": "c1", "event_type": "paid",
          "event_ts": "2026-01-01T00:00:00Z", "currency": "USD", "amount": 12.5}
    p.write_text(json.dumps(ok) + "\n" + '{"event_id": "j2", "order_' + "\n" + "[1,2]\n\n")
    r = ingest_events(env.db, p)
    assert (r.rows_read, r.rows_loaded, r.rows_rejected) == (3, 1, 2)
    assert env.q("SELECT amount_cents FROM fact_order_events")[0][0] == 1250


# ---------------------------------------------------------------------- nulls
def test_nulls_and_blank_required_fields(env):
    body = good(1) + "e2,,c1,paid,2026-01-01T00:00:00Z,USD,10.00\n" + "e3,o3,c1,paid,2026-01-01T00:00:00Z,USD,\n"
    r = ingest_events(env.db, env.events(body))
    assert r.rejects_by_reason == {"MISSING_REQUIRED_FIELD": 2}
    assert r.rows_loaded == 1


def test_null_amount_allowed_for_shipped(env):
    r = ingest_events(env.db, env.events("e1,o1,c1,shipped,2026-01-01T00:00:00Z,USD,\n"))
    assert r.rows_loaded == 1
    assert env.q("SELECT amount_cents FROM fact_order_events")[0][0] is None


# ----------------------------------------------------------------- duplicates
def test_duplicates_identical_and_conflicting_within_one_file(env):
    body = (good(1) + good(1)                                              # identical replay
            + "e2,o2,c1,paid,2026-01-01T00:00:02Z,USD,10.00\n"
            + "e2,o2,c1,paid,2026-01-01T00:00:02Z,EUR,10.00\n")            # same id, different content
    r = ingest_events(env.db, env.events(body))
    assert r.rows_loaded == 2
    assert r.rejects_by_reason == {"DUPLICATE_EVENT": 1, "CONFLICTING_DUPLICATE": 1}
    # first occurrence wins, never silently overwritten
    assert env.q("SELECT currency FROM fact_order_events WHERE event_id='e2'") == [("USD",)]


def test_duplicate_across_files_backfill_overlap(env):
    ingest_events(env.db, env.events(good(1) + good(2), name="a.csv"))
    r = ingest_events(env.db, env.events(good(2) + good(3), name="b.csv"))   # overlaps on e2
    assert (r.rows_loaded, r.rejects_by_reason) == (1, {"DUPLICATE_EVENT": 1})
    assert env.q("SELECT COUNT(*) FROM fact_order_events")[0][0] == 3


# ----------------------------------------------------------- referential key
def test_invalid_reference_key(env):
    r = ingest_events(env.db, env.events("e1,o1,c_unknown,paid,2026-01-01T00:00:00Z,USD,5.00\n" + good(2)))
    assert r.rejects_by_reason == {"INVALID_REFERENCE_CUSTOMER": 1}
    assert env.q("SELECT COUNT(*) FROM fact_order_events f LEFT JOIN dim_customers d "
                 "USING(customer_id) WHERE d.customer_id IS NULL")[0][0] == 0


# -------------------------------------------------------------- schema change
def test_missing_column_fails_whole_file_and_loads_nothing(env):
    hdr = "event_id,order_id,customer_id,event_type,event_ts,currency,amt\n"
    r = ingest_events(env.db, env.events("e1,o1,c1,paid,2026-01-01T00:00:00Z,USD,1.00\n", header=hdr))
    assert r.status == "FAILED" and "SCHEMA_MISMATCH" in r.failure_reason and "amount" in r.failure_reason
    assert env.q("SELECT COUNT(*) FROM fact_order_events")[0][0] == 0
    assert env.q("SELECT status FROM file_ledger")[0][0] == "FAILED_SCHEMA"


def test_extra_column_and_reordered_columns_are_tolerated(env):
    hdr = "channel,amount,currency,event_ts,event_type,customer_id,order_id,event_id\n"
    r = ingest_events(env.db, env.events("web,10.00,USD,2026-01-01T00:00:00Z,paid,c1,o1,e1\n", header=hdr))
    assert r.status == "SUCCESS" and r.rows_loaded == 1
    assert any("channel" in w for w in r.warnings)


def test_jsonl_missing_required_key_is_schema_failure(env, tmp_path):
    p = tmp_path / "x.jsonl"
    p.write_text(json.dumps({"event_id": "1", "order_id": "o"}) + "\n")
    assert ingest_events(env.db, p).status == "FAILED"


# ------------------------------------------------------- idempotent rerun
def test_rerun_same_file_is_skipped_and_creates_no_duplicates(env):
    f = env.events("".join(good(i) for i in range(1, 6)))
    first = ingest_events(env.db, f)
    before = env.q("SELECT COUNT(*) FROM fact_order_events")[0][0]
    second = ingest_events(env.db, f)
    assert first.status == "SUCCESS" and second.status == "SKIPPED"
    assert env.q("SELECT COUNT(*) FROM fact_order_events")[0][0] == before
    assert env.q("SELECT COUNT(*) FROM rejected_records")[0][0] == 0


def test_reports_are_idempotent(env, tmp_path):
    ingest_events(env.db, env.events(good(1) + good(2)))
    build_reports(env.db, tmp_path / "r1")
    a = (tmp_path / "r1" / "daily_revenue.csv").read_text()
    build_reports(env.db, tmp_path / "r2")
    assert a == (tmp_path / "r2" / "daily_revenue.csv").read_text()


# ---------------------------------------------- partial failure and retry
def test_partial_failure_then_resume_equals_clean_run(env, tmp_path):
    body = "".join(f"e{i},o{i},c1,paid,2026-01-01T00:00:00Z,USD,1.00\n" for i in range(1, 11))
    bad_row = "e5,o5,c_unknown,paid,2026-01-01T00:00:00Z,USD,1.00\n"
    body = body.replace(body.splitlines(True)[4], bad_row)              # record 5 -> invalid ref
    f = env.events(body)
    failed = ingest_events(env.db, f, chunk_size=3, fail_after_chunks=2)   # commits records 1..6
    assert failed.status == "FAILED" and "InjectedFailure" in failed.failure_reason
    assert env.q("SELECT committed_records, status FROM file_ledger") == [(6, "IN_PROGRESS")]
    assert env.q("SELECT COUNT(*) FROM fact_order_events")[0][0] == 5      # records 1-6 minus 1 reject
    retry = ingest_events(env.db, f, chunk_size=3)
    assert retry.status == "SUCCESS" and retry.resumed_after_record == 6
    assert retry.rows_read == 4                                            # only records 7..10 reprocessed
    # identical to a clean single run in a second database
    from order_pipeline.loader import load_customers as lc
    clean_db = tmp_path / "clean.db"
    lc(clean_db, env.customers)
    ingest_events(clean_db, f, chunk_size=3)
    import sqlite3
    sel = "SELECT event_id, amount_cents, event_ts FROM fact_order_events ORDER BY 1"
    clean = sqlite3.connect(clean_db).execute(sel).fetchall()
    assert env.q(sel) == clean
    assert env.q("SELECT COUNT(*) FROM rejected_records")[0][0] == 1      # reject not duplicated
    assert reconcile(env.db)["overall"] == "PASS"


# -------------------------------------------- late / out-of-order data
def test_late_event_is_flagged_and_current_state_uses_event_time(env):
    body = ("e1,o1,c1,created,2026-01-01T10:00:00Z,USD,10.00\n"
            "e3,o1,c1,shipped,2026-01-03T10:00:00Z,USD,\n"
            "e2,o1,c1,paid,2026-01-02T10:00:00Z,USD,10.00\n")               # arrives late
    r = ingest_events(env.db, env.events(body))
    assert r.rows_out_of_order == 1
    assert env.q("SELECT is_out_of_order FROM fact_order_events WHERE event_id='e2'") == [(1,)]
    status, paid = env.q("SELECT current_status, paid_cents FROM v_order_current WHERE order_id='o1'")[0]
    assert (status, paid) == ("shipped", 1000)              # not 'paid' (last to arrive)


# ---------------------------------------------- domain-specific cases
def test_refund_cannot_be_negative_and_net_is_paid_minus_refunded(env):
    body = ("e1,o1,c1,paid,2026-01-01T00:00:00Z,USD,100.00\n"
            "e2,o1,c1,refunded,2026-01-02T00:00:00Z,USD,30.25\n"
            "e3,o1,c1,refunded,2026-01-03T00:00:00Z,USD,-5.00\n")
    r = ingest_events(env.db, env.events(body))
    assert r.rejects_by_reason == {"INVALID_AMOUNT": 1}
    assert env.q("SELECT net_cents FROM v_order_current")[0][0] == 6975


def test_timezone_offsets_normalised_so_daily_buckets_are_utc(env):
    # 2026-01-02T02:00+05:30 is 2026-01-01T20:30Z -> belongs to Jan 1 in UTC
    ingest_events(env.db, env.events("e1,o1,c1,paid,2026-01-02T02:00:00+05:30,USD,10.00\n"))
    build_reports_dir = env.db.parent / "rep"
    build_reports(env.db, build_reports_dir)
    assert env.q("SELECT report_date FROM rpt_daily_revenue") == [("2026-01-01",)]


# --------------------------------------------------------------- customers
def test_customer_reload_is_idempotent_upsert(env):
    load_customers(env.db, env.customers)
    load_customers(env.db, env.customers)
    assert env.q("SELECT COUNT(*) FROM dim_customers")[0][0] == 2


def test_bad_customer_rows_quarantined(env, tmp_path):
    p = tmp_path / "c2.csv"
    p.write_text("customer_id,country,segment\n,IN,x\nc9,US,smb,extra\nc8,DE,smb\n")
    r = load_customers(env.db, p)
    assert (r.rows_loaded, r.rows_rejected) == (1, 2)
