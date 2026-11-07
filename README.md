# Order-Events Pipeline — chunked, idempotent, observable

Streams **large CSV / JSON-Lines** order-event files into a SQLite analytical store **without ever loading the
file into memory**. Bad rows go to a traceable quarantine table, reruns are safe, a crashed load resumes where it
stopped, and a reconciliation report proves the output is correct against labels produced independently by the data generator.

* Python 3.9+ standard library only (pytest for tests). No services, no secrets.
* **58 automated tests**, 150,000+ record run reconciles 18/18 in ~5 s using ~44 MB RAM.

## 1. Quick start (clean environment)

```bash
git clone <this repo> && cd order-events-pipeline
python3 -m pip install -r requirements.txt     # pytest only
make test                                      # 58 tests
make demo                                      # starter sample: ingest -> report -> reconcile -> sample queries
make scale ROWS=150000                         # 150k+ rows, same flow, prints memory + timing
make bench                                     # scaling + chunk-size measurement
```
No `make`? Each target is just a `python3 -m order_pipeline <command>`; see `Makefile`.

| command | purpose |
|---|---|
| `generate --rows N --seed S [--format csv\|jsonl] [--edge-case-files]` | reproducible synthetic data + `expected_answers.json` labels |
| `ingest --customers F --events F [--db] [--chunk-size] [--inject-failure-after-chunks N]` | load customers, then events |
| `report` | rebuild `rpt_daily_revenue`, export CSV reports |
| `reconcile --expected expected_answers.json` | source→target reconciliation (exit code 1 on FAIL) |
| `query --sql-file sql/sample_queries.sql` | run the demo queries |
| `benchmark` | throughput / peak-memory / chunk-size sweep |

## 2. Architecture / data flow

```
 customers.csv ─────────────► load_customers (upsert) ──► dim_customers ─┐ (FK / ref. check)
                                                                          │
 order_events.csv|.jsonl                                                  ▼
   │  RecordReader  (streams 1 record at a time; schema check on header)
   │      │ malformed row ───────────────────────────────┐
   ▼      ▼                                              │
 chunked(N rows) ─► validate_event (pure rules) ─► reference check ─► duplicate check
                         │ reject                    │ reject            │ reject
                         └──────────────┬────────────┴───────────────────┘
                                        ▼
                              rejected_records  (reason_code, raw_record, file, record_no, line)
   valid rows ─► fact_order_events (immutable event history, is_out_of_order flag)
   ── ONE transaction per chunk: facts + rejects + checkpoint (file_ledger.committed_records) ──
                                        │
   pipeline_runs (run_id, status, counts, failure_reason) + runs/<run_id>.json + log
                                        ▼
        v_order_current (view, derived)   rpt_daily_revenue (rebuilt)   CSV exports   reconciliation_report.{md,json}
```
Modules: `readers.py` (streaming I/O) · `validation.py` (pure rules) · `loader.py` (chunking, idempotency,
restart) · `db.py` (schema) · `reports.py` · `reconcile.py` · `generator.py` · `cli.py` · `benchmark.py`.

## 3. How the requirements are met

| Requirement | Where / how |
|---|---|
| Bounded memory | `RecordReader` is a generator; `chunked()` holds one chunk; cursor-streamed exports; bounded `lru_cache` for customer lookups. Peak RSS **43→45 MB while input grew 10×** (`sample_output/benchmark_results.md`); a test asserts Python-level memory is flat for 4× input. |
| Schema validation | Missing required column ⇒ whole file `FAILED` (`SCHEMA_MISMATCH`), nothing loaded, ledger `FAILED_SCHEMA`. Extra/re-ordered columns tolerated with a warning. |
| Business rules | See data dictionary: required fields, enum event types/currencies, amount ≥ 0 with ≤ 2 decimals and ≤ 1,000,000.00, ISO-8601 timestamps normalised to UTC, customer must exist. |
| Duplicates | `event_id` is the PK. Same content ⇒ `DUPLICATE_EVENT`; same id, different content ⇒ `CONFLICTING_DUPLICATE`. **First wins; nothing is silently overwritten.** |
| Reject path | `rejected_records` keeps reason, detail, raw source row, `source_file`, `source_record_no`, `source_line`, `run_id`. |
| Late / out-of-order | Facts are append-only; `is_out_of_order=1` when a later-timestamped event of the order was already loaded; current state is *derived* by `event_ts` (view `v_order_current`), so arrival order never corrupts status. |
| Idempotency | file `sha256` ledger (same file ⇒ `SKIPPED`), PK on events, deterministic reject ids (`INSERT OR REPLACE`), reports rebuilt from scratch, customers upserted. |
| Retry / restart | Chunk commit includes a checkpoint. A failed run is `FAILED` with `failure_reason`; the next run of the same file **resumes after the last committed record** (demo: `--inject-failure-after-chunks 2`). |
| Backfill | A new file overlapping old data is safe: replays are rejected as `DUPLICATE_EVENT`; genuinely new/late events load and are flagged out-of-order. |
| Observability | per-run row in `pipeline_runs`, `output/runs/<run_id>.json` (start/end, read/loaded/rejected/out-of-order, rejects by reason, failure reason, duration, peak RSS), console + optional `--log-file`. |
| Reconciliation | 18 checks: `read = loaded + rejected` per run, committed records = rows read, fact vs run log, no dup keys, no orphans, paid/refund cents fact vs report, order counts, **plus equality with generator labels** (loaded, rejected, rejects by reason, out-of-order, orders, paid/refund cents). |

## 4. Run behaviour cheat-sheet

| Situation | Result |
|---|---|
| Re-run identical file | `SKIPPED`, zero writes |
| Crash mid-file / DB error | `FAILED`, committed chunks kept, **re-run resumes** |
| Header-only or zero-byte file | `SUCCESS`, 0 rows, warning `EMPTY_INPUT` |
| Missing required column | `FAILED` (`SCHEMA_MISMATCH: …`), exit code 2, no data written |
| Input path missing | `FAILED`, recorded in `pipeline_runs` |
| Bad row | quarantined, run continues |

Exit codes: `0` ok/skipped, `2` failed, `1` reconciliation mismatch.

## 5. Naming conventions
snake_case tables/columns; `dim_*` dimensions, `fact_*` facts, `rpt_*` reports, `v_*` views; amounts as integer `*_cents`
(no floating point); timestamps ISO-8601 UTC `YYYY-MM-DDTHH:MM:SSZ`; every loaded/rejected row carries `run_id`,
`source_file`, `source_record_no`; IDs are deterministic (`evt_<order#>_<seq>`, `ord_<n>`, `c_<n>`; reject ids = sha1 of file hash + record no + reason); `run_id = run_<UTC time>_<6 hex>`.

## 6. Data & sample files
* `sample_data/` — starter sample (505 rows, seed 2) with every defect type, plus schema-change / empty / extra-column edge files.
  `expected_answers.json` is the **separate answer key** (never read by the pipeline; only by `reconcile`).
* Larger data: `python3 -m order_pipeline generate --rows 1000000 --out-dir big`; fixed seed ⇒ byte-identical output (tested).
  The generator streams through a bounded heap. All data is synthetic.
* `sample_output/` — committed evidence: reconciliation reports, CSV reports, run metrics, sample query output,
  `test_results.txt`, `benchmark_results.md`, and the 150k-row run summary.

## 7. Performance (1 CPU, SQLite, see `sample_output/benchmark_results.md`)

| input rows | wall s | rows/s | peak RSS MB |
|---:|---:|---:|---:|
| 50,977 | 1.8 | 27.6 k | 43.1 |
| 203,896 | 7.3 | 28.0 k | 44.1 |
| 509,794 | 19.4 | 26.3 k | 44.6 |

**Optimisation applied & measured — transaction-per-chunk instead of per-row commit** (30k base events):
chunk 1 → 7.9 k rows/s, 100 → 22 k, 1,000 → 27.5 k, 10,000 → 28.8 k rows/s. Throughput is ~3.6× better; memory grows
with chunk size (24 MB at 1 → 44 MB at 10k), so 1,000–10,000 is the sweet spot.
**Next optimisation (not done):** replace the three per-row lookups (dup check, out-of-order check, insert) with batched
`executemany` / staging-table + set-based SQL.

## 7b. Known limitations / next steps
* JSON support is **JSON-Lines** (one object per line). A single giant JSON array needs an incremental parser such as `ijson` (extra dependency).
* SQLite is single-writer; production would target Postgres/warehouse with `MERGE`, and partition by event date.
* `dim_customers` is upserted (current-state, no history/SCD2). Facts are fully historical.
* Amounts are not currency-converted; reports are per currency.
* Checkpoint resume assumes the same file content (guaranteed by the sha256 key); a changed file is a *new* file.
* Dedup/ooo checks are per row; fine at ~27 k rows/s, would be batched at larger scale.
* Next: orchestration (Airflow/Prometheus metrics), Parquet output, parallel file-level workers, schema registry.

See `DESIGN.md` for trade-offs and `docs/DATA_DICTIONARY.md` for schemas.
