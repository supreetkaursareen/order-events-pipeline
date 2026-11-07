# Order Events Data Pipeline
### A Chunked, Idempotent & Observable Data Engineering Pipeline

A production-style Python data engineering pipeline that processes large CSV and JSONL order-event files into an analytical SQLite database using memory-efficient, chunk-based ingestion.

The pipeline implements end-to-end data processing, including streaming ingestion, schema validation, data quality checks, duplicate detection, quarantine handling, incremental loading, failure recovery, reporting, and source-to-target reconciliation.

**Key Highlights**
- Processes 150,000+ records with approximately 44 MB peak memory.
- Achieves approximately 27,000–28,000 records per second.
- Supports CSV and JSON Lines (JSONL) input formats.
- Implements transaction-based chunk processing and checkpoint recovery.
- Maintains complete event history with out-of-order event detection.
- Includes 58 automated tests and 18 reconciliation checks.
- Uses Python 3.9+ standard library and SQLite, with pytest for testing.

---

## 1. Project Overview

The pipeline addresses common data engineering challenges associated with processing large datasets reliably and efficiently.

Instead of loading entire files into memory, records are streamed individually and processed in configurable chunks. Each chunk is validated, transformed, and written to SQLite within a transaction.

Invalid records are stored in a quarantine table with their original source information, while valid records are loaded into analytical fact and dimension tables.

The pipeline also supports safe reruns, recovery from partial failures, operational monitoring, and independent reconciliation to verify data correctness.

## 2. Technology Stack

| Category | Technologies |
|---|---|
| Programming Language | Python 3.9+ |
| Data Processing | Python Generators, Chunking, Iterators |
| Database | SQLite, SQL |
| Input Formats | CSV, JSONL |
| Data Quality | Schema Validation, Business Rule Checks |
| Testing | Pytest (58 automated tests) |
| Reporting | SQL, CSV, JSON, Markdown |
| Performance Monitoring | Execution Time, Throughput, Peak Memory |
| Version Control | Git, GitHub |

**Dependencies:** Python standard library, with pytest for automated testing. No external services, API credentials, or cloud infrastructure required.

## 3. Architecture & Data Flow

```text
              SOURCE FILES
             /            \
       customers.csv    order_events.csv / JSONL
             |                   |
             v                   v
      Load Customers        Record Reader
             |                   |
             v                   v
      dim_customers         Schema Validation
             |                   |
             |                   v
             |              Chunk Processing
             |                   |
             |                   v
             |             Data Validation
             |                   |
             |          +--------+--------+
             |          |                 |
             |       Valid Rows       Invalid Rows
             |          |                 |
             |          v                 v
             |     Reference Check   Quarantine Table
             |          |             rejected_records
             |          v
             |     Duplicate Check
             |          |
             |          v
             |   fact_order_events
             |          |
             +----------+
                        |
                        v
                Transaction Commit
                 + Checkpoint
                        |
                        v
                Reporting Layer
                 /     |      \
                v      v       v
              Views  CSV     Reconciliation
                     Reports    Reports
```

**Core processing modules:**

| Module | Responsibility |
|---|---|
| `readers.py` | Streaming CSV and JSONL readers |
| `validation.py` | Schema and business-rule validation |
| `loader.py` | Chunk processing, idempotency, and restart |
| `db.py` | Database schema and connection management |
| `reports.py` | Analytical reports and CSV exports |
| `reconcile.py` | Source-to-target reconciliation |
| `generator.py` | Reproducible synthetic data generation |
| `cli.py` | Command-line interface |
| `benchmark.py` | Performance and memory benchmarking |

## 4. Database Design

The pipeline uses a relational analytical model with separate dimension, fact, operational, and reporting structures.

| Table / View | Purpose |
|---|---|
| `dim_customers` | Customer reference and descriptive information |
| `fact_order_events` | Immutable order-event history |
| `rejected_records` | Invalid and rejected source records |
| `file_ledger` | File-level processing status and checkpoints |
| `pipeline_runs` | Execution metadata and operational metrics |
| `v_order_current` | Derived current order state |
| `rpt_daily_revenue` | Aggregated daily revenue reporting |

**Data modelling decisions:**
- One fact record represents one order event.
- `event_id` is the primary key for event-level uniqueness.
- Customer references are validated before event insertion.
- Foreign-key relationships protect referential integrity.
- Event history is append-only.
- Current order status is derived using event timestamps rather than file arrival order.
- Monetary values are stored as integer cents to avoid floating-point inaccuracies.
- Timestamps are normalized to ISO-8601 UTC format.

## 5. Data Quality & Business Rules

Each incoming record passes through validation before being published to the analytical tables.

**Validation rules include:**

- Required field and schema validation.
- Valid event types and currency codes.
- Non-negative monetary amounts with a maximum of two decimal places.
- Maximum transaction amount of 1,000,000.00.
- ISO-8601 timestamp validation and UTC normalization.
- Customer reference validation.
- Duplicate event detection.
- Conflicting event ID detection.
- Null and malformed record handling.

**Rejected records are stored with:**
- Rejection reason and reason code.
- Original raw record.
- Source file name.
- Source record number and line number.
- Pipeline run ID.

This ensures rejected data remains traceable and can be investigated without interrupting the processing of valid records.

## 6. Idempotency, Recovery & Incremental Processing

Reliability is implemented at both the file and record levels.

**Idempotency**
- SHA-256 file hashing identifies previously processed files.
- Identical files are skipped without unnecessary writes.
- Primary-key constraints prevent duplicate event insertion.
- Identical duplicate events and conflicting duplicate IDs are handled separately.
- Customer records are safely upserted.

**Checkpoint-based recovery**
- Each processing chunk is committed within a database transaction.
- Valid records, rejected records, and checkpoint updates are committed together.
- Failed executions retain previously committed chunks.
- Reprocessing resumes from the last committed record when the same file is supplied.

**Late-arriving events**
- Historical events are preserved.
- Out-of-order arrivals are flagged.
- Current order state is calculated from event timestamps.
- Backfills can introduce new historical events without duplicating existing events.

## 7. Observability & Operational Monitoring

Every pipeline execution generates an operational record containing:

- Unique run ID.
- Execution start and end timestamps.
- Source file and processing status.
- Total records read, loaded, and rejected.
- Rejection counts grouped by reason.
- Out-of-order event count.
- Execution duration.
- Peak memory usage.
- Failure reason, when applicable.

Operational outputs include:
- `pipeline_runs` database table.
- Per-run JSON execution summaries.
- Console logging.
- Optional file-based logs.

This makes pipeline execution traceable and simplifies debugging and operational monitoring.

## 8. Reconciliation & Data Correctness

An independent reconciliation process validates the correctness of the final analytical output.

The data generator creates `expected_answers.json` separately from the pipeline input. The ingestion process never uses these expected results.

The reconciliation process performs 18 checks covering:

1. Records read versus loaded and rejected.
2. Committed records versus processing counts.
3. Fact-table counts versus run metrics.
4. Primary-key uniqueness.
5. Referential integrity and orphan detection.
6. Paid transaction amount reconciliation.
7. Refund amount reconciliation.
8. Order count validation.
9. Loaded records versus independent generator labels.
10. Rejected records versus expected labels.
11. Rejection counts by reason.
12. Out-of-order event counts.
13–18. Additional aggregate and source-to-target consistency checks.

The reconciliation command generates Markdown and JSON reports and returns a non-zero exit code when mismatches are detected.

## 9. Performance & Optimization

The pipeline was benchmarked using increasing input sizes to evaluate throughput and memory consumption.

| Input Records | Execution Time | Throughput | Peak RSS |
|---:|---:|---:|---:|
| 50,977 | 1.8 sec | 27.6K/sec | 43.1 MB |
| 203,896 | 7.3 sec | 28.0K/sec | 44.1 MB |
| 509,794 | 19.4 sec | 26.3K/sec | 44.6 MB |

**Optimization implemented: Transaction-per-chunk processing**

Initially, committing each record individually resulted in significant transaction overhead.

The implementation was optimized to commit records in configurable chunks.

| Chunk Size | Throughput |
|---:|---:|
| 1 | 7.9K records/sec |
| 100 | 22K records/sec |
| 1,000 | 27.5K records/sec |
| 10,000 | 28.8K records/sec |

This improved throughput by approximately 3.6x compared with the single-record transaction approach.

The benchmark also demonstrates that memory consumption remains relatively stable as input size increases.

**Next optimization:** Replace repeated per-record database lookups with batched operations using `executemany` and staging tables with set-based SQL.

## 10. Testing & Edge Cases

The project includes **58 automated tests** covering functional correctness, data quality, recovery, and performance-related behavior.

Test scenarios include:

- Valid CSV and JSONL ingestion.
- Empty and header-only files.
- Malformed input records.
- Missing required columns.
- Unexpected and reordered schema columns.
- Null and invalid values.
- Duplicate and conflicting event IDs.
- Invalid customer references.
- Late-arriving and out-of-order events.
- Partial failure and checkpoint recovery.
- Reprocessing identical input files.
- Source-to-target reconciliation.
- Bounded-memory processing.

Run the complete test suite:

```bash
make test
```

## 11. Project Setup & Execution

### Prerequisites

- Python 3.9 or later.
- Git.
- Pytest.

### Clone the repository

```bash
git clone <your-repository-url>
cd order-events-pipeline
```

### Install dependencies

```bash
python3 -m pip install -r requirements.txt
```

### Run automated tests

```bash
make test
```

### Run the complete demonstration

```bash
make demo
```

The demonstration executes ingestion, reporting, reconciliation, and sample analytical queries.

### Run a larger dataset

```bash
make scale ROWS=150000
```

### Run performance benchmarks

```bash
make bench
```

### Generate a custom dataset

```bash
python3 -m order_pipeline generate \
    --rows 1000000 \
    --seed 42 \
    --out-dir big
```

### Available CLI Commands

| Command | Description |
|---|---|
| `generate` | Generate reproducible synthetic datasets |
| `ingest` | Load customer and order-event files |
| `report` | Rebuild analytical reports |
| `reconcile` | Validate source-to-target correctness |
| `query` | Execute sample SQL queries |
| `benchmark` | Measure throughput and memory usage |

All commands can be configured through their available CLI arguments. See the Makefile and CLI help for details.

## 12. Sample Data & Reproducibility

The repository includes:
- A starter dataset containing approximately 505 rows.
- Synthetic customer and order-event files.
- Deliberately invalid records covering different edge cases.
- Schema-change and empty-input samples.
- Independent expected-answer labels.
- Sample SQL queries and generated reports.
- Benchmark results and test execution evidence.

A fixed random seed ensures reproducible dataset generation. The generator also uses bounded-memory processing.

## 13. Limitations & Future Improvements

Current implementation decisions and potential improvements:

| Current Design | Potential Improvement |
|---|---|
| SQLite analytical storage | PostgreSQL or cloud data warehouse |
| Single-process ingestion | Parallel file-level processing |
| Per-record duplicate checks | Batched SQL and staging tables |
| Current-state customer dimension | Slowly Changing Dimensions (SCD Type 2) |
| CSV and JSONL support | Incremental Parquet processing |
| Local execution and logging | Airflow orchestration and Prometheus monitoring |
| Single-writer database | Distributed processing for larger workloads |

The current implementation intentionally prioritizes correctness, reliability, and reproducibility without introducing unnecessary infrastructure.

---

## Project Summary

This project demonstrates an end-to-end data engineering workflow covering scalable file ingestion, data validation, relational data modelling, transactional processing, idempotency, recovery, observability, performance optimization, and independent data reconciliation.

**Designed to demonstrate reliable data processing under realistic data quality and operational failure scenarios using a lightweight local architecture.**
