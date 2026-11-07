# Data dictionary

## Source: `customers.csv`
| column | type | null | notes |
|---|---|---|---|
| customer_id | text | no | key; unique after load (last occurrence wins) |
| country, segment | text | yes | |

## Source: `order_events.csv` / `.jsonl` (grain: one event per row)
| column | type | null | rule |
|---|---|---|---|
| event_id | text | no | globally unique key (`evt_<order#>_<seq>`) |
| order_id | text | no | business key of the order (`ord_<n>`) |
| customer_id | text | no | must exist in `dim_customers` |
| event_type | text | no | `created, paid, shipped, refunded, cancelled` (case-insensitive) |
| event_ts | text | no | ISO-8601; offsets converted to UTC; naive = UTC; years 2000–2100 |
| currency | text | no | `USD, EUR, GBP, INR` |
| amount | decimal string / number | **conditional** | required for `created, paid, refunded`; ≥ 0; ≤ 2 decimals; ≤ 1,000,000.00 (0.00 and max are valid) |

Extra columns are ignored (warning); column order is irrelevant; a missing column fails the file.

## Target tables
**dim_customers** — `customer_id` PK, `country`, `segment`, `source_file`, `run_id`, `loaded_at`.

**fact_order_events** — grain: one accepted event. `event_id` PK; `order_id`; `customer_id` FK→dim_customers;
`event_type` (CHECK); `amount_cents` INTEGER ≥ 0 or NULL; `currency`; `event_ts` (UTC ISO-8601);
`is_out_of_order` (1 if a later-timestamped event for the same order was already loaded);
`row_hash` (sha1 of normalised content; used to separate identical vs conflicting duplicates);
`source_file`, `source_record_no`, `run_id`, `ingested_at` (lineage). Index `(order_id, event_ts)`.

**rejected_records** — `reject_id` PK (deterministic sha1 of file hash+record no+reason, so retries don't duplicate), `run_id`, `entity`,
`source_file`, `file_sha256`, `source_record_no` (1-based data record), `source_line` (physical line), `reason_code`, `reason_detail`, `raw_record`, `rejected_at`.

| reason_code | meaning |
|---|---|
| MALFORMED_ROW | CSV field-count mismatch / invalid JSON / JSON not an object |
| MISSING_REQUIRED_FIELD | blank required field, or amount missing for created/paid/refunded |
| INVALID_EVENT_TYPE · INVALID_TIMESTAMP · INVALID_CURRENCY · INVALID_AMOUNT | value-level rule violations |
| INVALID_REFERENCE_CUSTOMER | customer_id not in dim_customers |
| DUPLICATE_EVENT | event_id already loaded, identical content |
| CONFLICTING_DUPLICATE | event_id already loaded, different content |

Precedence (first failure wins): malformed → missing → event_type → conditional amount → timestamp → currency → amount → customer → duplicate.

**file_ledger** — `file_sha256` PK, `source_file`, `status` (`IN_PROGRESS|COMPLETED|FAILED_SCHEMA`), `committed_records` (restart checkpoint), `last_run_id`, `updated_at`.

**pipeline_runs** — `run_id` PK, `entity`, `source_file`, `file_sha256`, `started_at`, `finished_at`, `status` (`RUNNING|SUCCESS|SKIPPED|FAILED`),
`rows_read`, `rows_loaded`, `rows_rejected`, `rows_out_of_order`, `resumed_after_record`, `chunk_size`, `failure_reason`.
Counters include only **committed** chunks, so `rows_read = rows_loaded + rows_rejected` holds even for failed runs.

**rpt_daily_revenue** — PK (`report_date` UTC, `currency`): `paid_events, paid_cents, refund_events, refunded_cents, net_cents`.

**v_order_current** (view) — one row per order, derived: `current_status` (latest by `event_ts`, tie-break `event_id`), `first_event_ts`,
`last_event_ts`, `event_count`, `paid_cents`, `refunded_cents`, `net_cents`.

## Keys & integrity
PK: `event_id`, `customer_id`, `reject_id`, `file_sha256`, `run_id`. FK: fact.customer_id → dim_customers (enforced, `PRAGMA foreign_keys=ON`).
Relationships: customer 1—N orders 1—N events.
