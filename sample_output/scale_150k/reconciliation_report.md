# Reconciliation report

Overall: **PASS** (18/18 checks passed)

| status | check | expected | actual |
|---|---|---|---|
| PASS | every run: rows_read = rows_loaded + rows_rejected (runs violating) | `0` | `0` |
| PASS | file order_events.csv: records committed = sum of rows_read over its runs | `152931` | `152931` |
| PASS | fact_order_events rows = sum(rows_loaded) in run log | `146392` | `146392` |
| PASS | rejected_records rows = sum(rows_rejected) in run log | `6539` | `6539` |
| PASS | duplicate event_id rows | `0` | `0` |
| PASS | orphan customer_id (referential integrity) | `0` | `0` |
| PASS | NULL/blank required fields in fact | `0` | `0` |
| PASS | paid cents by currency: fact vs rpt_daily_revenue | `{'EUR': 7039049822, 'GBP': 5057937960, 'INR': 5544392692, 'USD': 5636802550}` | `{'EUR': 7039049822, 'GBP': 5057937960, 'INR': 5544392692, 'USD': 5636802550}` |
| PASS | refunded cents by currency: fact vs rpt_daily_revenue | `{'EUR': 283965118, 'GBP': 221732410, 'INR': 256644398, 'USD': 360853564}` | `{'EUR': 283965118, 'GBP': 221732410, 'INR': 256644398, 'USD': 360853564}` |
| PASS | distinct orders: fact vs v_order_current | `51523` | `51523` |
| PASS | events: fact vs sum(event_count) in v_order_current | `146392` | `146392` |
| PASS | [expected] events loaded | `146392` | `146392` |
| PASS | [expected] events rejected | `6539` | `6539` |
| PASS | [expected] rejects by reason | `{'CONFLICTING_DUPLICATE': 729, 'DUPLICATE_EVENT': 2202, 'INVALID_AMOUNT': 402, 'INVALID_CURRENCY': 446, 'INVALID_EVENT_TYPE': 483, 'INVALID_REFERENCE_CUSTOMER': 601, 'INVALID_TIMESTAMP': 435, 'MALFORMED_ROW': 618, 'MISSING_REQUIRED_FIELD': 623}` | `{'CONFLICTING_DUPLICATE': 729, 'DUPLICATE_EVENT': 2202, 'INVALID_AMOUNT': 402, 'INVALID_CURRENCY': 446, 'INVALID_EVENT_TYPE': 483, 'INVALID_REFERENCE_CUSTOMER': 601, 'INVALID_TIMESTAMP': 435, 'MALFORMED_ROW': 618, 'MISSING_REQUIRED_FIELD': 623}` |
| PASS | [expected] distinct orders | `51523` | `51523` |
| PASS | [expected] out-of-order (late) events flagged | `3159` | `3159` |
| PASS | [expected] paid cents by currency | `{'EUR': 7039049822, 'GBP': 5057937960, 'INR': 5544392692, 'USD': 5636802550}` | `{'EUR': 7039049822, 'GBP': 5057937960, 'INR': 5544392692, 'USD': 5636802550}` |
| PASS | [expected] refunded cents by currency | `{'EUR': 283965118, 'GBP': 221732410, 'INR': 256644398, 'USD': 360853564}` | `{'EUR': 283965118, 'GBP': 221732410, 'INR': 256644398, 'USD': 360853564}` |
