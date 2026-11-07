# Reconciliation report

Overall: **PASS** (18/18 checks passed)

| status | check | expected | actual |
|---|---|---|---|
| PASS | every run: rows_read = rows_loaded + rows_rejected (runs violating) | `0` | `0` |
| PASS | file order_events.csv: records committed = sum of rows_read over its runs | `505` | `505` |
| PASS | fact_order_events rows = sum(rows_loaded) in run log | `485` | `485` |
| PASS | rejected_records rows = sum(rows_rejected) in run log | `20` | `20` |
| PASS | duplicate event_id rows | `0` | `0` |
| PASS | orphan customer_id (referential integrity) | `0` | `0` |
| PASS | NULL/blank required fields in fact | `0` | `0` |
| PASS | paid cents by currency: fact vs rpt_daily_revenue | `{'EUR': 5238763, 'GBP': 4422943, 'INR': 4980861, 'USD': 4461393}` | `{'EUR': 5238763, 'GBP': 4422943, 'INR': 4980861, 'USD': 4461393}` |
| PASS | refunded cents by currency: fact vs rpt_daily_revenue | `{'EUR': 77415, 'GBP': 356219, 'INR': 564481, 'USD': 255524}` | `{'EUR': 77415, 'GBP': 356219, 'INR': 564481, 'USD': 255524}` |
| PASS | distinct orders: fact vs v_order_current | `173` | `173` |
| PASS | events: fact vs sum(event_count) in v_order_current | `485` | `485` |
| PASS | [expected] events loaded | `485` | `485` |
| PASS | [expected] events rejected | `20` | `20` |
| PASS | [expected] rejects by reason | `{'CONFLICTING_DUPLICATE': 2, 'DUPLICATE_EVENT': 3, 'INVALID_AMOUNT': 3, 'INVALID_CURRENCY': 1, 'INVALID_EVENT_TYPE': 1, 'INVALID_REFERENCE_CUSTOMER': 4, 'INVALID_TIMESTAMP': 2, 'MALFORMED_ROW': 2, 'MISSING_REQUIRED_FIELD': 2}` | `{'CONFLICTING_DUPLICATE': 2, 'DUPLICATE_EVENT': 3, 'INVALID_AMOUNT': 3, 'INVALID_CURRENCY': 1, 'INVALID_EVENT_TYPE': 1, 'INVALID_REFERENCE_CUSTOMER': 4, 'INVALID_TIMESTAMP': 2, 'MALFORMED_ROW': 2, 'MISSING_REQUIRED_FIELD': 2}` |
| PASS | [expected] distinct orders | `173` | `173` |
| PASS | [expected] out-of-order (late) events flagged | `13` | `13` |
| PASS | [expected] paid cents by currency | `{'EUR': 5238763, 'GBP': 4422943, 'INR': 4980861, 'USD': 4461393}` | `{'EUR': 5238763, 'GBP': 4422943, 'INR': 4980861, 'USD': 4461393}` |
| PASS | [expected] refunded cents by currency | `{'EUR': 77415, 'GBP': 356219, 'INR': 564481, 'USD': 255524}` | `{'EUR': 77415, 'GBP': 356219, 'INR': 564481, 'USD': 255524}` |
