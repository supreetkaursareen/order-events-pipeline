-- 1. Run log: counts per load attempt
SELECT run_id, entity, source_file, status, rows_read, rows_loaded, rows_rejected, rows_out_of_order FROM pipeline_runs ORDER BY started_at;
-- 2. Rejects by reason (data-quality summary)
SELECT reason_code, COUNT(*) AS rows FROM rejected_records WHERE entity='order_events' GROUP BY reason_code ORDER BY rows DESC;
-- 3. Source-to-target reconciliation: read = loaded + rejected
SELECT SUM(rows_read) AS read, SUM(rows_loaded) AS loaded, SUM(rows_rejected) AS rejected, SUM(rows_read) - SUM(rows_loaded) - SUM(rows_rejected) AS unexplained FROM pipeline_runs WHERE entity='order_events';
-- 4. Net revenue per day (UTC) and currency
SELECT report_date, currency, paid_events, paid_cents/100.0 AS paid, refunded_cents/100.0 AS refunded, net_cents/100.0 AS net FROM rpt_daily_revenue ORDER BY report_date, currency;
-- 5. Current order status distribution (derived from event history)
SELECT current_status, COUNT(*) AS orders FROM v_order_current GROUP BY current_status ORDER BY orders DESC;
-- 6. Late-arriving events and the orders they touch
SELECT order_id, event_id, event_type, event_ts, source_file, source_record_no FROM fact_order_events WHERE is_out_of_order=1 ORDER BY order_id LIMIT 10;
-- 7. Trace one rejected record back to its source row
SELECT source_file, source_record_no, source_line, reason_code, reason_detail, raw_record FROM rejected_records WHERE entity='order_events' ORDER BY source_record_no LIMIT 5;
-- 8. Orders whose refunds exceed payments (business anomaly check)
SELECT order_id, paid_cents, refunded_cents FROM v_order_current WHERE refunded_cents > paid_cents;
