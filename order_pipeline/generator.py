"""Deterministic synthetic data generator (fixed seed) with SEPARATE answer labels.

Streams rows out through a bounded heap, so the file can be much larger than
what is held in memory. Writes:
    customers.csv            reference dimension
    order_events.csv|jsonl   the events (normal + boundary + defective + duplicate + late rows)
    expected_answers.json    labels computed independently of the pipeline (NOT part of input)
"""
from __future__ import annotations

import csv
import heapq
import json
import random
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

from . import config as C

BASE_START = datetime(2026, 1, 1, tzinfo=timezone.utc)
COUNTRIES = ["IN", "US", "GB", "DE", "FR", "SG", "AE"]
SEGMENTS = ["consumer", "smb", "enterprise"]
COLUMNS = list(C.REQUIRED_COLUMNS)

# defect rates are per base event; each rejected defect is exactly one reason
DEFECT_RATES = [
    ("MALFORMED_ROW", 0.004),
    ("MISSING_REQUIRED_FIELD", 0.004),
    ("INVALID_EVENT_TYPE", 0.003),
    ("INVALID_TIMESTAMP", 0.003),
    ("INVALID_CURRENCY", 0.003),
    ("INVALID_AMOUNT", 0.004),
    ("INVALID_REFERENCE_CUSTOMER", 0.004),
]
P_DUP_IDENTICAL, P_DUP_CONFLICT, P_LATE = 0.015, 0.005, 0.05


def _fmt_amount(cents: int) -> str:
    return f"{cents // 100}.{cents % 100:02d}"


def _fmt_ts(dt: datetime, rng: random.Random) -> str:
    r = rng.random()
    if r < 0.6:
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    if r < 0.8:
        return dt.astimezone(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%dT%H:%M:%S+05:30")
    return dt.strftime("%Y-%m-%d %H:%M:%S")        # naive -> assumed UTC


def _base_events(rng, n_customers):
    """Yield (order_start_dt, event dict) for each business event, grouped by order."""
    o = 0
    t0 = BASE_START
    while True:
        o += 1
        t0 += timedelta(seconds=rng.randint(20, 180))
        order_id = f"ord_{o:08d}"
        cust = f"c_{rng.randint(1, n_customers):06d}"
        cur = rng.choice(C.CURRENCIES)
        r = rng.random()
        amount = rng.randint(100, 250_000)
        if r < 0.004:
            amount = 0                                  # boundary: minimum
        elif r < 0.008:
            amount = C.MAX_AMOUNT_CENTS                 # boundary: maximum
        evs = [("created", t0, amount)]
        if rng.random() < 0.07:
            evs.append(("cancelled", t0 + timedelta(minutes=rng.randint(5, 60)), None))
        else:
            tp = t0 + timedelta(minutes=rng.randint(1, 120))
            evs.append(("paid", tp, amount))
            if rng.random() < 0.9:
                evs.append(("shipped", tp + timedelta(hours=rng.randint(1, 72)), None))
            if rng.random() < 0.08 and amount > 0:
                evs.append(("refunded", tp + timedelta(hours=rng.randint(2, 240)),
                            rng.randint(1, amount)))
        for k, (et, ts, amt) in enumerate(evs, start=1):
            yield t0, {"event_id": f"evt_{o:08d}_{k}", "order_id": order_id, "customer_id": cust,
                       "event_type": et, "currency": cur, "amount_cents": amt, "ts": ts}


def generate(out_dir, rows=500, seed=42, fmt="csv", window_days=2):
    """Generate files; returns the expected-answers dict. `rows` = approx. base events."""
    if fmt not in ("csv", "jsonl"):
        raise ValueError("fmt must be csv or jsonl")
    rng = random.Random(seed)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    n_customers = max(20, rows // 50)

    with open(out / "customers.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["customer_id", "country", "segment"])
        for i in range(1, n_customers + 1):
            w.writerow([f"c_{i:06d}", rng.choice(COUNTRIES), rng.choice(SEGMENTS)])

    events_path = out / f"order_events.{fmt}"
    heap, seq = [], 0                        # (emit_key_seconds, seq, item)
    exp = {"loaded": 0, "rejects": Counter(), "paid": Counter(), "refunded": Counter(),
           "ooo": 0, "orders": {}, "base_events": 0, "dup_rows": 0}
    window = window_days * 86400
    f = open(events_path, "w", newline="", encoding="utf-8")
    writer = csv.writer(f) if fmt == "csv" else None
    if writer:
        writer.writerow(COLUMNS)
    total_rows = 0

    def render(item):
        """item -> output line(s); also does the independent label accounting."""
        nonlocal total_rows
        kind, ev, tsfmt, mut = item["kind"], item["ev"], item["tsfmt"], item["mut"]
        rec = {"event_id": ev["event_id"], "order_id": ev["order_id"],
               "customer_id": ev["customer_id"], "event_type": ev["event_type"],
               "event_ts": tsfmt, "currency": ev["currency"],
               "amount": "" if ev["amount_cents"] is None else _fmt_amount(ev["amount_cents"])}
        malformed = False
        if kind == "dup_conflict":
            rec["currency"] = item["alt_currency"]
        if mut == "MALFORMED_ROW":
            malformed = True
        elif mut == "MISSING_REQUIRED_FIELD":
            if item["missing_amount"]:
                rec["amount"] = ""
            else:
                rec["order_id"] = ""
        elif mut == "INVALID_EVENT_TYPE":
            rec["event_type"] = "teleported"
        elif mut == "INVALID_TIMESTAMP":
            rec["event_ts"] = item["bad_ts"]
        elif mut == "INVALID_CURRENCY":
            rec["currency"] = "XXX"
        elif mut == "INVALID_AMOUNT":
            rec["amount"] = item["bad_amount"]
        elif mut == "INVALID_REFERENCE_CUSTOMER":
            rec["customer_id"] = "c_999999"
        # ---- independent expected-answer accounting (in emission order) -----
        if kind == "dup_identical":
            exp["rejects"]["DUPLICATE_EVENT"] += 1
        elif kind == "dup_conflict":
            exp["rejects"]["CONFLICTING_DUPLICATE"] += 1
        elif mut:
            exp["rejects"][mut] += 1
        else:
            exp["loaded"] += 1
            prev = exp["orders"].get(ev["order_id"])
            if prev is not None and ev["ts"] < prev:
                exp["ooo"] += 1
            exp["orders"][ev["order_id"]] = max(prev, ev["ts"]) if prev else ev["ts"]
            if ev["event_type"] == "paid":
                exp["paid"][ev["currency"]] += ev["amount_cents"]
            elif ev["event_type"] == "refunded":
                exp["refunded"][ev["currency"]] += ev["amount_cents"]
        total_rows += 1
        # ---- write -------------------------------------------------------------
        if fmt == "csv":
            row = [rec[c] for c in COLUMNS]
            if malformed:
                row.append("unexpected_extra_field")
            writer.writerow(row)
        else:
            line = json.dumps(rec)
            if malformed:
                line = line[: len(line) // 2]          # truncated JSON
            f.write(line + "\n")

    def push(key, item):
        nonlocal seq
        seq += 1
        heapq.heappush(heap, (key, seq, item))

    base_count = 0
    last_t0 = None
    for t0, ev in _base_events(rng, n_customers):
        # release everything whose emit key is before this order's start
        key_now = (t0 - BASE_START).total_seconds()
        while heap and heap[0][0] <= key_now:
            render(heapq.heappop(heap)[2])
        if base_count >= rows and ev["event_id"].endswith("_1"):
            break
        base_count += 1
        key = (ev["ts"] - BASE_START).total_seconds()
        if rng.random() < P_LATE:
            key += rng.uniform(3600, window)           # arrives late / out of order
        item = {"kind": "base", "ev": ev, "tsfmt": _fmt_ts(ev["ts"], rng), "mut": None}
        # choose at most one defect for this event
        r, acc = rng.random(), 0.0
        for reason, p in DEFECT_RATES:
            acc += p
            if r < acc:
                has_amt = ev["amount_cents"] is not None
                if reason == "INVALID_AMOUNT" and not has_amt:
                    break
                item["mut"] = reason
                item["missing_amount"] = has_amt and ev["event_type"] in C.AMOUNT_REQUIRED_TYPES and rng.random() < 0.5
                item["bad_ts"] = rng.choice(["2026-13-45T99:00:00Z", "yesterday", "not-a-date"])
                item["bad_amount"] = rng.choice(["-5.00", "abc", "12.345", "1000000.01", "NaN"])
                break
        push(key, item)
        if item["mut"] is None:                        # only valid originals get duplicated
            r2 = rng.random()
            if r2 < P_DUP_IDENTICAL:
                push(key + rng.uniform(1, window),
                     {"kind": "dup_identical", "ev": ev, "tsfmt": item["tsfmt"], "mut": None})
            elif r2 < P_DUP_IDENTICAL + P_DUP_CONFLICT:
                alt = rng.choice([c for c in C.CURRENCIES if c != ev["currency"]])
                push(key + rng.uniform(1, window),
                     {"kind": "dup_conflict", "ev": ev, "tsfmt": item["tsfmt"], "mut": None,
                      "alt_currency": alt})
    while heap:
        render(heapq.heappop(heap)[2])
    f.close()

    labels = {
        "description": "Expected results for a FRESH database loaded from this file. "
                       "Computed by the generator, independent of the pipeline code.",
        "seed": seed, "format": fmt, "source_file": events_path.name,
        "input_rows": total_rows,
        "events_loaded": exp["loaded"],
        "events_rejected": sum(exp["rejects"].values()),
        "rejects_by_reason": dict(sorted(exp["rejects"].items())),
        "out_of_order_events": exp["ooo"],
        "distinct_orders": len(exp["orders"]),
        "paid_cents_by_currency": dict(sorted(exp["paid"].items())),
        "refunded_cents_by_currency": dict(sorted(exp["refunded"].items())),
    }
    (out / "expected_answers.json").write_text(json.dumps(labels, indent=2, sort_keys=True))
    return labels


def write_edge_case_files(out_dir):
    """Small hand-made files for the negative scenarios (schema change, empty batch...)."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / "order_events_schema_change.csv").write_text(
        "event_id,order_id,customer_id,event_type,event_ts,currency,amt\n"
        "evt_x_1,ord_x,c_000001,paid,2026-01-01T00:00:00Z,USD,10.00\n")
    (out / "order_events_empty.csv").write_text(",".join(COLUMNS) + "\n")
    (out / "order_events_extra_column.csv").write_text(
        ",".join(COLUMNS) + ",channel\n"
        "evt_y_1,ord_y,c_000001,paid,2026-01-01T00:00:00Z,USD,10.00,web\n")
