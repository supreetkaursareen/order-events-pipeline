"""Pure, deterministic record validation / normalisation (no I/O)."""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Optional

from . import config as C


class Reject(Exception):
    """A record failed validation. Carries a stable code and human detail."""

    def __init__(self, code: str, detail: str):
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.detail = detail


@dataclass(frozen=True)
class OrderEvent:
    event_id: str
    order_id: str
    customer_id: str
    event_type: str
    amount_cents: Optional[int]
    currency: str
    event_ts: str          # ISO-8601, UTC, second precision, 'Z' suffix

    @property
    def row_hash(self) -> str:
        key = "|".join([
            self.event_id, self.order_id, self.customer_id, self.event_type,
            "" if self.amount_cents is None else str(self.amount_cents),
            self.currency, self.event_ts,
        ])
        return hashlib.sha1(key.encode("utf-8")).hexdigest()


def _s(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def parse_timestamp(text: str) -> str:
    """Parse ISO-8601; naive timestamps are assumed UTC; output normalised UTC."""
    t = text.strip()
    if t[-1:] in ("Z", "z"):
        t = t[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(t)
    except ValueError:
        raise Reject(C.INVALID_TIMESTAMP, f"not ISO-8601: {text!r}")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc).replace(microsecond=0)
    if not (C.MIN_YEAR <= dt.year <= C.MAX_YEAR):
        raise Reject(C.INVALID_TIMESTAMP, f"year out of range: {text!r}")
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_amount_cents(text: str) -> int:
    """Decimal string with <= 2 decimals, 0 <= x <= 1,000,000.00 -> integer cents."""
    try:
        d = Decimal(text)
    except InvalidOperation:
        raise Reject(C.INVALID_AMOUNT, f"not a number: {text!r}")
    if not d.is_finite():
        raise Reject(C.INVALID_AMOUNT, f"not finite: {text!r}")
    if d < 0:
        raise Reject(C.INVALID_AMOUNT, f"negative amount: {text!r}")
    if d.as_tuple().exponent < -2:
        raise Reject(C.INVALID_AMOUNT, f"more than 2 decimals: {text!r}")
    cents = int(d * 100)
    if cents > C.MAX_AMOUNT_CENTS:
        raise Reject(C.INVALID_AMOUNT, f"above maximum 1000000.00: {text!r}")
    return cents


def validate_event(rec: dict) -> OrderEvent:
    """Apply schema + business rules in a fixed precedence order.

    Precedence (first failure wins): missing required -> event_type ->
    conditional amount presence -> timestamp -> currency -> amount value.
    Reference (customer) and duplicate checks need the database and are done
    by the loader afterwards.
    """
    vals = {c: _s(rec.get(c)) for c in C.REQUIRED_COLUMNS}

    missing = [c for c in C.ALWAYS_REQUIRED_VALUES if not vals[c]]
    if missing:
        raise Reject(C.MISSING_REQUIRED_FIELD, f"missing: {','.join(missing)}")

    event_type = vals["event_type"].lower()
    if event_type not in C.EVENT_TYPES:
        raise Reject(C.INVALID_EVENT_TYPE, f"unknown event_type {vals['event_type']!r}")

    if event_type in C.AMOUNT_REQUIRED_TYPES and not vals["amount"]:
        raise Reject(C.MISSING_REQUIRED_FIELD, f"amount required for {event_type}")

    event_ts = parse_timestamp(vals["event_ts"])

    currency = vals["currency"].upper()
    if currency not in C.CURRENCIES:
        raise Reject(C.INVALID_CURRENCY, f"unsupported currency {vals['currency']!r}")

    amount_cents = parse_amount_cents(vals["amount"]) if vals["amount"] else None

    return OrderEvent(
        event_id=vals["event_id"], order_id=vals["order_id"],
        customer_id=vals["customer_id"], event_type=event_type,
        amount_cents=amount_cents, currency=currency, event_ts=event_ts,
    )
