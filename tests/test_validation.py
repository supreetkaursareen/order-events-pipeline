import pytest
from order_pipeline import config as C
from order_pipeline.validation import Reject, validate_event, parse_amount_cents, parse_timestamp


def rec(**kw):
    base = dict(event_id="e1", order_id="o1", customer_id="c1", event_type="paid",
                event_ts="2026-01-01T00:00:00Z", currency="USD", amount="10.00")
    base.update(kw)
    return base


def test_valid_event_is_normalised():
    ev = validate_event(rec(event_type="PAID", currency="usd", amount=" 10.5 "))
    assert (ev.event_type, ev.currency, ev.amount_cents) == ("paid", "USD", 1050)


def test_validation_is_deterministic():
    assert validate_event(rec()).row_hash == validate_event(rec()).row_hash


@pytest.mark.parametrize("field", ["event_id", "order_id", "customer_id", "event_type", "event_ts", "currency"])
def test_missing_required_field(field):
    with pytest.raises(Reject) as e:
        validate_event(rec(**{field: ""}))
    assert e.value.code == C.MISSING_REQUIRED_FIELD


def test_none_values_count_as_missing():
    with pytest.raises(Reject) as e:
        validate_event(rec(order_id=None))
    assert e.value.code == C.MISSING_REQUIRED_FIELD


def test_amount_required_for_paid_but_not_for_shipped():
    with pytest.raises(Reject) as e:
        validate_event(rec(amount=""))
    assert e.value.code == C.MISSING_REQUIRED_FIELD
    assert validate_event(rec(event_type="shipped", amount="")).amount_cents is None


# --- domain edge cases ---------------------------------------------------
@pytest.mark.parametrize("text,cents", [("0", 0), ("0.00", 0), ("0.01", 1), ("1000000.00", 100_000_000), ("12", 1200)])
def test_amount_boundaries_accepted(text, cents):
    assert parse_amount_cents(text) == cents


@pytest.mark.parametrize("text", ["-0.01", "1000000.01", "12.345", "abc", "NaN", "Infinity", ""])
def test_amount_invalid(text):
    with pytest.raises(Reject) as e:
        parse_amount_cents(text)
    assert e.value.code == C.INVALID_AMOUNT


@pytest.mark.parametrize("text", [
    "2026-01-01T05:30:00+05:30", "2026-01-01T00:00:00Z", "2026-01-01 00:00:00", "2026-01-01T00:00:00",
])
def test_timestamps_normalised_to_utc(text):
    assert parse_timestamp(text) == "2026-01-01T00:00:00Z"


@pytest.mark.parametrize("text", ["yesterday", "2026-13-45T00:00:00Z", "1999-01-01T00:00:00Z"])
def test_bad_timestamp(text):
    with pytest.raises(Reject) as e:
        parse_timestamp(text)
    assert e.value.code == C.INVALID_TIMESTAMP


def test_unknown_currency_and_event_type():
    with pytest.raises(Reject) as e:
        validate_event(rec(currency="XXX"))
    assert e.value.code == C.INVALID_CURRENCY
    with pytest.raises(Reject) as e:
        validate_event(rec(event_type="teleported"))
    assert e.value.code == C.INVALID_EVENT_TYPE
