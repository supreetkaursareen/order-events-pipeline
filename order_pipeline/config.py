"""Business constants shared by validation, generator and reconciliation."""

REQUIRED_COLUMNS = (
    "event_id", "order_id", "customer_id", "event_type",
    "event_ts", "currency", "amount",
)
# Columns that must be non-empty for every record (amount is conditional).
ALWAYS_REQUIRED_VALUES = (
    "event_id", "order_id", "customer_id", "event_type", "event_ts", "currency",
)
EVENT_TYPES = ("created", "paid", "shipped", "refunded", "cancelled")
AMOUNT_REQUIRED_TYPES = ("created", "paid", "refunded")
CURRENCIES = ("USD", "EUR", "GBP", "INR")
MAX_AMOUNT_CENTS = 100_000_000          # 1,000,000.00 inclusive
MIN_YEAR, MAX_YEAR = 2000, 2100

DEFAULT_CHUNK_SIZE = 10_000

# Reject reason codes (stable, documented in the data dictionary)
MALFORMED_ROW = "MALFORMED_ROW"
MISSING_REQUIRED_FIELD = "MISSING_REQUIRED_FIELD"
INVALID_EVENT_TYPE = "INVALID_EVENT_TYPE"
INVALID_TIMESTAMP = "INVALID_TIMESTAMP"
INVALID_CURRENCY = "INVALID_CURRENCY"
INVALID_AMOUNT = "INVALID_AMOUNT"
INVALID_REFERENCE_CUSTOMER = "INVALID_REFERENCE_CUSTOMER"
DUPLICATE_EVENT = "DUPLICATE_EVENT"
CONFLICTING_DUPLICATE = "CONFLICTING_DUPLICATE"
