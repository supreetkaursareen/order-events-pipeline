import sqlite3
import pytest

HEADER = "event_id,order_id,customer_id,event_type,event_ts,currency,amount\n"
CUSTOMERS = "customer_id,country,segment\nc1,IN,consumer\nc2,US,smb\n"


@pytest.fixture
def env(tmp_path):
    """Fresh workspace with a customers file; helpers to write event files."""
    class Env:
        db = tmp_path / "wh.db"
        customers = tmp_path / "customers.csv"
        metrics = tmp_path / "runs"

        def events(self, body, name="events.csv", header=HEADER):
            p = tmp_path / name
            p.write_text(header + body)
            return p

        def q(self, sql, params=()):
            c = sqlite3.connect(self.db)
            try:
                return c.execute(sql, params).fetchall()
            finally:
                c.close()
    e = Env()
    e.customers.write_text(CUSTOMERS)
    from order_pipeline.loader import load_customers
    assert load_customers(e.db, e.customers).status == "SUCCESS"
    return e
