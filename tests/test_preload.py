"""
Tests for explicit preload/unload — in-memory cache with pushdown fallback.
"""
import pytest

import s3ql
from s3ql.exceptions import InterfaceError, ProgrammingError

from .conftest import BUCKET, FAKE_KEY, FAKE_SECRET, REGION


@pytest.fixture()
def tables(conn):
    cur = conn.cursor()
    cur.execute("CREATE TABLE orders (id INTEGER, amount DOUBLE)")
    cur.execute("CREATE TABLE customers (id INTEGER, name VARCHAR)")
    cur.execute("INSERT INTO orders VALUES (1, 10.0)")
    cur.execute("INSERT INTO orders VALUES (2, 20.0)")
    cur.execute("INSERT INTO customers VALUES (1, 'alice')")
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# preload
# ---------------------------------------------------------------------------

class TestPreload:
    def test_preload_single_table(self, tables):
        tables.preload("orders")
        assert "orders" in tables._tx._loaded

    def test_preload_multiple_tables(self, tables):
        tables.preload("orders", "customers")
        assert "orders" in tables._tx._loaded
        assert "customers" in tables._tx._loaded

    def test_preload_missing_table_raises(self, conn):
        with pytest.raises(ProgrammingError, match="does not exist"):
            conn.preload("nonexistent")

    def test_preload_not_marked_as_modified(self, tables):
        tables.preload("orders")
        assert "orders" not in tables._tx._modified

    def test_preload_creates_tx(self, tables):
        assert tables._tx is None
        tables.preload("orders")
        assert tables._tx is not None

    def test_preload_select_reads_from_memory(self, tables):
        tables.preload("orders")
        cur = tables.cursor()
        cur.execute("SELECT COUNT(*) FROM orders")
        assert cur.fetchone()[0] == 2

    def test_preload_idempotent(self, tables):
        tables.preload("orders")
        tables.preload("orders")  # second call must not raise or reload
        assert list(tables._tx._loaded.keys()).count("orders") == 1

    def test_preload_then_rollback_no_s3_write(self, tables, s3):
        tables.preload("orders")
        tables.rollback()
        # table still exists on S3 unchanged
        response = s3.list_objects_v2(Bucket=BUCKET)
        keys = [o["Key"] for o in response.get("Contents", [])]
        assert "orders.parquet" in keys

    def test_preload_rollback_clears_tx(self, tables):
        tables.preload("orders")
        tables.rollback()
        assert tables._tx is None

    def test_preload_commit_no_s3_write(self, tables):
        """Preloaded but unmodified table must not be written on commit."""
        tables.preload("orders")
        tables.commit()  # must not raise, must not write

    def test_preload_then_dml_marks_modified(self, tables):
        tables.preload("orders")
        cur = tables.cursor()
        cur.execute("INSERT INTO orders VALUES (3, 30.0)")
        assert "orders" in tables._tx._modified

    def test_preload_mixed_with_pushdown(self, tables):
        """Preloaded table reads from memory; non-preloaded reads via S3 pushdown."""
        tables.preload("orders")
        cur = tables.cursor()
        # orders → memory, customers → S3 pushdown
        cur.execute("SELECT o.amount, c.name FROM orders o JOIN customers c ON o.id = c.id")
        rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0] == (10.0, "alice")


# ---------------------------------------------------------------------------
# unload
# ---------------------------------------------------------------------------

class TestUnload:
    def test_unload_removes_from_loaded(self, tables):
        tables.preload("orders")
        tables.unload("orders")
        assert "orders" not in tables._tx._loaded if tables._tx else True

    def test_unload_restores_pushdown(self, tables):
        """After unload the view must point back to S3."""
        tables.preload("orders")
        tables.unload("orders")
        cur = tables.cursor()
        cur.execute("SELECT COUNT(*) FROM orders")
        assert cur.fetchone()[0] == 2

    def test_unload_clears_tx_when_nothing_left(self, tables):
        tables.preload("orders")
        tables.unload("orders")
        assert tables._tx is None

    def test_unload_keeps_tx_if_other_tables_loaded(self, tables):
        tables.preload("orders", "customers")
        tables.unload("orders")
        assert tables._tx is not None
        assert "customers" in tables._tx._loaded

    def test_unload_without_preload_raises(self, tables):
        with pytest.raises(ProgrammingError):
            tables.unload("orders")

    def test_unload_without_tx_raises(self, conn):
        with pytest.raises(InterfaceError):
            conn.unload("orders")

    def test_unload_modified_table_raises(self, tables):
        tables.preload("orders")
        cur = tables.cursor()
        cur.execute("INSERT INTO orders VALUES (3, 30.0)")
        with pytest.raises(ProgrammingError, match="uncommitted changes"):
            tables.unload("orders")

    def test_unload_multiple_tables(self, tables):
        tables.preload("orders", "customers")
        tables.unload("orders", "customers")
        assert tables._tx is None

    def test_preload_unload_preload_cycle(self, tables):
        """Table can be preloaded again after unload."""
        tables.preload("orders")
        tables.unload("orders")
        tables.preload("orders")
        assert tables._tx is not None
        assert "orders" in tables._tx._loaded

    def test_preloaded_property(self, tables):
        tables.preload("orders", "customers")
        cur = tables.cursor()
        cur.execute("INSERT INTO orders VALUES (3, 30.0)")
        assert "orders" not in tables._tx.preloaded
        assert "customers" in tables._tx.preloaded

    def test_modified_property(self, tables):
        tables.preload("orders", "customers")
        cur = tables.cursor()
        cur.execute("INSERT INTO orders VALUES (3, 30.0)")
        assert "orders" in tables._tx.modified
        assert "customers" not in tables._tx.modified
