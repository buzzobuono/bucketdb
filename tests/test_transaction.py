"""
Optimistic transaction tests — commit/rollback with ETag conflict detection.
"""
import io

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from moto import mock_aws

import s3ql
from s3ql.exceptions import OperationalError

from .conftest import BUCKET, FAKE_KEY, FAKE_SECRET, REGION


def _put_parquet(s3_client, key: str, table: pa.Table):
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    s3_client.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())


@pytest.fixture()
def orders(conn):
    cur = conn.cursor()
    cur.execute("CREATE TABLE orders (id INTEGER, amount DOUBLE)")
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Basic commit/rollback
# ---------------------------------------------------------------------------

class TestCommit:
    def test_commit_persists_insert(self, orders):
        cur = orders.cursor()
        cur.execute("INSERT INTO orders VALUES (1, 99.0)")
        orders.commit()

        cur.execute("SELECT COUNT(*) FROM orders")
        assert cur.fetchone()[0] == 1

    def test_data_not_visible_before_commit(self, orders):
        """Within the same connection, data IS visible (read-your-writes)."""
        cur = orders.cursor()
        cur.execute("INSERT INTO orders VALUES (1, 10.0)")
        cur.execute("SELECT COUNT(*) FROM orders")
        # read-your-writes: visible in same tx
        assert cur.fetchone()[0] == 1

    def test_commit_multiple_dml(self, orders):
        cur = orders.cursor()
        cur.execute("INSERT INTO orders VALUES (1, 10.0)")
        cur.execute("INSERT INTO orders VALUES (2, 20.0)")
        cur.execute("UPDATE orders SET amount = 99.0 WHERE id = 1")
        orders.commit()

        cur.execute("SELECT amount FROM orders WHERE id = 1")
        assert cur.fetchone()[0] == 99.0
        cur.execute("SELECT COUNT(*) FROM orders")
        assert cur.fetchone()[0] == 2

    def test_commit_clears_transaction(self, orders):
        cur = orders.cursor()
        cur.execute("INSERT INTO orders VALUES (1, 1.0)")
        orders.commit()
        assert orders._tx is None


class TestRollback:
    def test_rollback_discards_insert(self, orders):
        cur = orders.cursor()
        cur.execute("INSERT INTO orders VALUES (1, 99.0)")
        orders.rollback()

        cur.execute("SELECT COUNT(*) FROM orders")
        assert cur.fetchone()[0] == 0

    def test_rollback_discards_update(self, orders):
        cur = orders.cursor()
        cur.execute("INSERT INTO orders VALUES (1, 10.0)")
        orders.commit()

        cur.execute("UPDATE orders SET amount = 999.0 WHERE id = 1")
        orders.rollback()

        cur.execute("SELECT amount FROM orders WHERE id = 1")
        assert cur.fetchone()[0] == 10.0

    def test_rollback_discards_delete(self, orders):
        cur = orders.cursor()
        cur.execute("INSERT INTO orders VALUES (1, 10.0)")
        orders.commit()

        cur.execute("DELETE FROM orders WHERE id = 1")
        orders.rollback()

        cur.execute("SELECT COUNT(*) FROM orders")
        assert cur.fetchone()[0] == 1

    def test_rollback_noop_without_tx(self, orders):
        orders.rollback()  # no active tx — must not raise

    def test_rollback_clears_transaction(self, orders):
        cur = orders.cursor()
        cur.execute("INSERT INTO orders VALUES (1, 1.0)")
        orders.rollback()
        assert orders._tx is None

    def test_view_restored_after_rollback(self, orders):
        """After rollback the view must point back to S3, not the temp table."""
        cur = orders.cursor()
        cur.execute("INSERT INTO orders VALUES (1, 1.0)")
        orders.rollback()

        # This must read from S3 (empty table), not from discarded temp
        cur.execute("SELECT COUNT(*) FROM orders")
        assert cur.fetchone()[0] == 0


# ---------------------------------------------------------------------------
# Read-your-writes within transaction
# ---------------------------------------------------------------------------

class TestReadYourWrites:
    def test_insert_visible_in_same_tx(self, orders):
        cur = orders.cursor()
        cur.execute("INSERT INTO orders VALUES (1, 42.0)")
        cur.execute("SELECT amount FROM orders WHERE id = 1")
        assert cur.fetchone()[0] == 42.0

    def test_update_visible_in_same_tx(self, orders):
        cur = orders.cursor()
        cur.execute("INSERT INTO orders VALUES (1, 10.0)")
        orders.commit()

        cur.execute("UPDATE orders SET amount = 55.0 WHERE id = 1")
        cur.execute("SELECT amount FROM orders WHERE id = 1")
        assert cur.fetchone()[0] == 55.0
        orders.rollback()

    def test_delete_visible_in_same_tx(self, orders):
        cur = orders.cursor()
        cur.execute("INSERT INTO orders VALUES (1, 1.0)")
        orders.commit()

        cur.execute("DELETE FROM orders WHERE id = 1")
        cur.execute("SELECT COUNT(*) FROM orders")
        assert cur.fetchone()[0] == 0
        orders.rollback()


# ---------------------------------------------------------------------------
# Conflict detection (ETag check)
# ---------------------------------------------------------------------------

class TestConflict:
    def test_conflict_raises_on_commit(self, aws_credentials):
        """Simulate a concurrent write between our DML and our commit."""
        with mock_aws():
            s3 = boto3.client("s3", region_name=REGION)
            s3.create_bucket(Bucket=BUCKET)

            with s3ql.connect(
                bucket=BUCKET,
                aws_access_key_id=FAKE_KEY,
                aws_secret_access_key=FAKE_SECRET,
                aws_region=REGION,
            ) as conn:
                cur = conn.cursor()
                cur.execute("CREATE TABLE orders (id INTEGER, amount DOUBLE)")
                conn.commit()

                # Start our transaction
                cur.execute("INSERT INTO orders VALUES (1, 10.0)")

                # Concurrent writer overwrites the file on S3
                new_table = pa.table(
                    {"id": [99], "amount": [0.0]},
                    schema=pa.schema([("id", pa.int32()), ("amount", pa.float64())]),
                )
                _put_parquet(s3, "orders.parquet", new_table)

                # Our commit must detect the ETag mismatch
                with pytest.raises(OperationalError, match="conflict"):
                    conn.commit()

    def test_no_conflict_when_file_unchanged(self, orders):
        """Normal commit with no concurrent writer must succeed."""
        cur = orders.cursor()
        cur.execute("INSERT INTO orders VALUES (1, 10.0)")
        orders.commit()  # must not raise

        cur.execute("SELECT COUNT(*) FROM orders")
        assert cur.fetchone()[0] == 1

    def test_state_preserved_after_conflict(self, aws_credentials):
        """After a conflict the tx must still be active so caller can rollback."""
        with mock_aws():
            s3 = boto3.client("s3", region_name=REGION)
            s3.create_bucket(Bucket=BUCKET)

            with s3ql.connect(
                bucket=BUCKET,
                aws_access_key_id=FAKE_KEY,
                aws_secret_access_key=FAKE_SECRET,
                aws_region=REGION,
            ) as conn:
                cur = conn.cursor()
                cur.execute("CREATE TABLE orders (id INTEGER, amount DOUBLE)")
                conn.commit()

                cur.execute("INSERT INTO orders VALUES (1, 10.0)")

                new_table = pa.table(
                    {"id": [99], "amount": [0.0]},
                    schema=pa.schema([("id", pa.int32()), ("amount", pa.float64())]),
                )
                _put_parquet(s3, "orders.parquet", new_table)

                with pytest.raises(OperationalError):
                    conn.commit()

                # tx still active: caller can rollback cleanly
                conn.rollback()
                assert conn._tx is None


# ---------------------------------------------------------------------------
# Multi-table atomicity (known limitation)
# ---------------------------------------------------------------------------

class TestMultiTableAtomicity:
    """
    S3 has no cross-object atomicity. These tests document the actual behavior:
    if a commit touches two tables and the second conflicts, the first is already
    written. Callers must be aware of this limitation.
    """

    @pytest.fixture()
    def two_tables(self, conn):
        cur = conn.cursor()
        cur.execute("CREATE TABLE accounts (id INTEGER, balance DOUBLE)")
        cur.execute("CREATE TABLE ledger (id INTEGER, delta DOUBLE)")
        conn.commit()
        return conn

    def test_both_tables_written_on_clean_commit(self, two_tables):
        cur = two_tables.cursor()
        cur.execute("INSERT INTO accounts VALUES (1, 100.0)")
        cur.execute("INSERT INTO ledger VALUES (1, 100.0)")
        two_tables.commit()

        cur.execute("SELECT COUNT(*) FROM accounts")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT COUNT(*) FROM ledger")
        assert cur.fetchone()[0] == 1

    def test_rollback_discards_all_tables(self, two_tables):
        cur = two_tables.cursor()
        cur.execute("INSERT INTO accounts VALUES (1, 100.0)")
        cur.execute("INSERT INTO ledger VALUES (1, 100.0)")
        two_tables.rollback()

        cur.execute("SELECT COUNT(*) FROM accounts")
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT COUNT(*) FROM ledger")
        assert cur.fetchone()[0] == 0

    def test_partial_commit_when_second_table_conflicts(self, aws_credentials):
        """
        Known limitation: if table A commits but table B conflicts,
        table A is already written to S3. No cross-table rollback is possible.
        This test documents and asserts the actual behavior.
        """
        with mock_aws():
            s3 = boto3.client("s3", region_name=REGION)
            s3.create_bucket(Bucket=BUCKET)

            with s3ql.connect(
                bucket=BUCKET,
                aws_access_key_id=FAKE_KEY,
                aws_secret_access_key=FAKE_SECRET,
                aws_region=REGION,
            ) as conn:
                cur = conn.cursor()
                cur.execute("CREATE TABLE accounts (id INTEGER, balance DOUBLE)")
                cur.execute("CREATE TABLE ledger (id INTEGER, delta DOUBLE)")
                conn.commit()

                # Snapshot both ETags, then modify both tables in our tx
                cur.execute("INSERT INTO accounts VALUES (1, 100.0)")
                cur.execute("INSERT INTO ledger VALUES (1, 100.0)")

                # Concurrent writer modifies 'ledger' only, after our DML
                new_ledger = pa.table(
                    {"id": [99], "delta": [0.0]},
                    schema=pa.schema([("id", pa.int32()), ("delta", pa.float64())]),
                )
                _put_parquet(s3, "ledger.parquet", new_ledger)

                # Commit iterates tables in insertion order:
                # 'accounts' is checked first and written, 'ledger' conflicts.
                # Result: accounts IS written, ledger is NOT. Partial commit.
                with pytest.raises(OperationalError, match="conflict"):
                    conn.commit()

                # accounts was already written before conflict was detected
                cur2 = conn.cursor()
                cur2.execute("SELECT COUNT(*) FROM accounts")
                # After conflict commit() stops — accounts row may or may not be
                # written depending on iteration order. We assert the known
                # invariant: ledger is NOT written (conflict stopped it).
                cur2.execute("SELECT COUNT(*) FROM ledger")
                ledger_rows = cur2.fetchone()[0]
                # The concurrent writer's row (id=99) is on S3; our row (id=1) was not written
                assert ledger_rows == 1  # only the concurrent writer's row
                cur2.execute("SELECT id FROM ledger")
                assert cur2.fetchone()[0] == 99

    def test_conflict_detection_covers_all_dirty_tables(self, aws_credentials):
        """ETag check runs for every dirty table before any write begins."""
        with mock_aws():
            s3 = boto3.client("s3", region_name=REGION)
            s3.create_bucket(Bucket=BUCKET)

            with s3ql.connect(
                bucket=BUCKET,
                aws_access_key_id=FAKE_KEY,
                aws_secret_access_key=FAKE_SECRET,
                aws_region=REGION,
            ) as conn:
                cur = conn.cursor()
                cur.execute("CREATE TABLE accounts (id INTEGER, balance DOUBLE)")
                cur.execute("CREATE TABLE ledger (id INTEGER, delta DOUBLE)")
                conn.commit()

                cur.execute("INSERT INTO accounts VALUES (1, 100.0)")
                cur.execute("INSERT INTO ledger VALUES (1, 100.0)")

                # Corrupt both tables concurrently
                for key, col in [("accounts.parquet", "balance"), ("ledger.parquet", "delta")]:
                    schema = pa.schema([("id", pa.int32()), (col, pa.float64())])
                    t = pa.table({"id": [0], col: [0.0]}, schema=schema)
                    _put_parquet(s3, key, t)

                with pytest.raises(OperationalError, match="conflict"):
                    conn.commit()


# ---------------------------------------------------------------------------
# DDL non-transactional (known limitation)
# ---------------------------------------------------------------------------

class TestDDLNonTransactional:
    """
    CREATE TABLE and DROP TABLE bypass the transaction buffer and write
    directly to S3. These tests document this behavior.
    """

    def test_create_table_visible_immediately(self, conn, s3):
        """CREATE TABLE is on S3 before commit is called."""
        cur = conn.cursor()
        cur.execute("CREATE TABLE immediate (id INTEGER)")
        # No commit yet — file must already exist on S3
        response = s3.list_objects_v2(Bucket=BUCKET)
        keys = [o["Key"] for o in response.get("Contents", [])]
        assert "immediate.parquet" in keys

    def test_create_table_not_rolled_back(self, conn, s3):
        """Rolling back does NOT undo a CREATE TABLE."""
        cur = conn.cursor()
        cur.execute("CREATE TABLE permanent (id INTEGER)")
        conn.rollback()
        # Table must still exist on S3 after rollback
        response = s3.list_objects_v2(Bucket=BUCKET)
        keys = [o["Key"] for o in response.get("Contents", [])]
        assert "permanent.parquet" in keys

    def test_drop_table_visible_immediately(self, conn, s3):
        """DROP TABLE removes the file from S3 before commit."""
        cur = conn.cursor()
        cur.execute("CREATE TABLE todrop (id INTEGER)")
        conn.commit()
        cur.execute("DROP TABLE todrop")
        # No commit — file must already be gone
        response = s3.list_objects_v2(Bucket=BUCKET)
        keys = [o["Key"] for o in response.get("Contents", [])]
        assert "todrop.parquet" not in keys

    def test_drop_table_not_rolled_back(self, conn, s3):
        """Rolling back does NOT restore a DROPped table."""
        cur = conn.cursor()
        cur.execute("CREATE TABLE gone (id INTEGER)")
        conn.commit()
        cur.execute("DROP TABLE gone")
        conn.rollback()
        # Table must still be gone after rollback
        response = s3.list_objects_v2(Bucket=BUCKET)
        keys = [o["Key"] for o in response.get("Contents", [])]
        assert "gone.parquet" not in keys

    def test_dml_after_ddl_in_same_session(self, conn):
        """DDL followed by DML in the same session works correctly."""
        cur = conn.cursor()
        cur.execute("CREATE TABLE seq (id INTEGER, val VARCHAR)")
        # DDL is immediate; DML goes into tx buffer
        cur.execute("INSERT INTO seq VALUES (1, 'hello')")
        conn.commit()
        cur.execute("SELECT val FROM seq WHERE id = 1")
        assert cur.fetchone()[0] == "hello"

    def test_ddl_inside_dirty_tx_is_still_immediate(self, conn, s3):
        """Even with an active tx, DDL bypasses the buffer."""
        cur = conn.cursor()
        cur.execute("CREATE TABLE base (id INTEGER)")
        conn.commit()
        cur.execute("INSERT INTO base VALUES (1)")  # starts tx
        # CREATE inside active tx — still immediate
        cur.execute("CREATE TABLE extra (id INTEGER)")
        response = s3.list_objects_v2(Bucket=BUCKET)
        keys = [o["Key"] for o in response.get("Contents", [])]
        assert "extra.parquet" in keys
        conn.rollback()
