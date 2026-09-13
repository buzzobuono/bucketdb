"""
PEP 249 compliance tests.
Reference: https://peps.python.org/pep-0249/
"""
import datetime
import time as time_module

import pytest

import bucketdb
from bucketdb.exceptions import NotSupportedError, ProgrammingError, InterfaceError

from .conftest import BUCKET, FAKE_KEY, FAKE_SECRET, REGION


# ---------------------------------------------------------------------------
# 1. Module-level attributes
# ---------------------------------------------------------------------------

class TestModuleAttributes:
    def test_apilevel(self):
        assert s3ql.apilevel == "2.0"

    def test_threadsafety(self):
        assert s3ql.threadsafety in (0, 1, 2, 3)

    def test_paramstyle(self):
        assert s3ql.paramstyle in ("qmark", "numeric", "named", "format", "pyformat")

    def test_connect_callable(self):
        assert callable(bucketdb.connect)

    def test_module_exports_type_objects(self):
        for name in ("STRING", "BINARY", "NUMBER", "DATETIME", "ROWID"):
            assert hasattr(s3ql, name), f"s3ql.{name} missing"

    def test_module_exports_type_constructors(self):
        for name in ("Date", "Time", "Timestamp", "Binary",
                     "DateFromTicks", "TimeFromTicks", "TimestampFromTicks"):
            assert hasattr(s3ql, name), f"s3ql.{name} missing"

    def test_module_exports_all_exceptions(self):
        for name in (
            "Warning", "Error", "InterfaceError", "DatabaseError",
            "DataError", "OperationalError", "IntegrityError",
            "InternalError", "ProgrammingError", "NotSupportedError",
        ):
            assert hasattr(s3ql, name), f"s3ql.{name} missing"

    def test_exception_hierarchy(self):
        assert issubclass(bucketdb.Warning, Exception)
        assert issubclass(bucketdb.Error, Exception)
        assert issubclass(bucketdb.InterfaceError, bucketdb.Error)
        assert issubclass(bucketdb.DatabaseError, bucketdb.Error)
        assert issubclass(bucketdb.DataError, bucketdb.DatabaseError)
        assert issubclass(bucketdb.OperationalError, bucketdb.DatabaseError)
        assert issubclass(bucketdb.IntegrityError, bucketdb.DatabaseError)
        assert issubclass(bucketdb.InternalError, bucketdb.DatabaseError)
        assert issubclass(bucketdb.ProgrammingError, bucketdb.DatabaseError)
        assert issubclass(bucketdb.NotSupportedError, bucketdb.DatabaseError)


# ---------------------------------------------------------------------------
# 2. Connection object
# ---------------------------------------------------------------------------

class TestConnectionObject:
    def test_has_close(self, conn):
        assert callable(conn.close)

    def test_has_commit(self, conn):
        assert callable(conn.commit)

    def test_has_rollback(self, conn):
        assert callable(conn.rollback)

    def test_has_cursor(self, conn):
        assert callable(conn.cursor)

    def test_commit_no_raise(self, conn):
        conn.commit()

    def test_rollback_no_raise(self, conn):
        conn.rollback()  # now a real no-op when no tx is active

    def test_cursor_returns_cursor(self, conn):
        cur = conn.cursor()
        assert cur is not None

    def test_close_makes_connection_unusable(self, s3, moto_server):
        c = bucketdb.connect(
            bucket=BUCKET,
            aws_access_key_id=FAKE_KEY,
            aws_secret_access_key=FAKE_SECRET,
            aws_region=REGION,
            endpoint_url=moto_server,
        )
        c.close()
        with pytest.raises(InterfaceError):
            c.cursor()

    def test_exceptions_on_connection(self, conn):
        """PEP 249: exceptions must be accessible as attributes of the connection."""
        for name in (
            "Warning", "Error", "InterfaceError", "DatabaseError",
            "DataError", "OperationalError", "IntegrityError",
            "InternalError", "ProgrammingError", "NotSupportedError",
        ):
            assert hasattr(conn, name), f"conn.{name} missing"

    def test_connection_exceptions_are_same_classes(self, conn):
        assert conn.ProgrammingError is bucketdb.ProgrammingError
        assert conn.NotSupportedError is bucketdb.NotSupportedError


# ---------------------------------------------------------------------------
# 3. Cursor object — attributes
# ---------------------------------------------------------------------------

class TestCursorAttributes:
    def test_has_description(self, conn):
        cur = conn.cursor()
        assert hasattr(cur, "description")

    def test_description_initially_none(self, conn):
        cur = conn.cursor()
        assert cur.description is None

    def test_has_rowcount(self, conn):
        cur = conn.cursor()
        assert hasattr(cur, "rowcount")

    def test_rowcount_initially_minus_one(self, conn):
        cur = conn.cursor()
        assert cur.rowcount == -1

    def test_has_arraysize(self, conn):
        cur = conn.cursor()
        assert hasattr(cur, "arraysize")

    def test_arraysize_default_one(self, conn):
        cur = conn.cursor()
        assert cur.arraysize == 1

    def test_arraysize_writable(self, conn):
        cur = conn.cursor()
        cur.arraysize = 10
        assert cur.arraysize == 10


# ---------------------------------------------------------------------------
# 4. Cursor object — methods
# ---------------------------------------------------------------------------

class TestCursorMethods:
    def test_has_execute(self, conn):
        assert callable(conn.cursor().execute)

    def test_has_executemany(self, conn):
        assert callable(conn.cursor().executemany)

    def test_has_fetchone(self, conn):
        assert callable(conn.cursor().fetchone)

    def test_has_fetchmany(self, conn):
        assert callable(conn.cursor().fetchmany)

    def test_has_fetchall(self, conn):
        assert callable(conn.cursor().fetchall)

    def test_has_close(self, conn):
        assert callable(conn.cursor().close)

    def test_has_setinputsizes(self, conn):
        assert callable(conn.cursor().setinputsizes)

    def test_has_setoutputsize(self, conn):
        assert callable(conn.cursor().setoutputsize)

    def test_setinputsizes_no_raise(self, conn):
        conn.cursor().setinputsizes([10, 20])

    def test_setoutputsize_no_raise(self, conn):
        conn.cursor().setoutputsize(1000)

    def test_setoutputsize_with_column_no_raise(self, conn):
        conn.cursor().setoutputsize(1000, 0)


# ---------------------------------------------------------------------------
# 5. description — 7-item tuple format
# ---------------------------------------------------------------------------

class TestDescription:
    @pytest.fixture(autouse=True)
    def table(self, conn):
        cur = conn.cursor()
        cur.execute("CREATE TABLE desc_test (id INTEGER, name VARCHAR, score DOUBLE)")
        cur.execute("INSERT INTO desc_test VALUES (1, 'alice', 9.5)")

    def test_description_is_sequence_after_select(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT * FROM desc_test")
        assert cur.description is not None
        assert len(cur.description) == 3

    def test_description_each_col_has_7_items(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT id, name, score FROM desc_test")
        for col in cur.description:
            assert len(col) == 7, f"Column descriptor has {len(col)} items, expected 7"

    def test_description_first_item_is_name(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT id, name, score FROM desc_test")
        names = [col[0] for col in cur.description]
        assert names == ["id", "name", "score"]

    def test_description_none_after_dml(self, conn):
        cur = conn.cursor()
        cur.execute("INSERT INTO desc_test VALUES (2, 'bob', 8.0)")
        assert cur.description is None

    def test_description_none_before_execute(self, conn):
        cur = conn.cursor()
        assert cur.description is None


# ---------------------------------------------------------------------------
# 6. rowcount
# ---------------------------------------------------------------------------

class TestRowcount:
    @pytest.fixture(autouse=True)
    def table(self, conn):
        cur = conn.cursor()
        cur.execute("CREATE TABLE rc_test (id INTEGER, val VARCHAR)")

    def test_rowcount_minus_one_before_execute(self, conn):
        cur = conn.cursor()
        assert cur.rowcount == -1

    def test_rowcount_after_select(self, conn):
        cur = conn.cursor()
        cur.execute("INSERT INTO rc_test VALUES (1, 'a')")
        cur.execute("INSERT INTO rc_test VALUES (2, 'b')")
        cur.execute("SELECT * FROM rc_test")
        assert cur.rowcount == 2

    def test_rowcount_after_insert(self, conn):
        cur = conn.cursor()
        cur.execute("INSERT INTO rc_test VALUES (1, 'x')")
        assert cur.rowcount >= 0

    def test_rowcount_minus_one_after_failed_execute(self, conn):
        cur = conn.cursor()
        with pytest.raises(ProgrammingError):
            cur.execute("SELECT * FROM nonexistent_table_xyz")
        assert cur.rowcount == -1


# ---------------------------------------------------------------------------
# 7. fetch methods contract
# ---------------------------------------------------------------------------

class TestFetchContract:
    @pytest.fixture(autouse=True)
    def rows(self, conn):
        cur = conn.cursor()
        cur.execute("CREATE TABLE fetch_test (id INTEGER)")
        for i in range(5):
            cur.execute(f"INSERT INTO fetch_test VALUES ({i})")

    def test_fetchone_returns_single_tuple(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT id FROM fetch_test ORDER BY id")
        row = cur.fetchone()
        assert isinstance(row, tuple)
        assert len(row) == 1

    def test_fetchone_exhaustion_returns_none(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT id FROM fetch_test ORDER BY id")
        for _ in range(5):
            cur.fetchone()
        assert cur.fetchone() is None

    def test_fetchmany_default_size(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT id FROM fetch_test ORDER BY id")
        batch = cur.fetchmany()
        assert len(batch) == cur.arraysize  # default arraysize=1

    def test_fetchmany_custom_size(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT id FROM fetch_test ORDER BY id")
        batch = cur.fetchmany(3)
        assert len(batch) == 3

    def test_fetchmany_respects_arraysize(self, conn):
        cur = conn.cursor()
        cur.arraysize = 4
        cur.execute("SELECT id FROM fetch_test ORDER BY id")
        batch = cur.fetchmany()
        assert len(batch) == 4

    def test_fetchmany_at_end_returns_empty(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT id FROM fetch_test ORDER BY id")
        cur.fetchall()
        assert cur.fetchmany(10) == []

    def test_fetchall_returns_all_rows(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT id FROM fetch_test ORDER BY id")
        rows = cur.fetchall()
        assert len(rows) == 5

    def test_fetchall_after_fetchone(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT id FROM fetch_test ORDER BY id")
        cur.fetchone()
        rows = cur.fetchall()
        assert len(rows) == 4

    def test_fetch_without_execute_raises(self, conn):
        cur = conn.cursor()
        with pytest.raises(ProgrammingError):
            cur.fetchone()
        with pytest.raises(ProgrammingError):
            cur.fetchmany()
        with pytest.raises(ProgrammingError):
            cur.fetchall()


# ---------------------------------------------------------------------------
# 8. Cursor closed state
# ---------------------------------------------------------------------------

class TestCursorClosed:
    def test_execute_on_closed_cursor_raises(self, conn):
        cur = conn.cursor()
        cur.close()
        with pytest.raises(InterfaceError):
            cur.execute("SELECT 1")

    def test_fetch_on_closed_cursor_raises(self, conn):
        cur = conn.cursor()
        cur.close()
        with pytest.raises(InterfaceError):
            cur.fetchone()

    def test_close_idempotent(self, conn):
        cur = conn.cursor()
        cur.close()
        cur.close()  # must not raise


# ---------------------------------------------------------------------------
# 9. Parameter binding (paramstyle = qmark)
# ---------------------------------------------------------------------------

class TestParameterBinding:
    @pytest.fixture(autouse=True)
    def table(self, conn):
        cur = conn.cursor()
        cur.execute("CREATE TABLE param_test (id INTEGER, name VARCHAR)")

    def test_execute_with_qmark_params(self, conn):
        cur = conn.cursor()
        cur.execute("INSERT INTO param_test VALUES (?, ?)", [42, "test"])
        cur.execute("SELECT name FROM param_test WHERE id = ?", [42])
        assert cur.fetchone()[0] == "test"

    def test_executemany_inserts_all(self, conn):
        cur = conn.cursor()
        data = [(i, f"user{i}") for i in range(10)]
        cur.executemany("INSERT INTO param_test VALUES (?, ?)", data)
        cur.execute("SELECT COUNT(*) FROM param_test")
        assert cur.fetchone()[0] == 10


# ---------------------------------------------------------------------------
# 10. Type objects
# ---------------------------------------------------------------------------

class TestTypeObjects:
    def test_string_is_str(self):
        assert bucketdb.STRING is str

    def test_binary_is_bytes(self):
        assert bucketdb.BINARY is bytes

    def test_number_is_decimal(self):
        from decimal import Decimal
        assert bucketdb.NUMBER is Decimal

    def test_datetime_is_datetime(self):
        assert bucketdb.DATETIME is datetime.datetime

    def test_rowid_is_int(self):
        assert bucketdb.ROWID is int


# ---------------------------------------------------------------------------
# 11. Type constructors
# ---------------------------------------------------------------------------

class TestTypeConstructors:
    def test_date_constructor(self):
        d = bucketdb.Date(2024, 1, 15)
        assert isinstance(d, datetime.date)
        assert d.year == 2024
        assert d.month == 1
        assert d.day == 15

    def test_time_constructor(self):
        t = bucketdb.Time(10, 30, 0)
        assert isinstance(t, datetime.time)
        assert t.hour == 10
        assert t.minute == 30

    def test_timestamp_constructor(self):
        ts = bucketdb.Timestamp(2024, 6, 1, 12, 0, 0)
        assert isinstance(ts, datetime.datetime)
        assert ts.year == 2024
        assert ts.hour == 12

    def test_binary_constructor(self):
        b = bucketdb.Binary(b"\x00\xff")
        assert isinstance(b, bytes)
        assert b == b"\x00\xff"

    def test_date_from_ticks(self):
        ticks = datetime.datetime(2024, 3, 15).timestamp()
        d = bucketdb.DateFromTicks(ticks)
        assert isinstance(d, datetime.date)
        assert d.year == 2024
        assert d.month == 3
        assert d.day == 15

    def test_time_from_ticks(self):
        ticks = datetime.datetime(2024, 1, 1, 8, 30, 0).timestamp()
        t = bucketdb.TimeFromTicks(ticks)
        assert isinstance(t, datetime.time)
        assert t.hour == 8
        assert t.minute == 30

    def test_timestamp_from_ticks(self):
        ref = datetime.datetime(2024, 6, 1, 12, 0, 0)
        ts = bucketdb.TimestampFromTicks(ref.timestamp())
        assert isinstance(ts, datetime.datetime)
        assert ts.year == 2024
        assert ts.month == 6
        assert ts.day == 1
