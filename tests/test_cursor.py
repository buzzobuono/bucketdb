"""Cursor-level PEP 249 contract tests."""
import pytest

from s3ql.exceptions import InterfaceError, ProgrammingError


@pytest.fixture()
def cur(conn):
    conn.cursor().execute("CREATE TABLE t (id INTEGER, val VARCHAR)")
    return conn.cursor()


class TestCursorLifecycle:
    def test_close_prevents_execute(self, cur):
        cur.close()
        with pytest.raises(InterfaceError):
            cur.execute("SELECT 1")

    def test_fetchone_without_execute_raises(self, cur):
        with pytest.raises(ProgrammingError):
            cur.fetchone()

    def test_fetchall_without_execute_raises(self, cur):
        with pytest.raises(ProgrammingError):
            cur.fetchall()

    def test_context_manager_closes_cursor(self, conn):
        with conn.cursor() as c:
            c.execute("SELECT 1")
        with pytest.raises(InterfaceError):
            c.execute("SELECT 1")

    def test_arraysize_default(self, cur):
        assert cur.arraysize == 1

    def test_fetchmany_respects_arraysize(self, conn):
        cur = conn.cursor()
        for i in range(5):
            cur.execute(f"INSERT INTO t VALUES ({i}, 'v')")
        cur.execute("SELECT id FROM t ORDER BY id")
        cur.arraysize = 3
        batch = cur.fetchmany()
        assert len(batch) == 3


class TestCursorDescription:
    def test_description_none_before_execute(self, cur):
        assert cur.description is None

    def test_description_none_after_dml(self, cur):
        cur.execute("INSERT INTO t VALUES (1, 'a')")
        assert cur.description is None

    def test_description_set_after_select(self, cur):
        cur.execute("SELECT id, val FROM t LIMIT 0")
        assert cur.description is not None
        col_names = [d[0] for d in cur.description]
        assert col_names == ["id", "val"]


class TestParameterBinding:
    def test_qmark_params_select(self, conn):
        cur = conn.cursor()
        cur.execute("INSERT INTO t VALUES (1, 'hello')")
        cur.execute("SELECT val FROM t WHERE id = ?", [1])
        assert cur.fetchone()[0] == "hello"

    def test_executemany(self, conn):
        cur = conn.cursor()
        data = [(i, f"item{i}") for i in range(10)]
        cur.executemany("INSERT INTO t VALUES (?, ?)", data)
        cur.execute("SELECT COUNT(*) FROM t")
        assert cur.fetchone()[0] == 10
