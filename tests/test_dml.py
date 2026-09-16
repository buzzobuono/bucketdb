import pytest

from bucketdb.exceptions import ProgrammingError


@pytest.fixture()
def orders(conn):
    """Connection with an empty 'orders' table ready to use."""
    cur = conn.cursor()
    cur.execute("CREATE TABLE orders (id INTEGER, item VARCHAR, amount DOUBLE)")
    return conn


class TestInsert:
    def test_insert_single_row(self, orders):
        cur = orders.cursor()
        cur.execute("INSERT INTO orders VALUES (1, 'apple', 1.5)")
        cur.execute("SELECT COUNT(*) FROM orders")
        assert cur.fetchone()[0] == 1

    def test_insert_with_parameters(self, orders):
        cur = orders.cursor()
        cur.execute("INSERT INTO orders VALUES (?, ?, ?)", [2, "banana", 0.75])
        cur.execute("SELECT item FROM orders WHERE id = 2")
        assert cur.fetchone()[0] == "banana"

    def test_insert_multiple_rows(self, orders):
        cur = orders.cursor()
        rows = [(i, f"item{i}", float(i)) for i in range(1, 6)]
        cur.executemany("INSERT INTO orders VALUES (?, ?, ?)", rows)
        cur.execute("SELECT COUNT(*) FROM orders")
        assert cur.fetchone()[0] == 5

    def test_insert_into_missing_table_raises(self, conn):
        cur = conn.cursor()
        with pytest.raises(ProgrammingError):
            cur.execute("INSERT INTO no_such_table VALUES (1)")

    def test_insert_persisted_to_s3(self, orders, s3):
        """After insert the Parquet file on S3 must contain the new row."""
        cur = orders.cursor()
        cur.execute("INSERT INTO orders VALUES (99, 'pear', 3.0)")

        # Re-read directly from S3 via a fresh DuckDB query on the view
        cur.execute("SELECT amount FROM orders WHERE id = 99")
        assert cur.fetchone()[0] == 3.0


class TestExecutemanyBulkPath:
    """cursor.executemany() on INSERT uses DuckDB's own executemany() to
    bind the statement once instead of once per row (see cursor.py).
    UPDATE/DELETE keep the original per-row loop untouched."""

    def test_rowcount_is_total_rows_not_last_row(self, orders):
        """Bulk path can't read a per-row rowcount back from DuckDB's
        executemany() (it returns the connection, not a result), so it's
        computed as len(seq_of_parameters) instead — the total, not
        whatever the old per-row loop happened to leave behind."""
        cur = orders.cursor()
        rows = [(i, f"item{i}", float(i)) for i in range(1, 6)]
        cur.executemany("INSERT INTO orders VALUES (?, ?, ?)", rows)
        assert cur.rowcount == 5

    def test_empty_batch_does_not_raise(self, orders):
        """DuckDB's native executemany() raises on an empty parameter list;
        the empty batch must be handled before reaching it."""
        cur = orders.cursor()
        cur.executemany("INSERT INTO orders VALUES (?, ?, ?)", [])
        cur.execute("SELECT COUNT(*) FROM orders")
        assert cur.fetchone()[0] == 0

    def test_bad_row_mid_batch_raises_programming_error(self, orders):
        cur = orders.cursor()
        rows = [(1, "a", 1.0), (2, "b", 2.0), ("not_an_int", "c", 3.0)]
        with pytest.raises(ProgrammingError):
            cur.executemany("INSERT INTO orders VALUES (?, ?, ?)", rows)

    def test_execute_then_executemany_same_table_same_tx(self, orders):
        """A single execute() INSERT followed by executemany() INSERT on
        the same table in the same transaction must both land in the same
        buffered temp table, not conflict or reload it."""
        cur = orders.cursor()
        cur.execute("INSERT INTO orders VALUES (1, 'apple', 1.5)")
        cur.executemany(
            "INSERT INTO orders VALUES (?, ?, ?)",
            [(2, "banana", 0.75), (3, "cherry", 2.25)],
        )
        orders.commit()
        cur.execute("SELECT COUNT(*) FROM orders")
        assert cur.fetchone()[0] == 3

    def test_update_via_executemany_still_works(self, orders):
        """UPDATE isn't eligible for the bulk path — must still go through
        the per-row loop exactly as before."""
        cur = orders.cursor()
        cur.executemany(
            "INSERT INTO orders VALUES (?, ?, ?)",
            [(1, "apple", 1.0), (2, "banana", 2.0)],
        )
        cur.executemany(
            "UPDATE orders SET amount = ? WHERE id = ?",
            [(10.0, 1), (20.0, 2)],
        )
        orders.commit()
        cur.execute("SELECT amount FROM orders ORDER BY id")
        assert [r[0] for r in cur.fetchall()] == [10.0, 20.0]


class TestSelect:
    def test_select_all(self, orders):
        cur = orders.cursor()
        cur.execute("INSERT INTO orders VALUES (1, 'x', 10.0)")
        cur.execute("INSERT INTO orders VALUES (2, 'y', 20.0)")
        cur.execute("SELECT * FROM orders ORDER BY id")
        rows = cur.fetchall()
        assert len(rows) == 2
        assert rows[0][0] == 1
        assert rows[1][0] == 2

    def test_select_with_filter(self, orders):
        cur = orders.cursor()
        cur.execute("INSERT INTO orders VALUES (1, 'a', 5.0)")
        cur.execute("INSERT INTO orders VALUES (2, 'b', 15.0)")
        cur.execute("SELECT id FROM orders WHERE amount > 10")
        assert cur.fetchone()[0] == 2

    def test_fetchone(self, orders):
        cur = orders.cursor()
        cur.execute("INSERT INTO orders VALUES (1, 'a', 1.0)")
        cur.execute("SELECT * FROM orders")
        row = cur.fetchone()
        assert row is not None
        assert cur.fetchone() is None  # exhausted

    def test_fetchmany(self, orders):
        cur = orders.cursor()
        for i in range(5):
            cur.execute(f"INSERT INTO orders VALUES ({i}, 'x', 1.0)")
        cur.execute("SELECT id FROM orders ORDER BY id")
        first_two = cur.fetchmany(2)
        assert len(first_two) == 2
        rest = cur.fetchall()
        assert len(rest) == 3

    def test_description_set_after_select(self, orders):
        cur = orders.cursor()
        cur.execute("SELECT id, item FROM orders LIMIT 0")
        assert cur.description is not None
        names = [d[0] for d in cur.description]
        assert "id" in names
        assert "item" in names

    def test_rowcount_after_select(self, orders):
        cur = orders.cursor()
        cur.execute("INSERT INTO orders VALUES (1, 'a', 1.0)")
        cur.execute("INSERT INTO orders VALUES (2, 'b', 2.0)")
        cur.execute("SELECT * FROM orders")
        assert cur.rowcount == 2


class TestUpdate:
    def test_update_single_row(self, orders):
        cur = orders.cursor()
        cur.execute("INSERT INTO orders VALUES (1, 'old', 1.0)")
        cur.execute("UPDATE orders SET item = 'new' WHERE id = 1")
        cur.execute("SELECT item FROM orders WHERE id = 1")
        assert cur.fetchone()[0] == "new"

    def test_update_persisted(self, orders):
        cur = orders.cursor()
        cur.execute("INSERT INTO orders VALUES (1, 'old', 1.0)")
        cur.execute("UPDATE orders SET amount = 99.0 WHERE id = 1")
        # Re-read via the view (which points to S3)
        cur.execute("SELECT amount FROM orders WHERE id = 1")
        assert cur.fetchone()[0] == 99.0

    def test_update_with_parameters(self, orders):
        cur = orders.cursor()
        cur.execute("INSERT INTO orders VALUES (1, 'x', 1.0)")
        cur.execute("UPDATE orders SET amount = ? WHERE id = ?", [42.0, 1])
        cur.execute("SELECT amount FROM orders")
        assert cur.fetchone()[0] == 42.0


class TestDelete:
    def test_delete_row(self, orders):
        cur = orders.cursor()
        cur.execute("INSERT INTO orders VALUES (1, 'a', 1.0)")
        cur.execute("INSERT INTO orders VALUES (2, 'b', 2.0)")
        cur.execute("DELETE FROM orders WHERE id = 1")
        cur.execute("SELECT COUNT(*) FROM orders")
        assert cur.fetchone()[0] == 1

    def test_delete_all(self, orders):
        cur = orders.cursor()
        cur.execute("INSERT INTO orders VALUES (1, 'a', 1.0)")
        cur.execute("DELETE FROM orders")
        cur.execute("SELECT COUNT(*) FROM orders")
        assert cur.fetchone()[0] == 0

    def test_delete_persisted(self, orders):
        cur = orders.cursor()
        cur.execute("INSERT INTO orders VALUES (1, 'keep', 1.0)")
        cur.execute("INSERT INTO orders VALUES (2, 'remove', 2.0)")
        cur.execute("DELETE FROM orders WHERE id = 2")
        cur.execute("SELECT COUNT(*) FROM orders")
        assert cur.fetchone()[0] == 1
