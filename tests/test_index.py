"""Tests for CREATE INDEX (sort key) and CREATE INDEX ... PARTITIONED."""
import pytest
import s3ql
from tests.conftest import BUCKET, REGION, FAKE_KEY, FAKE_SECRET


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def fresh_conn(moto_server, s3):
    return s3ql.connect(
        bucket=BUCKET,
        aws_access_key_id=FAKE_KEY,
        aws_secret_access_key=FAKE_SECRET,
        aws_region=REGION,
        endpoint_url=moto_server,
    )


# ---------------------------------------------------------------------------
# Sort-key index (non-partitioned)
# ---------------------------------------------------------------------------

class TestSortKeyIndex:
    def test_create_index_basic(self, conn):
        cur = conn.cursor()
        cur.execute("CREATE TABLE products (id INTEGER, name VARCHAR, price FLOAT)")
        cur.execute("CREATE INDEX idx_price ON products (price)")
        # Index is registered in the index store
        idef = conn.index_store.get_for_table("products")
        assert idef is not None
        assert idef.columns == ["price"]
        assert idef.partitioned is False

    def test_create_index_multi_column(self, conn):
        cur = conn.cursor()
        cur.execute("CREATE TABLE sales (region VARCHAR, date VARCHAR, amount FLOAT)")
        cur.execute("CREATE INDEX idx_region_date ON sales (region, date)")
        idef = conn.index_store.get_for_table("sales")
        assert idef.columns == ["region", "date"]

    def test_sort_applied_on_flush(self, conn):
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (val INTEGER)")
        cur.execute("CREATE INDEX idx_val ON t (val)")
        cur.execute("INSERT INTO t VALUES (30)")
        cur.execute("INSERT INTO t VALUES (10)")
        cur.execute("INSERT INTO t VALUES (20)")
        conn.commit()

        cur.execute("SELECT val FROM t")
        rows = [r[0] for r in cur.fetchall()]
        assert rows == sorted(rows)

    def test_sort_on_existing_data(self, conn):
        cur = conn.cursor()
        cur.execute("CREATE TABLE nums (n INTEGER)")
        cur.execute("INSERT INTO nums VALUES (5)")
        cur.execute("INSERT INTO nums VALUES (1)")
        cur.execute("INSERT INTO nums VALUES (3)")
        conn.commit()

        # Create index on existing data — should re-sort immediately
        cur.execute("CREATE INDEX idx_n ON nums (n)")
        conn.close()

    def test_drop_index(self, conn):
        cur = conn.cursor()
        cur.execute("CREATE TABLE t2 (x INTEGER)")
        cur.execute("CREATE INDEX idx_x ON t2 (x)")
        assert conn.index_store.get_by_name("idx_x") is not None
        cur.execute("DROP INDEX idx_x")
        assert conn.index_store.get_by_name("idx_x") is None
        assert conn.index_store.get_for_table("t2") is None

    def test_create_index_if_not_exists(self, conn):
        cur = conn.cursor()
        cur.execute("CREATE TABLE t3 (x INTEGER)")
        cur.execute("CREATE INDEX idx_x3 ON t3 (x)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_x3 ON t3 (x)")  # should not raise

    def test_drop_index_if_exists(self, conn):
        cur = conn.cursor()
        cur.execute("DROP INDEX IF EXISTS nonexistent")  # should not raise

    def test_index_persists_across_connections(self, s3, moto_server):
        with fresh_conn(moto_server, s3) as c1:
            cur = c1.cursor()
            cur.execute("CREATE TABLE persist_t (n INTEGER)")
            cur.execute("CREATE INDEX idx_persist ON persist_t (n)")

        with fresh_conn(moto_server, s3) as c2:
            idef = c2.index_store.get_for_table("persist_t")
            assert idef is not None
            assert idef.columns == ["n"]
            assert idef.partitioned is False

    def test_index_removed_on_drop_table(self, conn):
        cur = conn.cursor()
        cur.execute("CREATE TABLE tdrop (x INTEGER)")
        cur.execute("CREATE INDEX idx_tdrop ON tdrop (x)")
        cur.execute("DROP TABLE tdrop")
        assert conn.index_store.get_for_table("tdrop") is None


# ---------------------------------------------------------------------------
# Partitioned index
# ---------------------------------------------------------------------------

class TestPartitionedIndex:
    def test_create_partitioned_index(self, conn):
        cur = conn.cursor()
        cur.execute("CREATE TABLE orders (region VARCHAR, amount FLOAT)")
        cur.execute("CREATE INDEX idx_region ON orders (region) PARTITIONED")
        idef = conn.index_store.get_for_table("orders")
        assert idef is not None
        assert idef.partitioned is True
        assert idef.columns == ["region"]

    def test_insert_and_query_partitioned(self, conn):
        cur = conn.cursor()
        cur.execute("CREATE TABLE sales (region VARCHAR, amount FLOAT)")
        cur.execute("CREATE INDEX idx_sales_region ON sales (region) PARTITIONED")
        cur.execute("INSERT INTO sales VALUES ('IT', 100.0)")
        cur.execute("INSERT INTO sales VALUES ('DE', 200.0)")
        cur.execute("INSERT INTO sales VALUES ('IT', 150.0)")
        conn.commit()

        cur.execute("SELECT amount FROM sales WHERE region='IT' ORDER BY amount")
        rows = [r[0] for r in cur.fetchall()]
        assert rows == [100.0, 150.0]

        cur.execute("SELECT COUNT(*) FROM sales")
        assert cur.fetchone()[0] == 3

    def test_partition_files_created_on_s3(self, conn, s3):
        cur = conn.cursor()
        cur.execute("CREATE TABLE evts (country VARCHAR, v INTEGER)")
        cur.execute("CREATE INDEX idx_evts ON evts (country) PARTITIONED")
        cur.execute("INSERT INTO evts VALUES ('IT', 1)")
        cur.execute("INSERT INTO evts VALUES ('DE', 2)")
        conn.commit()

        objects = s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])
        keys = [o["Key"] for o in objects]
        assert any("country=IT" in k for k in keys)
        assert any("country=DE" in k for k in keys)

    def test_partitioned_multi_column(self, conn):
        cur = conn.cursor()
        cur.execute("CREATE TABLE stats (region VARCHAR, year INTEGER, revenue FLOAT)")
        cur.execute("CREATE INDEX idx_stats ON stats (region, year) PARTITIONED")
        cur.execute("INSERT INTO stats VALUES ('IT', 2024, 1000.0)")
        cur.execute("INSERT INTO stats VALUES ('IT', 2025, 1500.0)")
        cur.execute("INSERT INTO stats VALUES ('DE', 2024, 800.0)")
        conn.commit()

        cur.execute("SELECT revenue FROM stats WHERE region='IT' AND year=2024")
        assert cur.fetchone()[0] == 1000.0

    def test_empty_partitioned_table_queryable(self, conn):
        cur = conn.cursor()
        cur.execute("CREATE TABLE empty_t (region VARCHAR, val INTEGER)")
        cur.execute("CREATE INDEX idx_empty ON empty_t (region) PARTITIONED")
        # Should return 0 rows, not raise
        cur.execute("SELECT * FROM empty_t")
        assert cur.fetchall() == []

    def test_partitioned_persists_across_connections(self, s3, moto_server):
        with fresh_conn(moto_server, s3) as c1:
            cur = c1.cursor()
            cur.execute("CREATE TABLE logs (env VARCHAR, msg VARCHAR)")
            cur.execute("CREATE INDEX idx_logs ON logs (env) PARTITIONED")
            cur.execute("INSERT INTO logs VALUES ('prod', 'hello')")
            c1.commit()

        with fresh_conn(moto_server, s3) as c2:
            idef = c2.index_store.get_for_table("logs")
            assert idef is not None and idef.partitioned
            cur = c2.cursor()
            cur.execute("SELECT msg FROM logs WHERE env='prod'")
            assert cur.fetchone()[0] == "hello"

    def test_convert_existing_data_to_partitioned(self, conn, s3):
        cur = conn.cursor()
        cur.execute("CREATE TABLE legacy (cat VARCHAR, n INTEGER)")
        cur.execute("INSERT INTO legacy VALUES ('A', 1)")
        cur.execute("INSERT INTO legacy VALUES ('B', 2)")
        conn.commit()

        # Convert to partitioned after data exists
        cur.execute("CREATE INDEX idx_legacy ON legacy (cat) PARTITIONED")

        # Old flat file should be gone
        objects = s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])
        keys = [o["Key"] for o in objects]
        assert "legacy.parquet" not in keys
        assert any("cat=A" in k for k in keys)

        cur.execute("SELECT n FROM legacy WHERE cat='A'")
        assert cur.fetchone()[0] == 1

    def test_drop_partitioned_index_flattens_table(self, conn, s3):
        cur = conn.cursor()
        cur.execute("CREATE TABLE flat_t (region VARCHAR, v INTEGER)")
        cur.execute("CREATE INDEX idx_flat ON flat_t (region) PARTITIONED")
        cur.execute("INSERT INTO flat_t VALUES ('IT', 42)")
        conn.commit()

        cur.execute("DROP INDEX idx_flat")

        # Flat file should be back
        objects = s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])
        keys = [o["Key"] for o in objects]
        assert any(k.endswith("flat_t.parquet") for k in keys)

        cur.execute("SELECT v FROM flat_t WHERE region='IT'")
        assert cur.fetchone()[0] == 42
