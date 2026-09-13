"""Tests for CREATE INDEX (sort key) and CREATE INDEX ... PARTITIONED."""
import pytest
import s3ql
from tests.conftest import BUCKET, REGION, FAKE_KEY, FAKE_SECRET


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
        meta = conn.registry.meta("products")
        assert meta.sort_by == ["price"]
        assert meta.partition_by == []
        assert meta.index_name == "idx_price"

    def test_create_index_multi_column(self, conn):
        cur = conn.cursor()
        cur.execute("CREATE TABLE sales (region VARCHAR, date VARCHAR, amount FLOAT)")
        cur.execute("CREATE INDEX idx_rd ON sales (region, date)")
        meta = conn.registry.meta("sales")
        assert meta.sort_by == ["region", "date"]

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

    def test_drop_index(self, conn):
        cur = conn.cursor()
        cur.execute("CREATE TABLE t2 (x INTEGER)")
        cur.execute("CREATE INDEX idx_x ON t2 (x)")
        assert conn.registry.table_for_index("idx_x") == "t2"
        cur.execute("DROP INDEX idx_x")
        assert conn.registry.table_for_index("idx_x") is None
        assert conn.registry.meta("t2").index_name is None

    def test_create_index_if_not_exists(self, conn):
        cur = conn.cursor()
        cur.execute("CREATE TABLE t3 (x INTEGER)")
        cur.execute("CREATE INDEX idx_x3 ON t3 (x)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_x3 ON t3 (x)")

    def test_drop_index_if_exists(self, conn):
        cur = conn.cursor()
        cur.execute("DROP INDEX IF EXISTS nonexistent")

    def test_index_persists_across_connections(self, s3, moto_server):
        with fresh_conn(moto_server, s3) as c1:
            c1.cursor().execute("CREATE TABLE persist_t (n INTEGER)")
            c1.cursor().execute("CREATE INDEX idx_persist ON persist_t (n)")

        with fresh_conn(moto_server, s3) as c2:
            meta = c2.registry.meta("persist_t")
            assert meta.sort_by == ["n"]
            assert meta.index_name == "idx_persist"

    def test_index_removed_on_drop_table(self, conn):
        cur = conn.cursor()
        cur.execute("CREATE TABLE tdrop (x INTEGER)")
        cur.execute("CREATE INDEX idx_tdrop ON tdrop (x)")
        cur.execute("DROP TABLE tdrop")
        assert not conn.registry.exists("tdrop")


# ---------------------------------------------------------------------------
# Partitioned index
# ---------------------------------------------------------------------------

class TestPartitionedIndex:
    def test_create_partitioned_index(self, conn):
        cur = conn.cursor()
        cur.execute("CREATE TABLE orders (region VARCHAR, amount FLOAT)")
        cur.execute("CREATE INDEX idx_region ON orders (region) PARTITIONED")
        meta = conn.registry.meta("orders")
        assert meta.partition_by == ["region"]
        assert meta.index_name == "idx_region"

    def test_insert_and_query_partitioned(self, conn):
        cur = conn.cursor()
        cur.execute("CREATE TABLE sales (region VARCHAR, amount FLOAT)")
        cur.execute("CREATE INDEX idx_sales ON sales (region) PARTITIONED")
        cur.execute("INSERT INTO sales VALUES ('IT', 100.0)")
        cur.execute("INSERT INTO sales VALUES ('DE', 200.0)")
        cur.execute("INSERT INTO sales VALUES ('IT', 150.0)")
        conn.commit()
        cur.execute("SELECT amount FROM sales WHERE region='IT' ORDER BY amount")
        assert [r[0] for r in cur.fetchall()] == [100.0, 150.0]
        cur.execute("SELECT COUNT(*) FROM sales")
        assert cur.fetchone()[0] == 3

    def test_partition_files_in_meta(self, conn):
        cur = conn.cursor()
        cur.execute("CREATE TABLE evts (country VARCHAR, v INTEGER)")
        cur.execute("CREATE INDEX idx_evts ON evts (country) PARTITIONED")
        cur.execute("INSERT INTO evts VALUES ('IT', 1)")
        cur.execute("INSERT INTO evts VALUES ('DE', 2)")
        conn.commit()
        meta = conn.registry.meta("evts")
        partitions = {list(f.partition.values())[0] for f in meta.files if f.partition}
        assert "IT" in partitions
        assert "DE" in partitions

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
            meta = c2.registry.meta("logs")
            assert meta.partition_by == ["env"]
            cur = c2.cursor()
            cur.execute("SELECT msg FROM logs WHERE env='prod'")
            assert cur.fetchone()[0] == "hello"

    def test_convert_existing_data_to_partitioned(self, conn):
        cur = conn.cursor()
        cur.execute("CREATE TABLE legacy (cat VARCHAR, n INTEGER)")
        cur.execute("INSERT INTO legacy VALUES ('A', 1)")
        cur.execute("INSERT INTO legacy VALUES ('B', 2)")
        conn.commit()
        cur.execute("CREATE INDEX idx_legacy ON legacy (cat) PARTITIONED")
        meta = conn.registry.meta("legacy")
        assert meta.partition_by == ["cat"]
        assert len(meta.files) == 2
        cur.execute("SELECT n FROM legacy WHERE cat='A'")
        assert cur.fetchone()[0] == 1

    def test_drop_partitioned_index_flattens_table(self, conn):
        cur = conn.cursor()
        cur.execute("CREATE TABLE flat_t (region VARCHAR, v INTEGER)")
        cur.execute("CREATE INDEX idx_flat ON flat_t (region) PARTITIONED")
        cur.execute("INSERT INTO flat_t VALUES ('IT', 42)")
        conn.commit()
        cur.execute("DROP INDEX idx_flat")
        meta = conn.registry.meta("flat_t")
        assert meta.partition_by == []
        assert len(meta.files) == 1
        cur.execute("SELECT v FROM flat_t WHERE region='IT'")
        assert cur.fetchone()[0] == 42


# ---------------------------------------------------------------------------
# Vacuum
# ---------------------------------------------------------------------------

class TestVacuum:
    def test_vacuum_reduces_file_count(self, conn):
        cur = conn.cursor()
        cur.execute("CREATE TABLE v_t (n INTEGER)")
        for i in range(5):
            cur.execute(f"INSERT INTO v_t VALUES ({i})")
            conn.commit()
        meta = conn.registry.meta("v_t")
        assert len(meta.files) == 5
        conn.vacuum("v_t")
        meta = conn.registry.meta("v_t")
        assert len(meta.files) == 1

    def test_vacuum_preserves_data(self, conn):
        cur = conn.cursor()
        cur.execute("CREATE TABLE v2 (n INTEGER)")
        for i in range(3):
            cur.execute(f"INSERT INTO v2 VALUES ({i * 10})")
            conn.commit()
        conn.vacuum("v2")
        cur.execute("SELECT n FROM v2 ORDER BY n")
        assert [r[0] for r in cur.fetchall()] == [0, 10, 20]

    def test_vacuum_applies_sort_key(self, conn):
        cur = conn.cursor()
        cur.execute("CREATE TABLE v3 (n INTEGER)")
        cur.execute("CREATE INDEX idx_v3 ON v3 (n)")
        cur.execute("INSERT INTO v3 VALUES (30)")
        conn.commit()
        cur.execute("INSERT INTO v3 VALUES (10)")
        conn.commit()
        conn.vacuum("v3")
        meta = conn.registry.meta("v3")
        assert len(meta.files) == 1
        cur.execute("SELECT n FROM v3")
        rows = [r[0] for r in cur.fetchall()]
        assert rows == sorted(rows)

    def test_vacuum_via_cli_method(self, conn):
        cur = conn.cursor()
        cur.execute("CREATE TABLE v4 (x INTEGER)")
        cur.execute("INSERT INTO v4 VALUES (1)")
        conn.commit()
        cur.execute("INSERT INTO v4 VALUES (2)")
        conn.commit()
        conn.vacuum("v4")
        cur.execute("SELECT COUNT(*) FROM v4")
        assert cur.fetchone()[0] == 2


# ---------------------------------------------------------------------------
# Partial-load UPDATE/DELETE on partitioned tables
# ---------------------------------------------------------------------------

class TestPartitionedPartialLoad:
    def test_update_only_rewrites_matching_partition(self, conn):
        cur = conn.cursor()
        cur.execute("CREATE TABLE sales (region VARCHAR, amount FLOAT)")
        cur.execute("CREATE INDEX idx ON sales (region) PARTITIONED")
        cur.execute("INSERT INTO sales VALUES ('IT', 100.0)")
        cur.execute("INSERT INTO sales VALUES ('DE', 200.0)")
        conn.commit()

        meta_before = conn.registry.meta("sales")
        it_file = next(f for f in meta_before.files if f.partition.get("region") == "IT")
        de_file = next(f for f in meta_before.files if f.partition.get("region") == "DE")

        cur.execute("UPDATE sales SET amount = 999.0 WHERE region = 'IT'")
        conn.commit()

        meta_after = conn.registry.meta("sales")
        # DE file must be unchanged (same path)
        de_after = next(f for f in meta_after.files if f.partition.get("region") == "DE")
        assert de_after.path == de_file.path
        # IT file must be a new file
        it_after = next(f for f in meta_after.files if f.partition.get("region") == "IT")
        assert it_after.path != it_file.path

        cur.execute("SELECT amount FROM sales WHERE region = 'IT'")
        assert cur.fetchone()[0] == 999.0
        cur.execute("SELECT amount FROM sales WHERE region = 'DE'")
        assert cur.fetchone()[0] == 200.0

    def test_delete_only_rewrites_matching_partition(self, conn):
        cur = conn.cursor()
        cur.execute("CREATE TABLE logs (env VARCHAR, msg VARCHAR)")
        cur.execute("CREATE INDEX idx ON logs (env) PARTITIONED")
        cur.execute("INSERT INTO logs VALUES ('prod', 'ok')")
        cur.execute("INSERT INTO logs VALUES ('staging', 'test')")
        conn.commit()

        meta_before = conn.registry.meta("logs")
        staging_file = next(f for f in meta_before.files if f.partition.get("env") == "staging")

        cur.execute("DELETE FROM logs WHERE env = 'prod'")
        conn.commit()

        meta_after = conn.registry.meta("logs")
        # staging file preserved
        staging_after = next(f for f in meta_after.files if f.partition.get("env") == "staging")
        assert staging_after.path == staging_file.path

        cur.execute("SELECT COUNT(*) FROM logs WHERE env = 'prod'")
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT msg FROM logs WHERE env = 'staging'")
        assert cur.fetchone()[0] == "test"

    def test_unfiltered_update_falls_back_to_full_load(self, conn):
        cur = conn.cursor()
        cur.execute("CREATE TABLE orders (region VARCHAR, amount FLOAT)")
        cur.execute("CREATE INDEX idx ON orders (region) PARTITIONED")
        cur.execute("INSERT INTO orders VALUES ('IT', 10.0)")
        cur.execute("INSERT INTO orders VALUES ('DE', 20.0)")
        conn.commit()

        # No partition filter in WHERE — must fall back to full load and rewrite all
        cur.execute("UPDATE orders SET amount = amount * 2")
        conn.commit()

        cur.execute("SELECT amount FROM orders WHERE region = 'IT'")
        assert cur.fetchone()[0] == 20.0
        cur.execute("SELECT amount FROM orders WHERE region = 'DE'")
        assert cur.fetchone()[0] == 40.0

    def test_two_updates_on_different_partitions(self, conn):
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (cat VARCHAR, val INTEGER)")
        cur.execute("CREATE INDEX idx ON t (cat) PARTITIONED")
        cur.execute("INSERT INTO t VALUES ('A', 1)")
        cur.execute("INSERT INTO t VALUES ('B', 2)")
        conn.commit()

        # First UPDATE: partial load on A
        cur.execute("UPDATE t SET val = 10 WHERE cat = 'A'")
        # Second UPDATE: different partition — must upgrade to full load
        cur.execute("UPDATE t SET val = 20 WHERE cat = 'B'")
        conn.commit()

        cur.execute("SELECT val FROM t WHERE cat = 'A'")
        assert cur.fetchone()[0] == 10
        cur.execute("SELECT val FROM t WHERE cat = 'B'")
        assert cur.fetchone()[0] == 20
