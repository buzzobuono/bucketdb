import pytest

from bucketdb.exceptions import ProgrammingError


class TestCreateTable:
    def test_create_table(self, conn):
        cur = conn.cursor()
        cur.execute("CREATE TABLE users (id INTEGER, name VARCHAR)")
        assert conn.registry.exists("users")

    def test_create_table_uploads_parquet(self, conn, s3):
        cur = conn.cursor()
        cur.execute("CREATE TABLE products (id INTEGER, price DOUBLE)")
        response = s3.list_objects_v2(Bucket="test-bucket")
        keys = [o["Key"] for o in response.get("Contents", [])]
        assert "products/_meta.json" in keys

    def test_create_table_if_not_exists(self, conn):
        cur = conn.cursor()
        cur.execute("CREATE TABLE t1 (id INTEGER)")
        cur.execute("CREATE TABLE IF NOT EXISTS t1 (id INTEGER)")  # must not raise

    def test_create_table_duplicate_raises(self, conn):
        cur = conn.cursor()
        cur.execute("CREATE TABLE t2 (id INTEGER)")
        with pytest.raises(ProgrammingError, match="already exists"):
            cur.execute("CREATE TABLE t2 (id INTEGER)")

    def test_create_table_with_prefix(self, conn_with_prefix, s3):
        cur = conn_with_prefix.cursor()
        cur.execute("CREATE TABLE items (id INTEGER, label VARCHAR)")
        response = s3.list_objects_v2(Bucket="test-bucket", Prefix="warehouse/")
        keys = [o["Key"] for o in response.get("Contents", [])]
        assert "warehouse/items/_meta.json" in keys


class TestDropTable:
    def test_drop_table(self, conn):
        cur = conn.cursor()
        cur.execute("CREATE TABLE to_drop (id INTEGER)")
        cur.execute("DROP TABLE to_drop")
        assert not conn.registry.exists("to_drop")

    def test_drop_table_removes_s3_object(self, conn, s3):
        cur = conn.cursor()
        cur.execute("CREATE TABLE gone (id INTEGER)")
        cur.execute("DROP TABLE gone")
        response = s3.list_objects_v2(Bucket="test-bucket")
        keys = [o["Key"] for o in response.get("Contents", [])]
        assert "gone.parquet" not in keys

    def test_drop_table_if_exists(self, conn):
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS nonexistent")  # must not raise

    def test_drop_table_missing_raises(self, conn):
        cur = conn.cursor()
        with pytest.raises(ProgrammingError, match="does not exist"):
            cur.execute("DROP TABLE nonexistent")
