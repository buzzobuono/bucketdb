"""Test that tables pre-existing on S3 are discovered at connect time."""
import io

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import bucketdb
from bucketdb.meta import FileMeta, TableMeta, write_meta

from .conftest import BUCKET, FAKE_KEY, FAKE_SECRET, REGION


def _seed_table(s3_client, prefix: str, table_name: str, schema_cols: list, table: pa.Table):
    """Write a table directly to S3 the way bucketdb itself lays it out:
    <prefix><table>/_meta.json + <prefix><table>/data/<file>.parquet — as an
    external writer (or a table created by a prior connection) would leave it."""
    filename = "part-seed.parquet"
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    s3_client.put_object(
        Bucket=BUCKET, Key=f"{prefix}{table_name}/data/{filename}", Body=buf.getvalue()
    )
    meta = TableMeta(schema=schema_cols, files=[FileMeta(path=f"data/{filename}")])
    write_meta(s3_client, BUCKET, f"{prefix}{table_name}/_meta.json", meta)


@pytest.fixture()
def prepopulated_s3(s3):
    schema = pa.schema([("id", pa.int32()), ("name", pa.string())])
    table = pa.table({"id": [1, 2, 3], "name": ["alice", "bob", "carol"]}, schema=schema)
    _seed_table(s3, "", "customers", [["id", "INTEGER"], ["name", "VARCHAR"]], table)

    orders_schema = pa.schema([("order_id", pa.int32()), ("amount", pa.float64())])
    orders = pa.table({"order_id": [10, 20], "amount": [99.0, 5.5]}, schema=orders_schema)
    _seed_table(s3, "", "orders", [["order_id", "INTEGER"], ["amount", "DOUBLE"]], orders)

    yield s3


class TestDiscovery:
    def test_tables_discovered_on_connect(self, prepopulated_s3, moto_server):
        with bucketdb.connect(
            bucket=BUCKET,
            aws_access_key_id=FAKE_KEY,
            aws_secret_access_key=FAKE_SECRET,
            aws_region=REGION,
            endpoint_url=moto_server,
        ) as conn:
            assert "customers" in conn.registry.tables
            assert "orders" in conn.registry.tables

    def test_can_query_discovered_table(self, prepopulated_s3, moto_server):
        with bucketdb.connect(
            bucket=BUCKET,
            aws_access_key_id=FAKE_KEY,
            aws_secret_access_key=FAKE_SECRET,
            aws_region=REGION,
            endpoint_url=moto_server,
        ) as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM customers")
            assert cur.fetchone()[0] == 3

    def test_discovered_data_correct(self, prepopulated_s3, moto_server):
        with bucketdb.connect(
            bucket=BUCKET,
            aws_access_key_id=FAKE_KEY,
            aws_secret_access_key=FAKE_SECRET,
            aws_region=REGION,
            endpoint_url=moto_server,
        ) as conn:
            cur = conn.cursor()
            cur.execute("SELECT name FROM customers ORDER BY id")
            names = [r[0] for r in cur.fetchall()]
            assert names == ["alice", "bob", "carol"]

    def test_prefix_isolation(self, s3, moto_server):
        """Tables outside the prefix must not be discovered."""
        schema = pa.schema([("x", pa.int32())])
        t = pa.table({"x": [1]}, schema=schema)
        _seed_table(s3, "", "outside", [["x", "INTEGER"]], t)
        _seed_table(s3, "ns/", "inside", [["x", "INTEGER"]], t)

        with bucketdb.connect(
            bucket=BUCKET,
            prefix="ns/",
            aws_access_key_id=FAKE_KEY,
            aws_secret_access_key=FAKE_SECRET,
            aws_region=REGION,
            endpoint_url=moto_server,
        ) as conn:
            assert "inside" in conn.registry.tables
            assert "outside" not in conn.registry.tables
