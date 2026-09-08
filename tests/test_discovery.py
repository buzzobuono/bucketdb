"""Test that tables pre-existing on S3 are discovered at connect time."""
import io

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from moto import mock_aws

import s3ql

from .conftest import BUCKET, FAKE_KEY, FAKE_SECRET, REGION


def _upload_parquet(s3_client, key: str, table: pa.Table):
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    s3_client.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())


@pytest.fixture()
def prepopulated_s3(aws_credentials):
    with mock_aws():
        client = boto3.client("s3", region_name=REGION)
        client.create_bucket(Bucket=BUCKET)

        schema = pa.schema([("id", pa.int32()), ("name", pa.string())])
        table = pa.table({"id": [1, 2, 3], "name": ["alice", "bob", "carol"]}, schema=schema)
        _upload_parquet(client, "customers.parquet", table)

        orders_schema = pa.schema([("order_id", pa.int32()), ("amount", pa.float64())])
        orders = pa.table({"order_id": [10, 20], "amount": [99.0, 5.5]}, schema=orders_schema)
        _upload_parquet(client, "orders.parquet", orders)

        yield client


class TestDiscovery:
    def test_tables_discovered_on_connect(self, prepopulated_s3):
        with s3ql.connect(
            bucket=BUCKET,
            aws_access_key_id=FAKE_KEY,
            aws_secret_access_key=FAKE_SECRET,
            aws_region=REGION,
        ) as conn:
            assert "customers" in conn.registry.tables
            assert "orders" in conn.registry.tables

    def test_can_query_discovered_table(self, prepopulated_s3):
        with s3ql.connect(
            bucket=BUCKET,
            aws_access_key_id=FAKE_KEY,
            aws_secret_access_key=FAKE_SECRET,
            aws_region=REGION,
        ) as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM customers")
            assert cur.fetchone()[0] == 3

    def test_discovered_data_correct(self, prepopulated_s3):
        with s3ql.connect(
            bucket=BUCKET,
            aws_access_key_id=FAKE_KEY,
            aws_secret_access_key=FAKE_SECRET,
            aws_region=REGION,
        ) as conn:
            cur = conn.cursor()
            cur.execute("SELECT name FROM customers ORDER BY id")
            names = [r[0] for r in cur.fetchall()]
            assert names == ["alice", "bob", "carol"]

    def test_prefix_isolation(self, aws_credentials):
        """Tables outside the prefix must not be discovered."""
        with mock_aws():
            client = boto3.client("s3", region_name=REGION)
            client.create_bucket(Bucket=BUCKET)

            schema = pa.schema([("x", pa.int32())])
            t = pa.table({"x": [1]}, schema=schema)
            _upload_parquet(client, "outside.parquet", t)
            _upload_parquet(client, "ns/inside.parquet", t)

            with s3ql.connect(
                bucket=BUCKET,
                prefix="ns/",
                aws_access_key_id=FAKE_KEY,
                aws_secret_access_key=FAKE_SECRET,
                aws_region=REGION,
            ) as conn:
                assert "inside" in conn.registry.tables
                assert "outside" not in conn.registry.tables
