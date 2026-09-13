import boto3
import pytest
from moto.server import ThreadedMotoServer

import bucketdb

BUCKET = "test-bucket"
REGION = "us-east-1"
FAKE_KEY = "fakekey"
FAKE_SECRET = "fakesecret"


@pytest.fixture(scope="session")
def moto_server():
    """A real local HTTP server mocking S3.

    boto3-level mocking (moto's mock_aws decorator) only patches botocore,
    not DuckDB's httpfs extension, which speaks raw HTTP directly to S3.
    A real local server is required so both boto3 and DuckDB hit the mock.
    """
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    server.start()
    host, port = server.get_host_and_port()
    yield f"http://127.0.0.1:{port}"
    server.stop()


@pytest.fixture()
def s3(moto_server):
    client = boto3.client(
        "s3",
        region_name=REGION,
        aws_access_key_id=FAKE_KEY,
        aws_secret_access_key=FAKE_SECRET,
        endpoint_url=moto_server,
    )
    client.create_bucket(Bucket=BUCKET)
    yield client
    objects = client.list_objects_v2(Bucket=BUCKET).get("Contents", [])
    if objects:
        client.delete_objects(
            Bucket=BUCKET,
            Delete={"Objects": [{"Key": o["Key"]} for o in objects]},
        )
    client.delete_bucket(Bucket=BUCKET)


@pytest.fixture()
def conn(s3, moto_server):
    """Open an S3QL connection against the mocked bucket."""
    with bucketdb.connect(
        bucket=BUCKET,
        aws_access_key_id=FAKE_KEY,
        aws_secret_access_key=FAKE_SECRET,
        aws_region=REGION,
        endpoint_url=moto_server,
    ) as connection:
        yield connection


@pytest.fixture()
def conn_with_prefix(s3, moto_server):
    with bucketdb.connect(
        bucket=BUCKET,
        aws_access_key_id=FAKE_KEY,
        aws_secret_access_key=FAKE_SECRET,
        aws_region=REGION,
        prefix="warehouse/",
        endpoint_url=moto_server,
    ) as connection:
        yield connection
