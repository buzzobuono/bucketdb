import os

import boto3
import pytest
from moto import mock_aws

import s3ql

BUCKET = "test-bucket"
REGION = "us-east-1"
FAKE_KEY = "fakekey"
FAKE_SECRET = "fakesecret"


@pytest.fixture()
def aws_credentials(monkeypatch):
    """Prevent moto from picking up real AWS credentials."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", FAKE_KEY)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", FAKE_SECRET)
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")


@pytest.fixture()
def s3(aws_credentials):
    with mock_aws():
        client = boto3.client("s3", region_name=REGION)
        client.create_bucket(Bucket=BUCKET)
        yield client


@pytest.fixture()
def conn(s3):
    """Open an S3QL connection against the mocked bucket."""
    with s3ql.connect(
        bucket=BUCKET,
        aws_access_key_id=FAKE_KEY,
        aws_secret_access_key=FAKE_SECRET,
        aws_region=REGION,
    ) as connection:
        yield connection


@pytest.fixture()
def conn_with_prefix(s3):
    with s3ql.connect(
        bucket=BUCKET,
        aws_access_key_id=FAKE_KEY,
        aws_secret_access_key=FAKE_SECRET,
        aws_region=REGION,
        prefix="warehouse/",
    ) as connection:
        yield connection
