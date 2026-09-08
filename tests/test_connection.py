import pytest
from moto import mock_aws

import s3ql
from s3ql.exceptions import InterfaceError

from .conftest import BUCKET, FAKE_KEY, FAKE_SECRET, REGION


class TestConnect:
    def test_connect_returns_connection(self, conn):
        assert conn is not None

    def test_connection_is_open(self, conn):
        cur = conn.cursor()
        assert cur is not None

    def test_close_idempotent(self, s3):
        c = s3ql.connect(
            bucket=BUCKET,
            aws_access_key_id=FAKE_KEY,
            aws_secret_access_key=FAKE_SECRET,
            aws_region=REGION,
        )
        c.close()
        c.close()  # second close must not raise

    def test_cursor_after_close_raises(self, s3):
        c = s3ql.connect(
            bucket=BUCKET,
            aws_access_key_id=FAKE_KEY,
            aws_secret_access_key=FAKE_SECRET,
            aws_region=REGION,
        )
        c.close()
        with pytest.raises(InterfaceError):
            c.cursor()

    def test_commit_is_noop(self, conn):
        conn.commit()  # must not raise

    def test_context_manager(self, s3):
        with s3ql.connect(
            bucket=BUCKET,
            aws_access_key_id=FAKE_KEY,
            aws_secret_access_key=FAKE_SECRET,
            aws_region=REGION,
        ) as c:
            assert c.cursor() is not None

    def test_prefix_normalized(self, conn_with_prefix):
        assert conn_with_prefix.config.prefix == "warehouse/"
