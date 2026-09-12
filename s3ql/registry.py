from __future__ import annotations

import boto3
from botocore.exceptions import ClientError

from .config import S3Config
from .exceptions import OperationalError
from .meta import TableMeta, read_meta, build_read_sql, build_empty_sql


class TableRegistry:
    """Maps table names to TableMeta and keeps DuckDB views in sync."""

    def __init__(self, config: S3Config, db):
        self._config = config
        self._db = db
        self._metas: dict[str, TableMeta] = {}          # table → TableMeta
        self._indexes: dict[str, str] = {}              # index_name → table_name
        self._s3 = self._make_s3_client()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def discover(self):
        """Scan S3 prefix for _meta.json files and register all tables."""
        try:
            paginator = self._s3.get_paginator("list_objects_v2")
            pages = paginator.paginate(
                Bucket=self._config.bucket,
                Prefix=self._config.prefix,
            )
            for page in pages:
                for obj in page.get("Contents", []):
                    key: str = obj["Key"]
                    if not key.endswith("/_meta.json"):
                        continue
                    relative = key[len(self._config.prefix):]   # "orders/_meta.json"
                    table_name = relative[: -len("/_meta.json")]  # "orders"
                    if "/" in table_name:
                        continue  # nested path — skip
                    meta = read_meta(self._s3, self._config.bucket, key)
                    self._register(table_name, meta)
        except ClientError as exc:
            raise OperationalError(f"Cannot list S3 bucket: {exc}") from exc

    def register_table(self, table_name: str, meta: TableMeta):
        """Register or update a table with the given metadata."""
        self._register(table_name, meta)

    def unregister(self, table_name: str):
        """Remove a table from the registry and drop its DuckDB view."""
        if table_name in self._metas:
            meta = self._metas.pop(table_name)
            if meta.index_name:
                self._indexes.pop(meta.index_name, None)
            self._db.execute(f'DROP VIEW IF EXISTS "{table_name}";')

    def exists(self, table_name: str) -> bool:
        return table_name in self._metas

    def meta(self, table_name: str) -> TableMeta | None:
        return self._metas.get(table_name)

    def table_for_index(self, index_name: str) -> str | None:
        return self._indexes.get(index_name)

    @property
    def tables(self) -> list[str]:
        return list(self._metas.keys())

    @property
    def s3_client(self):
        return self._s3

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _register(self, name: str, meta: TableMeta):
        # Update index name mapping
        old_meta = self._metas.get(name)
        if old_meta and old_meta.index_name:
            self._indexes.pop(old_meta.index_name, None)

        self._metas[name] = meta
        if meta.index_name:
            self._indexes[meta.index_name] = name

        self._rebuild_view(name, meta)

    def _rebuild_view(self, name: str, meta: TableMeta):
        uris = [self._config.file_uri(name, f.path) for f in meta.files]
        sql = build_read_sql(meta, uris) or build_empty_sql(meta.schema)
        self._db.execute(f'CREATE OR REPLACE VIEW "{name}" AS {sql};')

    def _make_s3_client(self):
        kwargs = dict(
            region_name=self._config.aws_region,
            aws_access_key_id=self._config.aws_access_key_id,
            aws_secret_access_key=self._config.aws_secret_access_key,
        )
        if self._config.endpoint_url:
            kwargs["endpoint_url"] = self._config.endpoint_url
        return boto3.client("s3", **kwargs)
