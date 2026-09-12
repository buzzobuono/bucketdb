import boto3
from botocore.exceptions import ClientError

from .config import S3Config
from .exceptions import OperationalError


class TableRegistry:
    """Maps table names to S3 URIs and keeps DuckDB views in sync."""

    def __init__(self, config: S3Config, db):
        self._config = config
        self._db = db
        self._tables: dict[str, str] = {}  # name → s3 uri (or glob uri for partitioned)
        self._s3 = self._make_s3_client()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def discover(self, index_store=None):
        """Scan S3 and register tables (flat and partitioned)."""
        partitioned_tables: set[str] = set()

        if index_store:
            for idef in index_store.all_indexes:
                if idef.partitioned:
                    partitioned_tables.add(idef.table)
                    self._register_partitioned(idef.table, idef.columns, idef.schema or [])

        try:
            paginator = self._s3.get_paginator("list_objects_v2")
            pages = paginator.paginate(
                Bucket=self._config.bucket,
                Prefix=self._config.prefix,
            )
            for page in pages:
                for obj in page.get("Contents", []):
                    key: str = obj["Key"]
                    if not key.endswith(".parquet"):
                        continue
                    relative = key[len(self._config.prefix):]
                    # Only flat (non-nested) parquet files are registered here;
                    # partitioned tables live in subdirectories.
                    if "/" not in relative:
                        name = relative[: -len(".parquet")]
                        if name not in partitioned_tables:
                            uri = f"s3://{self._config.bucket}/{key}"
                            self._register(name, uri)
        except ClientError as exc:
            raise OperationalError(f"Cannot list S3 bucket: {exc}") from exc

    def register(self, table_name: str):
        """Register a flat table by name (file must exist on S3)."""
        uri = self._config.table_uri(table_name)
        self._register(table_name, uri)

    def register_partitioned(self, table_name: str, part_cols: list[str], schema: list[list[str]]):
        """Register or re-register a partitioned table view."""
        self._register_partitioned(table_name, part_cols, schema)

    def unregister(self, table_name: str):
        """Remove a table from the registry and drop its DuckDB view."""
        if table_name in self._tables:
            self._db.execute(f'DROP VIEW IF EXISTS "{table_name}";')
            del self._tables[table_name]

    def exists(self, table_name: str) -> bool:
        return table_name in self._tables

    def uri(self, table_name: str) -> str | None:
        return self._tables.get(table_name)

    @property
    def tables(self) -> list[str]:
        return list(self._tables.keys())

    @property
    def s3_client(self):
        return self._s3

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _register(self, name: str, uri: str):
        self._tables[name] = uri
        self._db.execute(
            f'CREATE OR REPLACE VIEW "{name}" AS SELECT * FROM read_parquet(\'{uri}\');'
        )

    def _register_partitioned(self, name: str, part_cols: list[str], schema: list[list[str]]):
        n = len(part_cols)
        glob_uri = self._config.table_glob_uri(name, n)
        self._tables[name] = glob_uri

        # Try building a real view from partition files; fall back to empty schema view.
        try:
            self._db.execute(
                f'CREATE OR REPLACE VIEW "{name}" AS '
                f"SELECT * FROM read_parquet('{glob_uri}', hive_partitioning=True);"
            )
        except Exception:
            if schema:
                cols_sql = ", ".join(
                    f'NULL::{dtype} AS "{col}"' for col, dtype in schema
                )
                self._db.execute(
                    f'CREATE OR REPLACE VIEW "{name}" AS SELECT {cols_sql} WHERE 1=0;'
                )

    def _key_to_table_name(self, key: str) -> str:
        name = key[len(self._config.prefix):]
        return name[: -len(".parquet")]

    def _make_s3_client(self):
        kwargs = dict(
            region_name=self._config.aws_region,
            aws_access_key_id=self._config.aws_access_key_id,
            aws_secret_access_key=self._config.aws_secret_access_key,
        )
        if self._config.endpoint_url:
            kwargs["endpoint_url"] = self._config.endpoint_url
        return boto3.client("s3", **kwargs)
