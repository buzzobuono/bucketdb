from __future__ import annotations

import re
from typing import TYPE_CHECKING

from .exceptions import OperationalError, ProgrammingError

if TYPE_CHECKING:
    from .connection import S3QLConnection

_TABLE_RE = re.compile(
    r"(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+[\"']?(\w+)[\"']?",
    re.IGNORECASE,
)


class Transaction:
    def __init__(self, conn: "S3QLConnection"):
        self._conn = conn
        # For flat tables: table → ETag string
        # For partitioned tables: table → {s3_key: ETag} dict
        self._snapshots: dict[str, str | dict[str, str]] = {}
        self._loaded: dict[str, str] = {}   # table → DuckDB temp table name
        self._modified: set[str] = set()    # tables with pending DML

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def preload(self, *tables: str):
        for table in tables:
            if not self._conn.registry.exists(table):
                raise ProgrammingError(f"Table '{table}' does not exist")
            if table not in self._loaded:
                self._load(table)

    def unload(self, *tables: str):
        for table in tables:
            if table not in self._loaded:
                raise ProgrammingError(f"Table '{table}' is not preloaded")
            if table in self._modified:
                raise ProgrammingError(
                    f"Cannot unload '{table}': has uncommitted changes — commit or rollback first"
                )
            temp = self._loaded.pop(table)
            self._snapshots.pop(table, None)
            self._conn.db.execute(f"DROP TABLE IF EXISTS {temp}")
            self._restore_view(table)

    def apply(self, sql: str, params: list) -> int:
        table = self._extract_table(sql)
        if not self._conn.registry.exists(table):
            raise ProgrammingError(f"Table '{table}' does not exist")
        if table not in self._loaded:
            self._load(table)
        self._modified.add(table)
        temp = self._loaded[table]
        dml = re.sub(rf'\b{re.escape(table)}\b', temp, sql, flags=re.IGNORECASE)
        if params:
            rel = self._conn.db.execute(dml, params)
        else:
            rel = self._conn.db.execute(dml)
        result = rel.fetchone()
        return result[0] if result else -1

    def flush(self):
        from .writer import _write_parquet_to_s3, _write_partitioned_arrow

        # Phase 1: verify ETags for all modified tables
        for table in self._modified:
            if self._conn.index_store.is_partitioned(table):
                self._verify_etags_partitioned(table)
            else:
                current = self._etag_flat(table)
                if current != self._snapshots[table]:
                    raise OperationalError(
                        f"Commit conflict: table '{table}' was modified by another writer"
                    )

        # Phase 2: write modified tables to S3
        for table in self._modified:
            temp = self._loaded[table]
            idef = self._conn.index_store.get_for_table(table)

            if idef and idef.partitioned:
                import pyarrow as pa
                arrow_table = self._conn.db.execute(f"SELECT * FROM {temp}").to_arrow_table()
                _write_partitioned_arrow(self._conn, table, idef.columns, arrow_table)
                self._conn.registry.register_partitioned(table, idef.columns, idef.schema or [])
            else:
                uri = self._conn.config.table_uri(table)
                arrow_table = self._conn.db.execute(f"SELECT * FROM {temp}").to_arrow_table()
                if idef and idef.columns and len(arrow_table) > 0:
                    arrow_table = arrow_table.sort_by(
                        [(col, "ascending") for col in idef.columns]
                    )
                _write_parquet_to_s3(self._conn, uri, arrow_table)

        # Phase 3: drop all temp tables and restore views
        for table, temp in self._loaded.items():
            self._conn.db.execute(f"DROP TABLE IF EXISTS {temp}")
            self._restore_view(table)
        self._snapshots.clear()
        self._loaded.clear()
        self._modified.clear()

    def discard(self):
        for table, temp in self._loaded.items():
            self._conn.db.execute(f"DROP TABLE IF EXISTS {temp}")
            self._restore_view(table)
        self._snapshots.clear()
        self._loaded.clear()
        self._modified.clear()

    @property
    def preloaded(self) -> list[str]:
        return [t for t in self._loaded if t not in self._modified]

    @property
    def modified(self) -> list[str]:
        return list(self._modified)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load(self, table: str):
        temp = f"_tx_{table}"
        if self._conn.index_store.is_partitioned(table):
            self._snapshots[table] = self._etag_partitioned(table)
            idef = self._conn.index_store.get_for_table(table)
            n = len(idef.columns)
            glob_uri = self._conn.config.table_glob_uri(table, n)
            try:
                self._conn.db.execute(
                    f"CREATE OR REPLACE TABLE {temp} AS "
                    f"SELECT * FROM read_parquet('{glob_uri}', hive_partitioning=True)"
                )
            except Exception:
                # No partition files yet — empty table with schema
                schema = idef.schema or []
                if schema:
                    cols_sql = ", ".join(
                        f'NULL::{dtype} AS "{col}"' for col, dtype in schema
                    )
                    self._conn.db.execute(
                        f"CREATE OR REPLACE TABLE {temp} AS SELECT {cols_sql} WHERE 1=0"
                    )
                else:
                    self._conn.db.execute(f"CREATE OR REPLACE TABLE {temp} (dummy INTEGER)")
        else:
            uri = self._conn.config.table_uri(table)
            self._snapshots[table] = self._etag_flat(table)
            self._conn.db.execute(
                f"CREATE OR REPLACE TABLE {temp} AS SELECT * FROM read_parquet('{uri}')"
            )

        self._loaded[table] = temp
        self._conn.db.execute(f'CREATE OR REPLACE VIEW "{table}" AS SELECT * FROM {temp}')

    def _restore_view(self, table: str):
        idef = self._conn.index_store.get_for_table(table)
        if idef and idef.partitioned:
            self._conn.registry.register_partitioned(table, idef.columns, idef.schema or [])
        else:
            uri = self._conn.config.table_uri(table)
            self._conn.db.execute(
                f'CREATE OR REPLACE VIEW "{table}" AS SELECT * FROM read_parquet(\'{uri}\')'
            )

    def _etag_flat(self, table: str) -> str:
        uri = self._conn.config.table_uri(table)
        bucket = self._conn.config.bucket
        key = uri[len(f"s3://{bucket}/"):]
        try:
            resp = self._conn.registry.s3_client.head_object(Bucket=bucket, Key=key)
            return resp["ETag"]
        except Exception as exc:
            raise OperationalError(f"Cannot read ETag for '{table}': {exc}") from exc

    def _etag_partitioned(self, table: str) -> dict[str, str]:
        """Return {s3_key: etag} for all current partition files."""
        bucket = self._conn.config.bucket
        prefix = self._conn.config.table_base_prefix(table)
        s3 = self._conn.registry.s3_client
        etags: dict[str, str] = {}
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".parquet"):
                    etags[key] = obj["ETag"]
        return etags

    def _verify_etags_partitioned(self, table: str):
        """Check that no partition files changed since load time."""
        snapshot = self._snapshots[table]  # {key: etag}
        if not isinstance(snapshot, dict):
            return
        current = self._etag_partitioned(table)
        for key, etag in snapshot.items():
            if current.get(key) != etag:
                raise OperationalError(
                    f"Commit conflict: partition '{key}' was modified by another writer"
                )

    @staticmethod
    def _extract_table(sql: str) -> str:
        m = _TABLE_RE.search(sql)
        if not m:
            raise ProgrammingError(f"Cannot extract table name from: {sql}")
        return m.group(1)
