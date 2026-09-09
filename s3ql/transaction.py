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
        self._snapshots: dict[str, str] = {}  # table → ETag at load time
        self._loaded: dict[str, str] = {}      # table → DuckDB temp table name
        self._modified: set[str] = set()       # tables with pending DML

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
        # Phase 1: verify ETag only for modified tables
        for table in self._modified:
            current = self._etag(table)
            if current != self._snapshots[table]:
                raise OperationalError(
                    f"Commit conflict: table '{table}' was modified by another writer"
                )
        # Phase 2: write only modified tables to S3
        from .writer import _write_parquet_to_s3
        for table in self._modified:
            temp = self._loaded[table]
            uri = self._conn.config.table_uri(table)
            arrow_table = self._conn.db.execute(f"SELECT * FROM {temp}").to_arrow_table()
            _write_parquet_to_s3(self._conn, uri, arrow_table)

        # Phase 3: drop all loaded tables and restore views
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
        uri = self._conn.config.table_uri(table)
        self._snapshots[table] = self._etag(table)
        temp = f"_tx_{table}"
        self._conn.db.execute(
            f"CREATE OR REPLACE TABLE {temp} AS SELECT * FROM read_parquet('{uri}')"
        )
        self._loaded[table] = temp
        self._conn.db.execute(
            f'CREATE OR REPLACE VIEW "{table}" AS SELECT * FROM {temp}'
        )

    def _restore_view(self, table: str):
        uri = self._conn.config.table_uri(table)
        self._conn.db.execute(
            f'CREATE OR REPLACE VIEW "{table}" AS SELECT * FROM read_parquet(\'{uri}\')'
        )

    def _etag(self, table: str) -> str:
        uri = self._conn.config.table_uri(table)
        bucket = self._conn.config.bucket
        key = uri[len(f"s3://{bucket}/"):]
        try:
            resp = self._conn.registry.s3_client.head_object(Bucket=bucket, Key=key)
            return resp["ETag"]
        except Exception as exc:
            raise OperationalError(f"Cannot read ETag for '{table}': {exc}") from exc

    @staticmethod
    def _extract_table(sql: str) -> str:
        m = _TABLE_RE.search(sql)
        if not m:
            raise ProgrammingError(f"Cannot extract table name from: {sql}")
        return m.group(1)
