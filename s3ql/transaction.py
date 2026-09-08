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
        self._snapshots: dict[str, str] = {}   # table → ETag at first touch
        self._dirty: dict[str, str] = {}        # table → DuckDB temp table name

    def apply(self, sql: str, params: list) -> int:
        table = self._extract_table(sql)
        if not self._conn.registry.exists(table):
            raise ProgrammingError(f"Table '{table}' does not exist")
        if table not in self._dirty:
            self._load(table)
        temp = self._dirty[table]
        dml = re.sub(rf'\b{re.escape(table)}\b', temp, sql, flags=re.IGNORECASE)
        if params:
            rel = self._conn.db.execute(dml, params)
        else:
            rel = self._conn.db.execute(dml)
        result = rel.fetchone()
        return result[0] if result else -1

    def flush(self):
        # Phase 1: verify no concurrent modification
        for table, etag in self._snapshots.items():
            current = self._etag(table)
            if current != etag:
                raise OperationalError(
                    f"Commit conflict: table '{table}' was modified by another writer"
                )
        # Phase 2: all checks passed → write to S3
        for table, temp in self._dirty.items():
            from .writer import _write_parquet_to_s3
            uri = self._conn.config.table_uri(table)
            arrow_table = self._conn.db.execute(f"SELECT * FROM {temp}").to_arrow_table()
            _write_parquet_to_s3(self._conn, uri, arrow_table)
            self._restore_view(table)
            self._conn.db.execute(f"DROP TABLE IF EXISTS {temp}")
        self._snapshots.clear()
        self._dirty.clear()

    def discard(self):
        for table, temp in self._dirty.items():
            self._conn.db.execute(f"DROP TABLE IF EXISTS {temp}")
            self._restore_view(table)
        self._snapshots.clear()
        self._dirty.clear()

    # ------------------------------------------------------------------

    def _load(self, table: str):
        uri = self._conn.config.table_uri(table)
        self._snapshots[table] = self._etag(table)
        temp = f"_tx_{table}"
        self._conn.db.execute(
            f"CREATE OR REPLACE TABLE {temp} AS SELECT * FROM read_parquet('{uri}')"
        )
        self._dirty[table] = temp
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
