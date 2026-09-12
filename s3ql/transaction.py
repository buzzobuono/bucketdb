from __future__ import annotations

import re
from typing import TYPE_CHECKING

from .exceptions import OperationalError, ProgrammingError
from .meta import (
    TableMeta, FileMeta, new_filename,
    read_meta, write_meta, get_meta_etag,
    build_read_sql, build_empty_sql,
)

if TYPE_CHECKING:
    from .connection import S3QLConnection

_TABLE_RE = re.compile(
    r"(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+[\"']?(\w+)[\"']?",
    re.IGNORECASE,
)


class Transaction:
    def __init__(self, conn: "S3QLConnection"):
        self._conn = conn
        self._snapshots: dict[str, str] = {}    # table → ETag of _meta.json at load time
        self._loaded: dict[str, str] = {}       # table → DuckDB temp table name
        self._modified: set[str] = set()        # tables with pending DML
        self._insert_only: set[str] = set()     # tables with INSERT only (no UPDATE/DELETE)
        self._metas: dict[str, TableMeta] = {}  # cached meta per loaded table

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
            self._metas.pop(table, None)
            self._conn.db.execute(f"DROP TABLE IF EXISTS {temp}")
            self._restore_view(table)

    def apply(self, sql: str, params: list) -> int:
        table = self._extract_table(sql)
        keyword = sql.strip().split()[0].upper()

        if not self._conn.registry.exists(table):
            raise ProgrammingError(f"Table '{table}' does not exist")

        if keyword == "INSERT":
            if table not in self._loaded:
                self._load_insert_only(table)
            # If already fully loaded, apply normally (insert_only flag already off)
        else:  # UPDATE or DELETE
            if table not in self._loaded:
                self._load(table)
            elif table in self._insert_only:
                self._upgrade_to_full_load(table)

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
            key = self._conn.config.meta_key(table)
            current = get_meta_etag(self._conn.registry.s3_client, self._conn.config.bucket, key)
            if current != self._snapshots[table]:
                raise OperationalError(
                    f"Commit conflict: table '{table}' was modified by another writer"
                )

        # Phase 2: write
        for table in self._modified:
            if table in self._insert_only:
                self._flush_insert_only(table)
            else:
                self._flush_full(table)

        # Phase 3: drop temp tables and restore views
        for table, temp in self._loaded.items():
            self._conn.db.execute(f"DROP TABLE IF EXISTS {temp}")
            self._restore_view(table)

        self._snapshots.clear()
        self._loaded.clear()
        self._modified.clear()
        self._insert_only.clear()
        self._metas.clear()

    def discard(self):
        for table, temp in self._loaded.items():
            self._conn.db.execute(f"DROP TABLE IF EXISTS {temp}")
            self._restore_view(table)
        self._snapshots.clear()
        self._loaded.clear()
        self._modified.clear()
        self._insert_only.clear()
        self._metas.clear()

    @property
    def preloaded(self) -> list[str]:
        return [t for t in self._loaded if t not in self._modified]

    @property
    def modified(self) -> list[str]:
        return list(self._modified)

    # ------------------------------------------------------------------
    # Load helpers
    # ------------------------------------------------------------------

    def _load(self, table: str):
        """Full load: reads all existing data files into a DuckDB temp table."""
        meta = self._read_meta(table)
        self._metas[table] = meta
        self._snapshots[table] = self._meta_etag(table)
        self._insert_only.discard(table)

        temp = f"_tx_{table}"
        files_sql = self._files_sql(meta, table)
        if files_sql:
            self._conn.db.execute(f"CREATE OR REPLACE TABLE {temp} AS {files_sql}")
        else:
            self._conn.db.execute(
                f"CREATE OR REPLACE TABLE {temp} AS {build_empty_sql(meta.schema)}"
            )
        self._loaded[table] = temp
        self._conn.db.execute(f'CREATE OR REPLACE VIEW "{table}" AS SELECT * FROM {temp}')

    def _load_insert_only(self, table: str):
        """Lightweight load: only schema, no data. Used for INSERT-only transactions."""
        meta = self._read_meta(table)
        self._metas[table] = meta
        self._snapshots[table] = self._meta_etag(table)
        self._insert_only.add(table)

        temp = f"_tx_{table}"
        # Empty buffer with correct schema
        self._conn.db.execute(
            f"CREATE OR REPLACE TABLE {temp} AS {build_empty_sql(meta.schema)}"
        )
        self._loaded[table] = temp

        # View: existing files UNION ALL new buffer (so SELECT sees all data)
        existing_sql = self._files_sql(meta, table)
        if existing_sql:
            view_sql = f"({existing_sql}) UNION ALL SELECT * FROM {temp}"
        else:
            view_sql = f"SELECT * FROM {temp}"
        self._conn.db.execute(f'CREATE OR REPLACE VIEW "{table}" AS {view_sql}')

    def _upgrade_to_full_load(self, table: str):
        """Promote an insert-only buffer to a full load (needed for UPDATE/DELETE)."""
        temp = self._loaded[table]
        meta = self._metas[table]

        staging = f"_staging_{table}"
        existing_sql = self._files_sql(meta, table)
        if existing_sql:
            self._conn.db.execute(f"CREATE OR REPLACE TABLE {staging} AS {existing_sql}")
        else:
            self._conn.db.execute(
                f"CREATE OR REPLACE TABLE {staging} AS {build_empty_sql(meta.schema)}"
            )
        # Merge buffered inserts into the full dataset
        self._conn.db.execute(f"INSERT INTO {staging} SELECT * FROM {temp}")
        self._conn.db.execute(f"DROP TABLE IF EXISTS {temp}")
        self._conn.db.execute(f"ALTER TABLE {staging} RENAME TO {temp}")

        self._conn.db.execute(f'CREATE OR REPLACE VIEW "{table}" AS SELECT * FROM {temp}')
        self._insert_only.discard(table)

    # ------------------------------------------------------------------
    # Flush helpers
    # ------------------------------------------------------------------

    def _flush_insert_only(self, table: str):
        """Write only the new rows as a new data file; append to meta file list."""
        from .writer import _write_parquet_to_s3, _write_partitioned_arrow

        temp = self._loaded[table]
        meta = self._metas[table]
        arrow = self._conn.db.execute(f"SELECT * FROM {temp}").to_arrow_table()

        if len(arrow) == 0:
            return

        if meta.partition_by:
            new_files = _write_partitioned_arrow(
                self._conn, table, meta.partition_by, arrow, meta.sort_by
            )
            meta.files.extend(new_files)
        else:
            if meta.sort_by:
                arrow = arrow.sort_by([(col, "ascending") for col in meta.sort_by])
            filename = new_filename()
            uri = self._conn.config.data_file_uri(table, filename)
            _write_parquet_to_s3(self._conn, uri, arrow)
            meta.files.append(FileMeta(path=f"data/{filename}"))

        self._write_meta(table, meta)

    def _flush_full(self, table: str):
        """Write consolidated data file(s); old files become orphans for vacuum."""
        from .writer import _write_parquet_to_s3, _write_partitioned_arrow

        temp = self._loaded[table]
        meta = self._metas[table]
        arrow = self._conn.db.execute(f"SELECT * FROM {temp}").to_arrow_table()

        if meta.partition_by:
            new_files = _write_partitioned_arrow(
                self._conn, table, meta.partition_by, arrow, meta.sort_by
            ) if len(arrow) > 0 else []
        else:
            if meta.sort_by and len(arrow) > 0:
                arrow = arrow.sort_by([(col, "ascending") for col in meta.sort_by])
            if len(arrow) > 0:
                filename = new_filename()
                uri = self._conn.config.data_file_uri(table, filename)
                _write_parquet_to_s3(self._conn, uri, arrow)
                new_files = [FileMeta(path=f"data/{filename}")]
            else:
                new_files = []

        meta.files = new_files  # old files are now orphans
        self._write_meta(table, meta)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _read_meta(self, table: str) -> TableMeta:
        key = self._conn.config.meta_key(table)
        return read_meta(self._conn.registry.s3_client, self._conn.config.bucket, key)

    def _meta_etag(self, table: str) -> str:
        key = self._conn.config.meta_key(table)
        return get_meta_etag(self._conn.registry.s3_client, self._conn.config.bucket, key)

    def _write_meta(self, table: str, meta: TableMeta):
        key = self._conn.config.meta_key(table)
        write_meta(self._conn.registry.s3_client, self._conn.config.bucket, key, meta)
        self._conn.registry.register_table(table, meta)

    def _files_sql(self, meta: TableMeta, table: str) -> str | None:
        uris = [self._conn.config.file_uri(table, f.path) for f in meta.files]
        return build_read_sql(meta, uris)

    def _restore_view(self, table: str):
        meta = self._metas.get(table) or self._conn.registry.meta(table)
        if meta:
            self._conn.registry.register_table(table, meta)

    @staticmethod
    def _extract_table(sql: str) -> str:
        m = _TABLE_RE.search(sql)
        if not m:
            raise ProgrammingError(f"Cannot extract table name from: {sql}")
        return m.group(1)
