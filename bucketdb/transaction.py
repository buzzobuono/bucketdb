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
        # None = full load (all files); list = partial load (only these files loaded into temp)
        self._loaded_files: dict[str, list[FileMeta] | None] = {}

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
            self._loaded_files.pop(table, None)
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
        else:  # UPDATE or DELETE
            if table not in self._loaded:
                self._load_for_dml(table, sql)
            elif table in self._insert_only:
                self._upgrade_to_full_load(table)
            elif self._loaded_files.get(table) is not None:
                # Partially loaded — upgrade to full for subsequent DML safety
                self._upgrade_partial_to_full(table)

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
        self._loaded_files.clear()

    def discard(self):
        for table, temp in self._loaded.items():
            self._conn.db.execute(f"DROP TABLE IF EXISTS {temp}")
            self._restore_view(table)
        self._snapshots.clear()
        self._loaded.clear()
        self._modified.clear()
        self._insert_only.clear()
        self._metas.clear()
        self._loaded_files.clear()

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
        self._loaded_files[table] = None  # full load

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

    def _load_for_dml(self, table: str, sql: str):
        """Load for UPDATE/DELETE: partial for partitioned tables when filter is extractable."""
        meta = self._conn.registry.meta(table)
        if meta and meta.partition_by:
            pf = self._extract_partition_filter(sql, meta.partition_by)
            if pf is not None:
                self._load_partitioned_partial(table, pf)
                return
        self._load(table)

    def _load_partitioned_partial(self, table: str, partition_filter: dict[str, str]):
        """Load only partition files matching the filter; keep others on S3."""
        meta = self._read_meta(table)
        self._metas[table] = meta
        self._snapshots[table] = self._meta_etag(table)

        def matches(f: FileMeta) -> bool:
            return all(str(f.partition.get(col, "")) == str(val)
                       for col, val in partition_filter.items())

        matched = [f for f in meta.files if matches(f)]
        kept = [f for f in meta.files if not matches(f)]

        temp = f"_tx_{table}"
        if matched:
            uris = [self._conn.config.file_uri(table, f.path) for f in matched]
            sql_read = build_read_sql(meta, uris)
            self._conn.db.execute(f"CREATE OR REPLACE TABLE {temp} AS {sql_read}")
        else:
            self._conn.db.execute(
                f"CREATE OR REPLACE TABLE {temp} AS {build_empty_sql(meta.schema)}"
            )

        self._loaded[table] = temp
        self._loaded_files[table] = matched  # track which files are in temp

        # View: kept files from S3 UNION ALL temp (matched, will be modified)
        kept_uris = [self._conn.config.file_uri(table, f.path) for f in kept]
        kept_sql = build_read_sql(meta, kept_uris) if kept_uris else None
        if kept_sql:
            view_sql = f"({kept_sql}) UNION ALL SELECT * FROM {temp}"
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
        self._loaded_files[table] = None  # now full

    def _upgrade_partial_to_full(self, table: str):
        """Promote a partial partitioned load to full (needed for second DML on same table)."""
        temp = self._loaded[table]
        meta = self._metas[table]
        already_loaded = self._loaded_files.get(table) or []
        remaining = [f for f in meta.files if f not in already_loaded]

        if remaining:
            uris = [self._conn.config.file_uri(table, f.path) for f in remaining]
            sql_read = build_read_sql(meta, uris)
            self._conn.db.execute(f"INSERT INTO {temp} SELECT * FROM ({sql_read})")

        self._loaded_files[table] = None  # now full
        self._conn.db.execute(f'CREATE OR REPLACE VIEW "{table}" AS SELECT * FROM {temp}')

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
        """Write consolidated data file(s); untouched partition files are preserved."""
        from .writer import _write_parquet_to_s3, _write_partitioned_arrow

        temp = self._loaded[table]
        meta = self._metas[table]
        arrow = self._conn.db.execute(f"SELECT * FROM {temp}").to_arrow_table()

        # Files not loaded into temp are kept as-is (partial partitioned load)
        loaded_files = self._loaded_files.get(table)
        kept_files = (
            [f for f in meta.files if f not in loaded_files]
            if loaded_files is not None
            else []
        )

        if meta.partition_by:
            new_files = (
                _write_partitioned_arrow(self._conn, table, meta.partition_by, arrow, meta.sort_by)
                if len(arrow) > 0 else []
            )
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

        meta.files = kept_files + new_files
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
    def _extract_partition_filter(sql: str, partition_cols: list[str]) -> dict[str, str] | None:
        """Extract equality conditions for partition columns from a WHERE clause.

        Returns a dict of {col: value} if all partition columns have exact equality
        filters, or None if any column is missing or uses a non-equality operator.
        """
        result = {}
        for col in partition_cols:
            # String literal: col = 'value'
            m = re.search(rf"\b{re.escape(col)}\s*=\s*'([^']*)'", sql, re.IGNORECASE)
            if not m:
                # Numeric literal: col = 42 or col = 3.14
                m = re.search(
                    rf"\b{re.escape(col)}\s*=\s*(-?\d+(?:\.\d+)?)\b", sql, re.IGNORECASE
                )
            if not m:
                return None
            result[col] = m.group(1)
        return result

    @staticmethod
    def _extract_table(sql: str) -> str:
        m = _TABLE_RE.search(sql)
        if not m:
            raise ProgrammingError(f"Cannot extract table name from: {sql}")
        return m.group(1)
