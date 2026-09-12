from __future__ import annotations

import io
import re
from typing import TYPE_CHECKING, Any, Sequence

import pyarrow as pa
import pyarrow.parquet as pq

from .exceptions import OperationalError, ProgrammingError
from .meta import (
    TableMeta, FileMeta, new_filename,
    read_meta, write_meta, get_meta_etag,
    build_read_sql, build_empty_sql,
)

if TYPE_CHECKING:
    from .connection import S3QLConnection

_CREATE_TABLE_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[\"']?(\w+)[\"']?\s*\((.+)\)",
    re.IGNORECASE | re.DOTALL,
)
_DROP_TABLE_RE = re.compile(
    r"DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?[\"']?(\w+)[\"']?",
    re.IGNORECASE,
)
_CREATE_INDEX_RE = re.compile(
    r"CREATE\s+INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\s+ON\s+(\w+)\s*\(([^)]+)\)"
    r"(\s+PARTITIONED)?",
    re.IGNORECASE,
)
_DROP_INDEX_RE = re.compile(
    r"DROP\s+INDEX\s+(?:IF\s+EXISTS\s+)?(\w+)",
    re.IGNORECASE,
)


def handle_write(conn: "S3QLConnection", sql: str, parameters: Sequence[Any]) -> int:
    tokens = sql.split()
    keyword = tokens[0].upper()
    second = tokens[1].upper() if len(tokens) > 1 else ""

    if keyword == "CREATE":
        return _create_index(conn, sql) if second == "INDEX" else _create_table(conn, sql)
    if keyword == "DROP":
        return _drop_index(conn, sql) if second == "INDEX" else _drop_table(conn, sql)

    raise ProgrammingError(f"Unsupported write statement: {keyword}")


# ------------------------------------------------------------------
# DDL — tables
# ------------------------------------------------------------------

def _create_table(conn: "S3QLConnection", sql: str) -> int:
    m = _CREATE_TABLE_RE.search(sql)
    if not m:
        raise ProgrammingError(f"Cannot parse CREATE TABLE: {sql}")

    table_name = m.group(1)
    if_not_exists = "IF NOT EXISTS" in sql.upper()

    if conn.registry.exists(table_name):
        if if_not_exists:
            return 0
        raise ProgrammingError(f"Table '{table_name}' already exists")

    conn.db.execute(f'CREATE TABLE _tmp_{table_name} {sql[sql.index("("):]}')
    schema = [[r[0], r[1]] for r in conn.db.execute(f"DESCRIBE _tmp_{table_name}").fetchall()]
    conn.db.execute(f"DROP TABLE _tmp_{table_name}")

    meta = TableMeta(schema=schema)
    key = conn.config.meta_key(table_name)
    write_meta(conn.registry.s3_client, conn.config.bucket, key, meta)
    conn.registry.register_table(table_name, meta)
    return 0


def _drop_table(conn: "S3QLConnection", sql: str) -> int:
    m = _DROP_TABLE_RE.search(sql)
    if not m:
        raise ProgrammingError(f"Cannot parse DROP TABLE: {sql}")

    table_name = m.group(1)
    if_exists = "IF EXISTS" in sql.upper()

    if not conn.registry.exists(table_name):
        if if_exists:
            return 0
        raise ProgrammingError(f"Table '{table_name}' does not exist")

    _delete_table_prefix(conn, table_name)
    conn.registry.unregister(table_name)
    return 0


# ------------------------------------------------------------------
# DDL — indexes
# ------------------------------------------------------------------

def _create_index(conn: "S3QLConnection", sql: str) -> int:
    m = _CREATE_INDEX_RE.search(sql)
    if not m:
        raise ProgrammingError(f"Cannot parse CREATE INDEX: {sql}")

    index_name = m.group(1)
    table_name = m.group(2)
    columns = [c.strip().strip('"') for c in m.group(3).split(",")]
    partitioned = bool(m.group(4))
    if_not_exists = "IF NOT EXISTS" in sql.upper()

    if conn.registry.table_for_index(index_name):
        if if_not_exists:
            return 0
        raise ProgrammingError(f"Index '{index_name}' already exists")

    if not conn.registry.exists(table_name):
        raise ProgrammingError(f"Table '{table_name}' does not exist")

    meta = conn.registry.meta(table_name)
    if meta.index_name:
        raise ProgrammingError(f"Table '{table_name}' already has an index")

    col_names = [row[0] for row in meta.schema]
    for col in columns:
        if col not in col_names:
            raise ProgrammingError(f"Column '{col}' not found in table '{table_name}'")

    if partitioned:
        meta.partition_by = columns
        if meta.files:
            _rewrite_partitioned(conn, table_name, meta)
    else:
        meta.sort_by = columns
        if meta.files:
            _rewrite_sorted(conn, table_name, meta)

    meta.index_name = index_name
    key = conn.config.meta_key(table_name)
    write_meta(conn.registry.s3_client, conn.config.bucket, key, meta)
    conn.registry.register_table(table_name, meta)
    return 0


def _drop_index(conn: "S3QLConnection", sql: str) -> int:
    m = _DROP_INDEX_RE.search(sql)
    if not m:
        raise ProgrammingError(f"Cannot parse DROP INDEX: {sql}")

    index_name = m.group(1)
    if_exists = "IF EXISTS" in sql.upper()

    table_name = conn.registry.table_for_index(index_name)
    if not table_name:
        if if_exists:
            return 0
        raise ProgrammingError(f"Index '{index_name}' does not exist")

    meta = conn.registry.meta(table_name)

    if meta.files:
        # Consolidate all files into one flat file
        uris = [conn.config.file_uri(table_name, f.path) for f in meta.files]
        sql_read = build_read_sql(meta, uris)
        arrow = conn.db.execute(sql_read).to_arrow_table() if sql_read else pa.table({})
        old_files = list(meta.files)

        if len(arrow) > 0:
            filename = new_filename()
            uri = conn.config.data_file_uri(table_name, filename)
            _write_parquet_to_s3(conn, uri, arrow)
            meta.files = [FileMeta(path=f"data/{filename}")]
        else:
            meta.files = []

        for f in old_files:
            if not any(nf.path == f.path for nf in meta.files):
                _delete_object(conn, conn.config.file_uri(table_name, f.path))

    meta.index_name = None
    meta.sort_by = []
    meta.partition_by = []

    key = conn.config.meta_key(table_name)
    write_meta(conn.registry.s3_client, conn.config.bucket, key, meta)
    conn.registry.register_table(table_name, meta)
    return 0


# ------------------------------------------------------------------
# Vacuum
# ------------------------------------------------------------------

def vacuum(conn: "S3QLConnection", table_name: str):
    """Compact all data files into one (or one per partition), applying sort key."""
    if not conn.registry.exists(table_name):
        raise ProgrammingError(f"Table '{table_name}' does not exist")

    meta = conn.registry.meta(table_name)
    if not meta.files:
        return

    uris = [conn.config.file_uri(table_name, f.path) for f in meta.files]
    sql_read = build_read_sql(meta, uris)
    arrow = conn.db.execute(sql_read).to_arrow_table()

    old_files = list(meta.files)

    if meta.partition_by and len(arrow) > 0:
        new_files = _write_partitioned_arrow(conn, table_name, meta.partition_by, arrow, meta.sort_by)
    elif len(arrow) > 0:
        if meta.sort_by:
            arrow = arrow.sort_by([(col, "ascending") for col in meta.sort_by])
        filename = new_filename()
        uri = conn.config.data_file_uri(table_name, filename)
        _write_parquet_to_s3(conn, uri, arrow)
        new_files = [FileMeta(path=f"data/{filename}")]
    else:
        new_files = []

    meta.files = new_files
    key = conn.config.meta_key(table_name)
    write_meta(conn.registry.s3_client, conn.config.bucket, key, meta)

    for f in old_files:
        if not any(nf.path == f.path for nf in new_files):
            try:
                _delete_object(conn, conn.config.file_uri(table_name, f.path))
            except Exception:
                pass  # best-effort; orphans are harmless

    conn.registry.register_table(table_name, meta)


# ------------------------------------------------------------------
# Internal: rewrite helpers (used by CREATE INDEX on non-empty tables)
# ------------------------------------------------------------------

def _rewrite_sorted(conn: "S3QLConnection", table_name: str, meta: TableMeta):
    uris = [conn.config.file_uri(table_name, f.path) for f in meta.files]
    arrow = conn.db.execute(build_read_sql(meta, uris)).to_arrow_table()
    if len(arrow) == 0:
        return
    arrow = arrow.sort_by([(col, "ascending") for col in meta.sort_by])
    old_files = list(meta.files)
    filename = new_filename()
    uri = conn.config.data_file_uri(table_name, filename)
    _write_parquet_to_s3(conn, uri, arrow)
    meta.files = [FileMeta(path=f"data/{filename}")]
    for f in old_files:
        _delete_object(conn, conn.config.file_uri(table_name, f.path))


def _rewrite_partitioned(conn: "S3QLConnection", table_name: str, meta: TableMeta):
    uris = [conn.config.file_uri(table_name, f.path) for f in meta.files]
    arrow = conn.db.execute(build_read_sql(meta, uris)).to_arrow_table()
    if len(arrow) == 0:
        return
    old_files = list(meta.files)
    new_files = _write_partitioned_arrow(conn, table_name, meta.partition_by, arrow, meta.sort_by)
    meta.files = new_files
    for f in old_files:
        _delete_object(conn, conn.config.file_uri(table_name, f.path))


def _write_partitioned_arrow(
    conn: "S3QLConnection",
    table_name: str,
    part_cols: list[str],
    arrow_table: pa.Table,
    sort_by: list[str] | None = None,
) -> list[FileMeta]:
    """Write rows grouped by partition values. Returns list of FileMeta."""
    tmp = "_writer_part_tmp"
    conn.db.execute(f"CREATE OR REPLACE TABLE {tmp} AS SELECT * FROM arrow_table")

    quoted = ", ".join(f'"{c}"' for c in part_cols)
    partitions = conn.db.execute(f"SELECT DISTINCT {quoted} FROM {tmp}").fetchall()
    where_tmpl = " AND ".join(f'"{c}" = ?' for c in part_cols)

    result: list[FileMeta] = []
    for pvals in partitions:
        part_arrow = conn.db.execute(
            f"SELECT * FROM {tmp} WHERE {where_tmpl}", list(pvals)
        ).to_arrow_table()

        sort_cols = [c for c in (sort_by or []) if c not in part_cols]
        if sort_cols and len(part_arrow) > 0:
            part_arrow = part_arrow.sort_by([(c, "ascending") for c in sort_cols])

        filename = new_filename()
        uri = conn.config.data_file_uri(table_name, filename)
        _write_parquet_to_s3(conn, uri, part_arrow)
        part_dict = {col: str(val) for col, val in zip(part_cols, pvals)}
        result.append(FileMeta(path=f"data/{filename}", partition=part_dict))

    conn.db.execute(f"DROP TABLE IF EXISTS {tmp}")
    return result


# ------------------------------------------------------------------
# S3 helpers
# ------------------------------------------------------------------

def _write_parquet_to_s3(conn: "S3QLConnection", uri: str, table: pa.Table):
    bucket, key = _parse_uri(uri)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    try:
        conn.registry.s3_client.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    except Exception as exc:
        raise OperationalError(f"Failed to write Parquet to S3 ({uri}): {exc}") from exc


def _delete_object(conn: "S3QLConnection", uri: str):
    bucket, key = _parse_uri(uri)
    try:
        conn.registry.s3_client.delete_object(Bucket=bucket, Key=key)
    except Exception as exc:
        raise OperationalError(f"Failed to delete S3 object ({uri}): {exc}") from exc


def _delete_table_prefix(conn: "S3QLConnection", table_name: str):
    bucket = conn.config.bucket
    prefix = conn.config.table_prefix(table_name)
    s3 = conn.registry.s3_client
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if objects:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})


def _parse_uri(uri: str) -> tuple[str, str]:
    without_scheme = uri[len("s3://"):]
    bucket, _, key = without_scheme.partition("/")
    return bucket, key
