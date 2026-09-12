from __future__ import annotations

import io
import re
from typing import TYPE_CHECKING, Any, Sequence

import pyarrow as pa
import pyarrow.parquet as pq

from .exceptions import OperationalError, ProgrammingError
from .index_store import IndexDef

if TYPE_CHECKING:
    from .connection import S3QLConnection

# Minimal SQL pattern matchers (DuckDB parses the real SQL; these only
# detect the statement type and extract names for dispatch)
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
_INSERT_RE = re.compile(r"INSERT\s+INTO\s+[\"']?(\w+)[\"']?", re.IGNORECASE)
_UPDATE_RE = re.compile(r"UPDATE\s+[\"']?(\w+)[\"']?", re.IGNORECASE)
_DELETE_RE = re.compile(r"DELETE\s+FROM\s+[\"']?(\w+)[\"']?", re.IGNORECASE)


def handle_write(conn: "S3QLConnection", sql: str, parameters: Sequence[Any]) -> int:
    tokens = sql.split()
    keyword = tokens[0].upper()
    second = tokens[1].upper() if len(tokens) > 1 else ""

    if keyword == "CREATE":
        if second == "INDEX":
            return _create_index(conn, sql)
        return _create_table(conn, sql)
    if keyword == "DROP":
        if second == "INDEX":
            return _drop_index(conn, sql)
        return _drop_table(conn, sql)
    if keyword == "INSERT":
        return _insert(conn, sql, parameters)
    if keyword == "UPDATE":
        return _update(conn, sql, parameters)
    if keyword == "DELETE":
        return _delete(conn, sql, parameters)

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
    rel = conn.db.execute(f"SELECT * FROM _tmp_{table_name} LIMIT 0")
    arrow_table = rel.to_arrow_table()
    conn.db.execute(f"DROP TABLE _tmp_{table_name}")

    uri = conn.config.table_uri(table_name)
    _write_parquet_to_s3(conn, uri, arrow_table)
    conn.registry.register(table_name)
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

    if conn.index_store.is_partitioned(table_name):
        _delete_table_prefix(conn, table_name)
    else:
        _delete_s3_object(conn, conn.config.table_uri(table_name))

    conn.index_store.remove_for_table(table_name)
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

    if conn.index_store.get_by_name(index_name):
        if if_not_exists:
            return 0
        raise ProgrammingError(f"Index '{index_name}' already exists")

    if not conn.registry.exists(table_name):
        raise ProgrammingError(f"Table '{table_name}' does not exist")

    if conn.index_store.get_for_table(table_name):
        raise ProgrammingError(f"Table '{table_name}' already has an index")

    for col in columns:
        try:
            conn.db.execute(f'SELECT "{col}" FROM "{table_name}" LIMIT 0')
        except Exception:
            raise ProgrammingError(f"Column '{col}' not found in table '{table_name}'")

    schema = [[row[0], row[1]] for row in conn.db.execute(f'DESCRIBE "{table_name}"').fetchall()]

    if partitioned:
        _convert_to_partitioned(conn, table_name, columns, schema)
        idef = IndexDef(
            name=index_name, table=table_name, columns=columns,
            partitioned=True, schema=schema,
        )
        conn.index_store.add(idef)
        conn.registry.register_partitioned(table_name, columns, schema)
    else:
        _sort_flat_table(conn, table_name, columns)
        idef = IndexDef(
            name=index_name, table=table_name, columns=columns,
            partitioned=False, schema=None,
        )
        conn.index_store.add(idef)

    return 0


def _drop_index(conn: "S3QLConnection", sql: str) -> int:
    m = _DROP_INDEX_RE.search(sql)
    if not m:
        raise ProgrammingError(f"Cannot parse DROP INDEX: {sql}")

    index_name = m.group(1)
    if_exists = "IF EXISTS" in sql.upper()

    idef = conn.index_store.get_by_name(index_name)
    if not idef:
        if if_exists:
            return 0
        raise ProgrammingError(f"Index '{index_name}' does not exist")

    table_name = idef.table

    if idef.partitioned:
        # Flatten all partitions back into a single Parquet file
        n = len(idef.columns)
        glob_uri = conn.config.table_glob_uri(table_name, n)
        try:
            arrow_table = conn.db.execute(
                f"SELECT * FROM read_parquet('{glob_uri}', hive_partitioning=True)"
            ).to_arrow_table()
        except Exception:
            arrow_table = pa.table({})

        _delete_table_prefix(conn, table_name)
        flat_uri = conn.config.table_uri(table_name)
        _write_parquet_to_s3(conn, flat_uri, arrow_table)
        conn.registry.register(table_name)

    conn.index_store.remove_by_name(index_name)
    return 0


# ------------------------------------------------------------------
# Internal: index helpers
# ------------------------------------------------------------------

def _sort_flat_table(conn: "S3QLConnection", table_name: str, columns: list[str]):
    uri = conn.config.table_uri(table_name)
    arrow_table = conn.db.execute(f"SELECT * FROM read_parquet('{uri}')").to_arrow_table()
    if len(arrow_table) > 0:
        arrow_table = arrow_table.sort_by([(col, "ascending") for col in columns])
        _write_parquet_to_s3(conn, uri, arrow_table)


def _convert_to_partitioned(
    conn: "S3QLConnection", table_name: str, part_cols: list[str], schema: list[list[str]]
):
    uri = conn.config.table_uri(table_name)
    arrow_table = conn.db.execute(f"SELECT * FROM read_parquet('{uri}')").to_arrow_table()

    if len(arrow_table) > 0:
        _write_partitioned_arrow(conn, table_name, part_cols, arrow_table)

    _delete_s3_object(conn, uri)


def _write_partitioned_arrow(
    conn: "S3QLConnection", table_name: str, part_cols: list[str], arrow_table: pa.Table
):
    """Distribute rows in arrow_table into per-partition S3 files."""
    import duckdb
    tmp = "_writer_part_tmp"
    conn.db.execute(f"CREATE OR REPLACE TABLE {tmp} AS SELECT * FROM arrow_table")

    quoted_cols = ", ".join(f'"{c}"' for c in part_cols)
    partitions = conn.db.execute(
        f"SELECT DISTINCT {quoted_cols} FROM {tmp}"
    ).fetchall()

    excl_cols = ", ".join(f'"{c}"' for c in part_cols)
    where_parts_tmpl = " AND ".join(f'"{c}" = ?' for c in part_cols)

    for pvals in partitions:
        part_arrow = conn.db.execute(
            f"SELECT * EXCLUDE ({excl_cols}) FROM {tmp} WHERE {where_parts_tmpl}",
            list(pvals),
        ).to_arrow_table()
        key = conn.config.partition_s3_key(table_name, part_cols, pvals)
        uri = f"s3://{conn.config.bucket}/{key}"
        _write_parquet_to_s3(conn, uri, part_arrow)

    conn.db.execute(f"DROP TABLE IF EXISTS {tmp}")


def _delete_table_prefix(conn: "S3QLConnection", table_name: str):
    """Delete all S3 objects under a partitioned table's prefix."""
    bucket = conn.config.bucket
    prefix = conn.config.table_base_prefix(table_name)
    s3 = conn.registry.s3_client
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            s3.delete_object(Bucket=bucket, Key=obj["Key"])


# ------------------------------------------------------------------
# DML — copy-on-write (legacy path, still used by writer dispatch)
# ------------------------------------------------------------------

def _insert(conn: "S3QLConnection", sql: str, parameters: Sequence[Any]) -> int:
    m = _INSERT_RE.search(sql)
    if not m:
        raise ProgrammingError(f"Cannot parse INSERT: {sql}")
    return _copy_on_write(conn, m.group(1), sql, parameters)


def _update(conn: "S3QLConnection", sql: str, parameters: Sequence[Any]) -> int:
    m = _UPDATE_RE.search(sql)
    if not m:
        raise ProgrammingError(f"Cannot parse UPDATE: {sql}")
    return _copy_on_write(conn, m.group(1), sql, parameters)


def _delete(conn: "S3QLConnection", sql: str, parameters: Sequence[Any]) -> int:
    m = _DELETE_RE.search(sql)
    if not m:
        raise ProgrammingError(f"Cannot parse DELETE: {sql}")
    return _copy_on_write(conn, m.group(1), sql, parameters)


def _copy_on_write(
    conn: "S3QLConnection", table_name: str, sql: str, parameters: Sequence[Any]
) -> int:
    if not conn.registry.exists(table_name):
        raise ProgrammingError(f"Table '{table_name}' does not exist")

    uri = conn.config.table_uri(table_name)
    conn.db.execute(
        f'CREATE OR REPLACE TABLE _cow_{table_name} AS SELECT * FROM read_parquet(\'{uri}\');'
    )
    dml = _replace_table_ref(sql, table_name, f"_cow_{table_name}")
    if parameters:
        rel = conn.db.execute(dml, parameters)
    else:
        rel = conn.db.execute(dml)
    rows_affected = rel.fetchone()[0]
    arrow_table = conn.db.execute(f"SELECT * FROM _cow_{table_name}").to_arrow_table()
    _write_parquet_to_s3(conn, uri, arrow_table)
    conn.db.execute(
        f'CREATE OR REPLACE VIEW "{table_name}" AS SELECT * FROM read_parquet(\'{uri}\');'
    )
    conn.db.execute(f"DROP TABLE _cow_{table_name}")
    return rows_affected


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


def _delete_s3_object(conn: "S3QLConnection", uri: str):
    bucket, key = _parse_uri(uri)
    try:
        conn.registry.s3_client.delete_object(Bucket=bucket, Key=key)
    except Exception as exc:
        raise OperationalError(f"Failed to delete S3 object ({uri}): {exc}") from exc


def _parse_uri(uri: str) -> tuple[str, str]:
    without_scheme = uri[len("s3://"):]
    bucket, _, key = without_scheme.partition("/")
    return bucket, key


def _replace_table_ref(sql: str, old: str, new: str) -> str:
    return re.sub(rf'\b{re.escape(old)}\b', new, sql, flags=re.IGNORECASE)
