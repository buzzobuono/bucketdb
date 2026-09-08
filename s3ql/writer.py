from __future__ import annotations

import io
import re
from typing import TYPE_CHECKING, Any, Sequence

import pyarrow as pa
import pyarrow.parquet as pq

from .exceptions import OperationalError, ProgrammingError

if TYPE_CHECKING:
    from .connection import S3QLConnection

# Minimal SQL pattern matchers (DuckDB parses the real SQL; these only
# detect the statement type and extract the table name for dispatch)
_CREATE_TABLE_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[\"']?(\w+)[\"']?\s*\((.+)\)",
    re.IGNORECASE | re.DOTALL,
)
_DROP_TABLE_RE = re.compile(
    r"DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?[\"']?(\w+)[\"']?",
    re.IGNORECASE,
)
_INSERT_RE = re.compile(
    r"INSERT\s+INTO\s+[\"']?(\w+)[\"']?",
    re.IGNORECASE,
)
_UPDATE_RE = re.compile(
    r"UPDATE\s+[\"']?(\w+)[\"']?",
    re.IGNORECASE,
)
_DELETE_RE = re.compile(
    r"DELETE\s+FROM\s+[\"']?(\w+)[\"']?",
    re.IGNORECASE,
)


def handle_write(conn: "S3QLConnection", sql: str, parameters: Sequence[Any]) -> int:
    keyword = sql.split()[0].upper()

    if keyword == "CREATE":
        return _create_table(conn, sql)
    if keyword == "DROP":
        return _drop_table(conn, sql)
    if keyword == "INSERT":
        return _insert(conn, sql, parameters)
    if keyword == "UPDATE":
        return _update(conn, sql, parameters)
    if keyword == "DELETE":
        return _delete(conn, sql, parameters)

    raise ProgrammingError(f"Unsupported write statement: {keyword}")


# ------------------------------------------------------------------
# DDL
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

    # Let DuckDB parse and create an empty in-memory table, then export to S3
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

    uri = conn.config.table_uri(table_name)
    _delete_s3_object(conn, uri)
    conn.registry.unregister(table_name)
    return 0


# ------------------------------------------------------------------
# DML — copy-on-write pattern
# ------------------------------------------------------------------

def _insert(conn: "S3QLConnection", sql: str, parameters: Sequence[Any]) -> int:
    m = _INSERT_RE.search(sql)
    if not m:
        raise ProgrammingError(f"Cannot parse INSERT: {sql}")
    table_name = m.group(1)
    return _copy_on_write(conn, table_name, sql, parameters)


def _update(conn: "S3QLConnection", sql: str, parameters: Sequence[Any]) -> int:
    m = _UPDATE_RE.search(sql)
    if not m:
        raise ProgrammingError(f"Cannot parse UPDATE: {sql}")
    table_name = m.group(1)
    return _copy_on_write(conn, table_name, sql, parameters)


def _delete(conn: "S3QLConnection", sql: str, parameters: Sequence[Any]) -> int:
    m = _DELETE_RE.search(sql)
    if not m:
        raise ProgrammingError(f"Cannot parse DELETE: {sql}")
    table_name = m.group(1)
    return _copy_on_write(conn, table_name, sql, parameters)


def _copy_on_write(
    conn: "S3QLConnection", table_name: str, sql: str, parameters: Sequence[Any]
) -> int:
    if not conn.registry.exists(table_name):
        raise ProgrammingError(f"Table '{table_name}' does not exist")

    uri = conn.config.table_uri(table_name)

    # 1. Load current table data into a real DuckDB table
    conn.db.execute(f'CREATE OR REPLACE TABLE _cow_{table_name} AS SELECT * FROM read_parquet(\'{uri}\');')

    # 2. Apply the DML on the in-memory copy (swap view name for table name)
    dml = _replace_table_ref(sql, table_name, f"_cow_{table_name}")
    if parameters:
        rel = conn.db.execute(dml, parameters)
    else:
        rel = conn.db.execute(dml)

    rows_affected = rel.fetchone()[0]

    # 3. Export back to S3
    arrow_table = conn.db.execute(f"SELECT * FROM _cow_{table_name}").to_arrow_table()
    _write_parquet_to_s3(conn, uri, arrow_table)

    # 4. Refresh the view and clean up
    conn.db.execute(f'CREATE OR REPLACE VIEW "{table_name}" AS SELECT * FROM read_parquet(\'{uri}\');')
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
    # s3://bucket/key
    without_scheme = uri[len("s3://"):]
    bucket, _, key = without_scheme.partition("/")
    return bucket, key


def _replace_table_ref(sql: str, old: str, new: str) -> str:
    # Replace bare table name with the temp table name, word-boundary safe
    return re.sub(rf'\b{re.escape(old)}\b', new, sql, flags=re.IGNORECASE)
