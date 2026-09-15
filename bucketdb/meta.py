from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass, field
from typing import Any

from botocore.exceptions import ClientError

from .exceptions import OperationalError


@dataclass
class FileMeta:
    path: str                          # relative to table prefix, e.g. "data/part-abc.parquet"
    partition: dict[str, Any] = field(default_factory=dict)  # {"region": "IT"}


@dataclass
class TableMeta:
    schema: list[list[str]]            # [[col_name, duckdb_type], ...]
    files: list[FileMeta] = field(default_factory=list)
    partition_by: list[str] = field(default_factory=list)
    sort_by: list[str] = field(default_factory=list)
    index_name: str | None = None


def new_filename() -> str:
    return f"part-{uuid.uuid4().hex[:12]}.parquet"


def read_meta(s3, bucket: str, key: str) -> TableMeta:
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        d = json.loads(resp["Body"].read())
    except ClientError as exc:
        raise OperationalError(f"Cannot read table metadata ({key}): {exc}") from exc
    files = [
        FileMeta(path=f["path"], partition=f.get("partition", {}))
        for f in d.get("files", [])
    ]
    return TableMeta(
        schema=d["schema"],
        files=files,
        partition_by=d.get("partition_by", []),
        sort_by=d.get("sort_by", []),
        index_name=d.get("index_name"),
    )


def write_meta(s3, bucket: str, key: str, meta: TableMeta):
    d: dict[str, Any] = {"schema": meta.schema}
    if meta.partition_by:
        d["partition_by"] = meta.partition_by
    if meta.sort_by:
        d["sort_by"] = meta.sort_by
    if meta.index_name:
        d["index_name"] = meta.index_name
    d["files"] = [
        {"path": f.path, **({"partition": f.partition} if f.partition else {})}
        for f in meta.files
    ]
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(d).encode())
    except Exception as exc:
        raise OperationalError(f"Cannot write table metadata ({key}): {exc}") from exc


def get_meta_etag(s3, bucket: str, key: str) -> str:
    try:
        resp = s3.head_object(Bucket=bucket, Key=key)
        return resp["ETag"]
    except Exception as exc:
        raise OperationalError(f"Cannot read ETag for metadata ({key}): {exc}") from exc


def build_read_sql(meta: TableMeta, file_uris: list[str]) -> str | None:
    """Return a SELECT SQL string reading all file URIs, or None if empty."""
    if not file_uris:
        return None
    if len(file_uris) == 1:
        return f"SELECT * FROM read_parquet('{file_uris[0]}')"
    joined = ", ".join(f"'{u}'" for u in file_uris)
    return f"SELECT * FROM read_parquet([{joined}])"


def build_empty_sql(schema: list[list[str]]) -> str:
    cols = ", ".join(f'NULL::{dtype} AS "{col}"' for col, dtype in schema)
    return f"SELECT {cols} WHERE 1=0"


def extract_partition_filter(sql: str, partition_cols: list[str]) -> dict[str, list[str]] | None:
    """Extract equality/IN conditions for partition columns from a WHERE clause.

    Returns {col: [value, ...]} if every partition column has a filter that
    narrows it to a known, finite set of values (col = value or
    col IN (v1, v2, ...)), or None if any column is missing a filter or uses
    an operator we can't narrow from (e.g. NOT IN, <, LIKE).
    """
    result: dict[str, list[str]] = {}
    for col in partition_cols:
        in_match = re.search(
            rf"\b{re.escape(col)}\s+(NOT\s+)?IN\s*\(([^)]*)\)", sql, re.IGNORECASE
        )
        if in_match:
            if in_match.group(1):
                return None  # NOT IN excludes values, doesn't narrow to a set
            values = [v.strip().strip("'\"") for v in in_match.group(2).split(",")]
            values = [v for v in values if v]
            if not values:
                return None
            result[col] = values
            continue

        # String literal: col = 'value'
        m = re.search(rf"\b{re.escape(col)}\s*=\s*'([^']*)'", sql, re.IGNORECASE)
        if not m:
            # Numeric literal: col = 42 or col = 3.14
            m = re.search(
                rf"\b{re.escape(col)}\s*=\s*(-?\d+(?:\.\d+)?)\b", sql, re.IGNORECASE
            )
        if not m:
            return None
        result[col] = [m.group(1)]
    return result
