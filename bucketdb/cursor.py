from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, Sequence

from .exceptions import InterfaceError, ProgrammingError
from .meta import extract_partition_filter

if TYPE_CHECKING:
    from .connection import S3QLConnection

_SQL_CLAUSE_KEYWORDS = (
    r"WHERE|GROUP|ORDER|LIMIT|HAVING|JOIN|ON|UNION|INTERSECT|EXCEPT|"
    r"WINDOW|QUALIFY|USING|LEFT|RIGHT|INNER|OUTER|CROSS|FULL|NATURAL|SAMPLE"
)
# Table name after FROM, plus an optional alias (bare or after AS) — the
# lookahead stops a following clause keyword (WHERE, GROUP BY, ...) from
# being swallowed as if it were an alias.
_SELECT_FROM_TABLE_RE = re.compile(
    rf'\bFROM\s+["\']?(\w+)["\']?'
    rf'(?:\s+(?:AS\s+)?(?!(?:{_SQL_CLAUSE_KEYWORDS})\b)["\']?(\w+)["\']?)?',
    re.IGNORECASE,
)


def _try_prune_partitioned_select(conn: "S3QLConnection", sql: str) -> str:
    """Best-effort optimization: if `sql` is a simple single-table SELECT with
    a filter narrowing every column of a PARTITIONED index to a known set of
    values (col = value or col IN (...)), rewrite it to read only the
    matching data files instead of the table's full view (which otherwise
    reads every partition — DuckDB's read_parquet() doesn't prune files by
    path/stats on its own for an explicit file list).

    Falls back to the original SQL untouched whenever anything doesn't match
    this narrow pattern: never changes results, only cost.
    """
    if not re.search(r"\bWHERE\b", sql, re.IGNORECASE):
        return sql
    if re.search(r"\bJOIN\b", sql, re.IGNORECASE):
        return sql
    if len(re.findall(r"\bSELECT\b", sql, re.IGNORECASE)) != 1:
        # A subquery, CTE or UNION branch also contains "SELECT": the first
        # "FROM x" found in the raw text might not be the table this WHERE
        # clause actually filters (e.g. a scalar subquery earlier in the
        # SELECT list), so rewriting it could silently change results
        # instead of just cost. Bail out rather than risk that.
        return sql
    table_match = _SELECT_FROM_TABLE_RE.search(sql)
    if not table_match:
        return sql
    table, alias = table_match.group(1), table_match.group(2)

    tx = conn._tx
    if tx is not None and table in tx._loaded:
        # Table has buffered/uncommitted changes in this transaction — its
        # view no longer maps 1:1 to the committed S3 files, don't bypass it.
        return sql

    meta = conn.registry.meta(table)
    if not meta or not meta.partition_by:
        return sql

    partition_filter = extract_partition_filter(sql, meta.partition_by)
    if partition_filter is None:
        return sql

    pruned_sql = conn.registry.pruned_read_sql(table, partition_filter)
    if pruned_sql is None:
        return sql

    replacement = f'FROM ({pruned_sql}) AS "{alias or table}"'
    return sql[: table_match.start()] + replacement + sql[table_match.end() :]


def _build_description(rel) -> list[tuple] | None:
    """Return PEP 249-compliant description: list of 7-item tuples.

    (name, type_code, display_size, internal_size, precision, scale, null_ok)
    """
    raw = getattr(rel, "description", None)
    if not raw:
        return None
    result = []
    for col in raw:
        if isinstance(col, (list, tuple)) and len(col) >= 7:
            result.append(tuple(col[:7]))
        elif isinstance(col, (list, tuple)):
            padded = tuple(col) + (None,) * (7 - len(col))
            result.append(padded)
        else:
            result.append((str(col), None, None, None, None, None, None))
    return result or None


class S3QLCursor:
    def __init__(self, connection: "S3QLConnection"):
        self._conn = connection
        self._closed = False
        self._result = None
        self._description = None
        self._rowcount = -1
        self.arraysize = 1

    # ------------------------------------------------------------------
    # PEP 249 attributes
    # ------------------------------------------------------------------

    @property
    def description(self):
        return self._description

    @property
    def rowcount(self) -> int:
        return self._rowcount

    # ------------------------------------------------------------------
    # PEP 249 methods
    # ------------------------------------------------------------------

    def execute(self, operation: str, parameters: Sequence[Any] | None = None):
        self._assert_open()
        from .writer import handle_write
        try:
            sql = operation.strip()
            keyword = sql.split()[0].upper() if sql else ""

            if keyword in ("INSERT", "UPDATE", "DELETE"):
                tx = self._conn._get_or_begin_tx()
                self._rowcount = tx.apply(sql, parameters or [])
                self._description = None
                self._result = None
            elif keyword in ("CREATE", "DROP", "ALTER"):
                self._rowcount = handle_write(self._conn, sql, parameters or [])
                self._description = None
                self._result = None
            else:
                if keyword == "SELECT":
                    sql = _try_prune_partitioned_select(self._conn, sql)
                if parameters:
                    rel = self._conn.db.execute(sql, parameters)
                else:
                    rel = self._conn.db.execute(sql)
                self._result = rel.fetchall()
                self._description = _build_description(rel)
                self._rowcount = len(self._result)
        except ProgrammingError:
            self._rowcount = -1
            raise
        except Exception as exc:
            self._rowcount = -1
            raise ProgrammingError(str(exc)) from exc

    def executemany(self, operation: str, seq_of_parameters):
        self._assert_open()
        for params in seq_of_parameters:
            self.execute(operation, params)

    def fetchone(self) -> tuple | None:
        self._assert_open()
        if self._result is None:
            raise ProgrammingError("No query has been executed")
        if not self._result:
            return None
        return self._result.pop(0)

    def fetchmany(self, size: int | None = None) -> list[tuple]:
        self._assert_open()
        if self._result is None:
            raise ProgrammingError("No query has been executed")
        n = size if size is not None else self.arraysize
        chunk, self._result = self._result[:n], self._result[n:]
        return chunk

    def fetchall(self) -> list[tuple]:
        self._assert_open()
        if self._result is None:
            raise ProgrammingError("No query has been executed")
        rows, self._result = self._result, []
        return rows

    def setinputsizes(self, sizes):
        pass  # no-op as permitted by PEP 249

    def setoutputsize(self, size, column=None):
        pass  # no-op as permitted by PEP 249

    def close(self):
        self._closed = True
        self._result = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    # ------------------------------------------------------------------

    def _assert_open(self):
        if self._closed:
            raise InterfaceError("Cursor is closed")
        self._conn._assert_open()
