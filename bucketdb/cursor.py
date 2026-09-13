from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence

from .exceptions import InterfaceError, ProgrammingError

if TYPE_CHECKING:
    from .connection import S3QLConnection


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
