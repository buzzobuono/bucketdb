import csv
import os
import re
import shutil
import sys
import tempfile
import threading
import time

import duckdb

from .config import S3Config
from .exceptions import (
    Warning,
    Error,
    InterfaceError,
    DatabaseError,
    DataError,
    OperationalError,
    IntegrityError,
    InternalError,
    ProgrammingError,
    NotSupportedError,
)
from .registry import TableRegistry
from .transaction import Transaction

_HTTP_METHOD_RE = re.compile(r"'request':\s*\{'type':\s*(\w+)")
_HTTP_URL_RE = re.compile(r"'url':\s*'([^']*)'")
_HTTP_STATUS_RE = re.compile(r"'response':\s*\{'status':\s*(\w+)")
_HTTP_DURATION_RE = re.compile(r"'duration_ms':\s*(\d+)")
_HTTP_CONTENT_RANGE_RE = re.compile(r"content-range=bytes (\d+)-(\d+)/(\d+)")
_HTTP_CONTENT_LENGTH_RE = re.compile(r"Content-Length=(\d+)")
_HTTP_TIMESTAMP_RE = re.compile(r"\d{4}-\d{2}-\d{2} (\d{2}:\d{2}:\d{2})\.(\d{3})")
_GRAY = "\033[90m"
_RESET = "\033[0m"


def _describe_extent(method: str, message: str) -> str:
    """Whether this request read the whole S3 object or only a byte range.

    A HEAD carries no body, only metadata. A GET's response 'content-range'
    header (bytes START-END/TOTAL) tells us exactly what was read; when
    START==0 and END is the last byte, the range happens to cover the whole
    object even though it was fetched as a Range request.
    """
    if method == "HEAD":
        return "metadata"
    range_match = _HTTP_CONTENT_RANGE_RE.search(message)
    if range_match:
        start, end, total = (int(g) for g in range_match.groups())
        size = end - start + 1
        if start == 0 and end == total - 1:
            return f"full ({size}B)"
        return f"partial {start}-{end}/{total} ({size}B)"
    length_match = _HTTP_CONTENT_LENGTH_RE.search(message)
    if length_match:
        return f"full ({length_match.group(1)}B)"
    return "?"


def _parse_http_log_message(message: str) -> tuple[str, str, str | None, int | None, str] | None:
    """Parse one DuckDB HTTP-logger message (a Python-repr-like dict) into
    (method, url, status, duration_ms, extent), or None if it doesn't look
    like one. See _describe_extent() for what 'extent' means."""
    method = _HTTP_METHOD_RE.search(message)
    url = _HTTP_URL_RE.search(message)
    if not (method and url):
        return None
    status_match = _HTTP_STATUS_RE.search(message)
    status = status_match.group(1) if status_match else None
    duration_match = _HTTP_DURATION_RE.search(message)
    duration = int(duration_match.group(1)) if duration_match else None
    extent = _describe_extent(method.group(1), message)
    return method.group(1), url.group(1), status, duration, extent


class S3QLConnection:
    # PEP 249 requires exceptions accessible on the connection object
    Warning = Warning
    Error = Error
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    DataError = DataError
    OperationalError = OperationalError
    IntegrityError = IntegrityError
    InternalError = InternalError
    ProgrammingError = ProgrammingError
    NotSupportedError = NotSupportedError

    def __init__(self, config: S3Config, debug_http: bool = False):
        self._config = config
        self._closed = False
        self._debug_http = debug_http
        self._db = duckdb.connect(database=":memory:")
        self._registry = TableRegistry(config, self._db)
        self._tx: Transaction | None = None
        self._setup_duckdb()
        if debug_http:
            self._enable_http_logging()
        self._registry.discover()
        if debug_http:
            self.drain_http_log()

    # ------------------------------------------------------------------
    # PEP 249 interface
    # ------------------------------------------------------------------

    def cursor(self):
        self._assert_open()
        from .cursor import S3QLCursor
        return S3QLCursor(self)

    def commit(self):
        self._assert_open()
        try:
            if self._tx:
                self._tx.flush()
                self._tx = None
        finally:
            if self._debug_http:
                self.drain_http_log()

    def rollback(self):
        self._assert_open()
        try:
            if self._tx:
                self._tx.discard()
                self._tx = None
        finally:
            if self._debug_http:
                self.drain_http_log()

    def close(self):
        if not self._closed:
            if self._debug_http:
                self._stop_http_logging()
            self._db.close()
            self._closed = True

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @property
    def db(self) -> duckdb.DuckDBPyConnection:
        return self._db

    @property
    def config(self) -> S3Config:
        return self._config

    @property
    def registry(self) -> TableRegistry:
        return self._registry

    def preload(self, *tables: str):
        self._assert_open()
        self._get_or_begin_tx().preload(*tables)

    def unload(self, *tables: str):
        self._assert_open()
        if self._tx is None:
            raise InterfaceError("No active transaction — nothing to unload")
        self._tx.unload(*tables)
        if not self._tx._loaded:
            self._tx = None

    def vacuum(self, table_name: str):
        """Compact all data files for a table into one, applying sort key if set."""
        self._assert_open()
        from .writer import vacuum as do_vacuum
        do_vacuum(self, table_name)

    def _get_or_begin_tx(self) -> Transaction:
        if self._tx is None:
            self._tx = Transaction(self)
        return self._tx

    def _assert_open(self):
        if self._closed:
            raise InterfaceError("Connection is closed")

    def _setup_duckdb(self):
        try:
            self._db.execute("SET temp_directory='';")
            self._db.execute("INSTALL httpfs; LOAD httpfs;")
            cfg = self._config
            self._db.execute(f"SET s3_region='{cfg.aws_region}';")
            self._db.execute(f"SET s3_access_key_id='{cfg.aws_access_key_id}';")
            self._db.execute(f"SET s3_secret_access_key='{cfg.aws_secret_access_key}';")
            if cfg.endpoint_url:
                endpoint = cfg.endpoint_url.replace("https://", "").replace("http://", "").rstrip("/")
                use_ssl = cfg.endpoint_url.startswith("https://")
                self._db.execute(f"SET s3_endpoint='{endpoint}';")
                self._db.execute(f"SET s3_use_ssl={'true' if use_ssl else 'false'};")
                self._db.execute("SET s3_url_style='path';")
        except Exception as exc:
            raise OperationalError(f"Failed to initialize DuckDB S3 extension: {exc}") from exc

    def _enable_http_logging(self):
        """Print every real HTTP request DuckDB's httpfs makes to S3.

        DuckDB's logger can only write to its own storage backends, not call
        back into Python, and it can only write to stdout by taking over the
        process's real fd 1 — which also happens to be exactly what GNU
        Readline uses to draw the interactive prompt (arrow-key history,
        line editing). Redirecting fd 1 for the logger breaks Readline, so
        instead the logger writes to a private file and a background thread
        tails it. `drain_http_log()` (called after every statement, see
        cursor.py and commit()/rollback() below) blocks until that thread
        has caught up to the current end of the file, so a statement's own
        S3 calls are always printed before control returns to the caller —
        no lag, no bleeding into the next command's output.

        Only covers reads: writes go through boto3's put_object (see
        writer.py), never through DuckDB.
        """
        try:
            self._http_log_dir = tempfile.mkdtemp(prefix="bucketdb_http_")
            self._db.execute(
                "CALL enable_logging(storage='file', storage_path=?, level='DEBUG');",
                [self._http_log_dir],
            )
            # enabled_log_types must be set *after* enable_logging(), which
            # otherwise resets it back to "all types".
            self._db.execute("SET logging_mode='ENABLE_SELECTED';")
            self._db.execute("SET enabled_log_types='HTTP';")
        except Exception as exc:
            raise OperationalError(f"Failed to enable DuckDB HTTP logging: {exc}") from exc

        self._http_log_bytes_read = 0
        self._http_log_stop = threading.Event()
        self._http_log_thread = threading.Thread(
            target=self._tail_http_log, daemon=True
        )
        self._http_log_thread.start()

    def _tail_http_log(self):
        path = os.path.join(self._http_log_dir, "duckdb_log_entries.csv")
        while not self._http_log_stop.is_set() and not os.path.exists(path):
            time.sleep(0.01)
        if self._http_log_stop.is_set():
            return

        buffer = b""
        with open(path, "rb") as f:
            f.readline()  # header
            while not self._http_log_stop.is_set():
                chunk = f.read()
                if not chunk:
                    time.sleep(0.005)
                    continue
                buffer += chunk
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    self._print_http_log_line(line.decode("utf-8", errors="replace"))
                self._http_log_bytes_read = f.tell() - len(buffer)

    def drain_http_log(self, timeout: float = 1.0):
        """Block until every HTTP call made so far has been printed.

        Call this right after a statement completes so its own S3 calls are
        guaranteed on screen before control returns to the caller (e.g.
        before the CLI shows the next prompt) — without this, the tailing
        thread's small polling lag can otherwise let a line surface after
        the *next* statement has already started.
        """
        if not self._debug_http:
            return
        path = os.path.join(self._http_log_dir, "duckdb_log_entries.csv")
        deadline = time.monotonic() + timeout
        # Two rounds: DuckDB's own file-logging write can trail the SQL call
        # that triggered it by a few ms (its httpfs layer does some of its
        # I/O — and apparently some of its logging — off the calling
        # thread). One extra re-check absorbs that straggler in practice.
        for _ in range(4):
            try:
                target = os.path.getsize(path)
            except OSError:
                return
            while self._http_log_bytes_read < target and time.monotonic() < deadline:
                time.sleep(0.002)
            time.sleep(0.05)

    @staticmethod
    def _print_http_log_line(line: str):
        try:
            row = next(csv.reader([line]))
        except StopIteration:
            return
        if len(row) < 5 or row[2] != "HTTP":
            return
        parsed = _parse_http_log_message(row[4])
        if not parsed:
            return
        method, url, status, duration, extent = parsed
        time_match = _HTTP_TIMESTAMP_RE.search(row[1])
        time_display = f"{time_match.group(1)}.{time_match.group(2)}" if time_match else "?"
        status_code = re.search(r"(\d+)$", status) if status else None
        status_display = status_code.group(1) if status_code else (status or "?")
        duration_display = f"{duration}ms" if duration is not None else "?"
        text = (
            f"[s3] {time_display}  {method:<5} {status_display:>3}  {duration_display:>6}  "
            f"{extent:<22}  {url}"
        )
        if sys.stdout.isatty():
            text = f"{_GRAY}{text}{_RESET}"
        print(text)

    def _stop_http_logging(self):
        self._http_log_stop.set()
        self._http_log_thread.join(timeout=1)
        shutil.rmtree(self._http_log_dir, ignore_errors=True)
