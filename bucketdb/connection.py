import datetime
import re
import sys
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


def _describe_boto3_extent(op_name: str, http_response, parsed: dict, body_len: int | None) -> str:
    """Same idea as _describe_extent(), but for a boto3 S3 call — bucketdb's
    own writes (writer.py) and metadata reads/ETag checks (transaction.py),
    none of which go through DuckDB's httpfs."""
    if op_name == "HeadObject":
        return "metadata"
    if op_name == "GetObject":
        length = http_response.headers.get("Content-Length")
        return f"full ({length}B)" if length else "?"
    if op_name == "PutObject":
        return f"write ({body_len}B)" if body_len is not None else "write"
    if op_name == "DeleteObject":
        return "delete"
    if op_name == "DeleteObjects":
        n = len((parsed or {}).get("Deleted", []))
        return f"delete ({n})"
    if op_name == "ListObjectsV2":
        return f"list ({(parsed or {}).get('KeyCount', '?')})"
    return "?"


def _emit_s3_log_line(time_display: str, method: str, status_display: str,
                       duration_ms: int | None, extent: str, url: str):
    duration_display = f"{duration_ms}ms" if duration_ms is not None else "?"
    text = (
        f"[s3] {time_display}  {method:<5} {status_display:>3}  {duration_display:>6}  "
        f"{extent:<22}  {url}"
    )
    if sys.stdout.isatty():
        text = f"{_GRAY}{text}{_RESET}"
    print(text)


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
            self._enable_boto3_logging()
        self._registry.discover()
        if debug_http:
            self._flush_http_log()

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
                self._flush_http_log()

    def rollback(self):
        self._assert_open()
        try:
            if self._tx:
                self._tx.discard()
                self._tx = None
        finally:
            if self._debug_http:
                self._flush_http_log()

    def close(self):
        if not self._closed:
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
        try:
            self._get_or_begin_tx().preload(*tables)
        finally:
            if self._debug_http:
                self._flush_http_log()

    def unload(self, *tables: str):
        self._assert_open()
        if self._tx is None:
            raise InterfaceError("No active transaction — nothing to unload")
        try:
            self._tx.unload(*tables)
            if not self._tx._loaded:
                self._tx = None
        finally:
            if self._debug_http:
                self._flush_http_log()

    def vacuum(self, table_name: str):
        """Compact all data files for a table into one, applying sort key if set."""
        self._assert_open()
        from .writer import vacuum as do_vacuum
        try:
            do_vacuum(self, table_name)
        finally:
            if self._debug_http:
                self._flush_http_log()

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
        """Turn on DuckDB's structured HTTP logging, read back via SQL.

        DuckDB keeps log entries in its own in-memory, queryable table
        (`duckdb_logs()`) rather than only being able to write to a file or
        to stdout. Because a DuckDB connection executes one statement at a
        time on a single thread, the log write for an HTTP call happens in
        the same execution context as the call itself — by the time
        db.execute() returns from a statement, every entry that statement
        produced is already committed to that table. Reading it right after
        (see _flush_http_log) is therefore exact: no polling, no background
        thread, no file, and no possible lag — unlike an earlier version of
        this that tailed a log file DuckDB wrote to asynchronously, which
        could and did occasionally print a call's line late or not at all.

        Only covers reads: writes go through boto3's put_object (see
        writer.py), never through DuckDB.
        """
        try:
            self._db.execute("CALL enable_logging(level='DEBUG');")
            # enabled_log_types must be set *after* enable_logging(), which
            # otherwise resets it back to "all types".
            self._db.execute("SET logging_mode='ENABLE_SELECTED';")
            self._db.execute("SET enabled_log_types='HTTP';")
        except Exception as exc:
            raise OperationalError(f"Failed to enable DuckDB HTTP logging: {exc}") from exc

    def _flush_http_log(self):
        """Print every HTTP request DuckDB has made since the last flush,
        then clear the log table. Call this right after a statement
        completes (see __init__/commit/rollback and cursor.execute's
        finally) so its own S3 calls — and only its own — are printed.

        Casting `timestamp` to VARCHAR in SQL (rather than fetching it as a
        native TIMESTAMPTZ) avoids requiring the optional `pytz` package to
        materialize it on the Python side.
        """
        if not self._debug_http:
            return
        rows = self._db.execute(
            "SELECT timestamp::VARCHAR, message FROM duckdb_logs() "
            "WHERE type = 'HTTP' ORDER BY timestamp"
        ).fetchall()
        if not rows:
            return
        for timestamp, message in rows:
            self._print_http_log_line(timestamp, message)
        self._db.execute("CALL truncate_duckdb_logs();")

    @staticmethod
    def _print_http_log_line(timestamp: str, message: str):
        parsed = _parse_http_log_message(message)
        if not parsed:
            return
        method, url, status, duration, extent = parsed
        time_match = _HTTP_TIMESTAMP_RE.search(timestamp)
        time_display = f"{time_match.group(1)}.{time_match.group(2)}" if time_match else "?"
        status_code = re.search(r"(\d+)$", status) if status else None
        status_display = status_code.group(1) if status_code else (status or "?")
        _emit_s3_log_line(time_display, method, status_display, duration, extent, url)

    # ------------------------------------------------------------------
    # boto3 request logging (writes + metadata reads never seen by DuckDB)
    # ------------------------------------------------------------------

    def _enable_boto3_logging(self):
        """Print every real S3 request bucketdb itself makes via boto3.

        Writes (CREATE/DROP TABLE, INSERT/UPDATE/DELETE flush) and the
        _meta.json reads/ETag checks in transaction.py go straight through
        boto3 — never through DuckDB (see writer.py) — so _flush_http_log()
        never sees them. botocore's before-call/after-call events fire
        synchronously inside the boto3 client method call itself, so unlike
        DuckDB's async file-backed logging there is no separate pipeline to
        race here: the line prints the instant the call returns, always.
        """
        events = self._registry.s3_client.meta.events
        events.register("before-call.s3.*", self._before_boto3_call)
        events.register("after-call.s3.*", self._after_boto3_call)

    @staticmethod
    def _before_boto3_call(params, context, **kwargs):
        context["_bucketdb_start"] = time.monotonic()
        context["_bucketdb_method"] = params.get("method", "?")
        context["_bucketdb_url"] = params.get("url", "?")
        # botocore wraps a PUT's Body in its own BytesIO by this point;
        # .getbuffer().nbytes reads its size without touching the read
        # position, so the actual upload is unaffected.
        body = params.get("body")
        if isinstance(body, (bytes, bytearray)):
            body_len = len(body)
        elif hasattr(body, "getbuffer"):
            body_len = body.getbuffer().nbytes
        else:
            body_len = None
        context["_bucketdb_body_len"] = body_len

    def _after_boto3_call(self, http_response, parsed, model, context, **kwargs):
        if not self._debug_http:
            return
        start = context.get("_bucketdb_start")
        duration_ms = int((time.monotonic() - start) * 1000) if start is not None else None
        method = context.get("_bucketdb_method", "?")
        url = context.get("_bucketdb_url", "?")
        status_display = str(getattr(http_response, "status_code", "?"))
        extent = _describe_boto3_extent(model.name, http_response, parsed, context.get("_bucketdb_body_len"))
        time_display = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
        _emit_s3_log_line(time_display, method, status_display, duration_ms, extent, url)
