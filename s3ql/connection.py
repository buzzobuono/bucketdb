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
from .index_store import IndexStore
from .registry import TableRegistry
from .transaction import Transaction


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

    def __init__(self, config: S3Config):
        self._config = config
        self._closed = False
        self._db = duckdb.connect(database=":memory:")
        self._registry = TableRegistry(config, self._db)
        self._index_store = IndexStore(config, self._registry.s3_client)
        self._tx: Transaction | None = None
        self._setup_duckdb()
        self._index_store.load()
        self._registry.discover(self._index_store)

    # ------------------------------------------------------------------
    # PEP 249 interface
    # ------------------------------------------------------------------

    def cursor(self):
        self._assert_open()
        from .cursor import S3QLCursor
        return S3QLCursor(self)

    def commit(self):
        self._assert_open()
        if self._tx:
            self._tx.flush()
            self._tx = None

    def rollback(self):
        self._assert_open()
        if self._tx:
            self._tx.discard()
            self._tx = None

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
    def registry(self) -> "TableRegistry":
        return self._registry

    @property
    def index_store(self) -> "IndexStore":
        return self._index_store

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
                # Strip protocol and trailing slash for DuckDB's endpoint setting
                endpoint = cfg.endpoint_url.replace("https://", "").replace("http://", "").rstrip("/")
                use_ssl = cfg.endpoint_url.startswith("https://")
                self._db.execute(f"SET s3_endpoint='{endpoint}';")
                self._db.execute(f"SET s3_use_ssl={'true' if use_ssl else 'false'};")
                self._db.execute("SET s3_url_style='path';")
        except Exception as exc:
            raise OperationalError(f"Failed to initialize DuckDB S3 extension: {exc}") from exc
