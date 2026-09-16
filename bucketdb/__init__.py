from datetime import date, datetime, time
from decimal import Decimal
from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("bucketdb")
except PackageNotFoundError:
    __version__ = "0.0.0+unknown"

from .connection import S3QLConnection
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

apilevel = "2.0"
threadsafety = 1
paramstyle = "qmark"

# ------------------------------------------------------------------
# PEP 249 type objects
# ------------------------------------------------------------------
STRING   = str
BINARY   = bytes
NUMBER   = Decimal
DATETIME = datetime
ROWID    = int

# ------------------------------------------------------------------
# PEP 249 type constructors
# ------------------------------------------------------------------
Date      = date
Time      = time
Timestamp = datetime
Binary    = bytes

def DateFromTicks(ticks: float) -> date:
    return datetime.fromtimestamp(ticks).date()

def TimeFromTicks(ticks: float) -> time:
    return datetime.fromtimestamp(ticks).time()

def TimestampFromTicks(ticks: float) -> datetime:
    return datetime.fromtimestamp(ticks)


def connect(
    bucket: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    aws_region: str = "us-east-1",
    prefix: str = "",
    endpoint_url: str | None = None,
    debug_http: bool = False,
) -> S3QLConnection:
    from .config import S3Config
    cfg = S3Config(
        bucket=bucket,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_region=aws_region,
        prefix=prefix,
        endpoint_url=endpoint_url,
    )
    return S3QLConnection(cfg, debug_http=debug_http)
