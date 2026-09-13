# bucketdb

PEP 249 (DB-API 2.0) SQL driver backed by DuckDB + S3. Each table is a Parquet
file on S3; DuckDB is the query engine; boto3 talks to S3 for anything DuckDB's
`httpfs` extension doesn't cover (listing, deleting, ETag checks).

## Architecture

- `bucketdb/__init__.py` — `connect()` entry point, PEP 249 module attributes/types
- `bucketdb/config.py` — `S3Config` dataclass (bucket, credentials, prefix, endpoint)
- `bucketdb/connection.py` — `S3QLConnection`; sets up DuckDB's `httpfs` extension for S3 access
- `bucketdb/registry.py` — discovers `.parquet` files on S3 at connect time, maps them to DuckDB views
- `bucketdb/cursor.py` — `S3QLCursor`; dispatches SELECT vs DML (INSERT/UPDATE/DELETE, buffered) vs DDL (CREATE/DROP/ALTER, immediate)
- `bucketdb/transaction.py` — buffers DML in an in-memory DuckDB temp table per dirty table; on `commit()`, verifies the ETag of **every** dirty table before writing **any** of them (no partial commit on conflict). Also exposes `preload(*tables)` / `unload(*tables)` for explicit in-memory caching. Tracks `_loaded` (all tables in memory) and `_modified` (tables with pending DML) separately — `flush()` writes only `_modified`. A table in `_loaded` but not `_modified` was necessarily preloaded explicitly (the only other path into `_loaded` is `apply()`, which always adds to `_modified`).
- `bucketdb/writer.py` — copy-on-write to S3 for DDL (`_create_table`/`_drop_table`); also has dead `_insert`/`_update`/`_delete`/`_copy_on_write` functions unreachable from `cursor.py` (DML always goes through `transaction.py` instead)
- `bucketdb/cli.py` — the `bucketdb` console command (interactive SQL shell + one-shot queries), registered via `[project.scripts]` in `pyproject.toml`

## DuckDB API gotchas (bit us once — pyarrow/duckdb version drift)

The pinned dependency floors (`duckdb>=0.10`) are loose; whatever gets installed today may not behave like 0.10 did:

- `.arrow()` on a DuckDB relation can return a `pyarrow.RecordBatchReader` instead of a `pyarrow.Table` (breaks `pq.write_table`). Use `.to_arrow_table()`.
- `SELECT changes()` no longer exists as a scalar function. Row count comes from `fetchone()` on the DML statement's own result — DuckDB returns `[(n_affected,)]` for INSERT/UPDATE/DELETE.

If tests suddenly fail after a `pip install --upgrade duckdb`, suspect the installed `duckdb`/`pyarrow` API surface first.

## Testing

`moto`'s `@mock_aws` decorator only patches boto3/botocore — it does **not** intercept DuckDB's `httpfs` extension, which speaks raw HTTP directly to S3. Reading through a DuckDB view (`read_parquet('s3://...')`) with decorator-based mocking will silently hit **real AWS** and hang or 403.

`tests/conftest.py` therefore runs a real local `moto.server.ThreadedMotoServer` and points both boto3 and `bucketdb.connect(endpoint_url=...)` at it. Any new test that opens its own connection or boto3 client (rather than using the `conn`/`s3` fixtures) must pass `endpoint_url=moto_server` too, or it will hit real AWS the same way.

```bash
pip install -e ".[dev]"   # moto[s3,server] needs flask — pulled in automatically
pytest tests/
```

## The `bucketdb` CLI

`bucketdb/cli.py` reads connection parameters with priority CLI flags → environment variables → `.env` file in the cwd (same keys: `URL`, `ID`, `SECRET`, `BUCKET`, `REGION`, `PREFIX`). After `pip install -e .`, the `bucketdb` command is on PATH inside the venv.

```bash
bucketdb                          # interactive shell (arrow keys + history in ~/.bucketdb_history)
bucketdb "SELECT * FROM orders"   # one-shot query
bucketdb .tables                  # list discovered tables
```

## Examples (manual, hit real S3 — not pytest)

`examples/*.py` connect to a real bucket using `.env` (see README's "Examples" section). They are intentionally outside `tests/` so `pytest` never touches real infrastructure. `demo_prodotti_vendite_negozi.py` is **not idempotent** — rerunning it re-inserts the same rows (INSERT, not upsert), duplicating data.

## Local `.env`

Gitignored. Keys: `URL` (S3-compatible endpoint, e.g. S3-compatible service), `ID`, `SECRET`, `BUCKET`, `REGION`, `PREFIX`. Real credentials live only in the untracked local file.
