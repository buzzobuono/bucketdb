# s3ql

A PEP 249-compliant Python SQL driver backed by **DuckDB** and **S3**.

Each S3 object is a table stored as Parquet. DuckDB is the SQL engine. The driver exposes a standard DB-API 2.0 interface so it works as a drop-in wherever a Python database driver is expected.

## How it works

```
Your code  →  PEP 249 driver  →  DuckDB (in-memory)  →  S3 (Parquet files)
```

- One **bucket** = one database
- One **Parquet file** = one table (`s3://bucket/prefix/orders.parquet` → table `orders`)
- Tables are discovered automatically at connect time by listing the S3 prefix
- DML is **buffered in memory** and written to S3 on `commit()`
- Concurrent writes are detected via **ETag check** at commit time

## Requirements

- Python ≥ 3.10
- `duckdb` ≥ 0.10
- `boto3` ≥ 1.34
- `pyarrow` ≥ 15

## Installation

```bash
pip install s3ql
```

## Usage

```python
import s3ql

conn = s3ql.connect(
    bucket="my-bucket",
    aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
    aws_secret_access_key="wJalrXUtnFEMI/K7MDENG",
    aws_region="eu-west-1",
    prefix="warehouse/",       # optional
    endpoint_url="http://...", # optional, for MinIO or S3-compatible backends
)

cur = conn.cursor()

cur.execute("CREATE TABLE orders (id INTEGER, item VARCHAR, amount DOUBLE)")
cur.execute("INSERT INTO orders VALUES (?, ?, ?)", [1, "apple", 9.99])
conn.commit()

cur.execute("SELECT * FROM orders WHERE amount > ?", [5.0])
for row in cur.fetchall():
    print(row)

conn.close()
```

Context manager is supported:

```python
with s3ql.connect(bucket="my-bucket", ...) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM orders")
        print(cur.fetchone())
```

## Transactions

Writes are buffered in memory until `commit()` is called. At commit time the driver checks the ETag of each modified table on S3. If the file was changed by a concurrent writer the commit raises `OperationalError` and the transaction remains active so the caller can roll back.

```python
try:
    cur.execute("UPDATE orders SET amount = 0.0 WHERE id = 1")
    conn.commit()
except s3ql.OperationalError:
    conn.rollback()  # discard in-memory buffer, no S3 write
```

**Known limitations:**

- DDL (`CREATE TABLE`, `DROP TABLE`) is immediate and not transactional
- ETags for every dirty table are verified before any table is written, so a conflict on one table blocks the whole commit — no table is left partially written. There is still no cross-object atomicity on S3 itself: a conflicting write landing on a table *during* the write phase (after its own check passed) is not detected
- `rollback()` has no effect on DDL statements

## Supported SQL

Anything DuckDB understands — window functions, CTEs, aggregates, joins across tables in the same bucket.

```sql
SELECT o.item, SUM(o.amount) AS total
FROM orders o
JOIN customers c ON o.customer_id = c.id
GROUP BY o.item
HAVING total > 100
```

## Command line

`pip install -e .` registers an `s3ql` command: an interactive SQL shell (arrow-key line editing, persistent history in `~/.s3ql_history`) or a one-shot query runner.

Connection parameters are resolved in this order: CLI flags → environment variables → a `.env` file in the current directory (`--env-file` to point elsewhere). All use the same keys: `URL`, `ID`, `SECRET`, `BUCKET`, `REGION`, `PREFIX`.

```bash
s3ql                              # interactive shell
s3ql "SELECT * FROM orders"       # one-shot query
s3ql .tables                      # list discovered tables
s3ql --bucket my-bucket --endpoint-url http://localhost:9000 "SELECT 1"
```

## PEP 249 compliance

| Feature | Status |
|---|---|
| Module attributes (`apilevel`, `threadsafety`, `paramstyle`) | ✓ |
| Exception hierarchy | ✓ |
| `Connection`: `close`, `commit`, `rollback`, `cursor` | ✓ |
| `Cursor`: all mandatory methods | ✓ |
| `description` — 7-item tuples | ✓ |
| `rowcount`, `arraysize` | ✓ |
| `setinputsizes`, `setoutputsize` | ✓ (no-op) |
| Type objects and constructors | ✓ |
| `callproc` | — (no stored procedures in DuckDB) |
| `nextset` | — (single result set per execute) |

## Examples

`examples/` has standalone scripts (not pytest tests — they hit a **real** S3-compatible bucket) that read connection parameters from a `.env` file in the project root (`URL`, `ID`, `SECRET`, `BUCKET`, `REGION`, `PREFIX`):

- `demo_prodotti_vendite_negozi.py` — creates and seeds a small 3-table schema (`negozi`, `prodotti`, `vendite`)
- `mostra_dati.py` — lists every discovered table and prints its contents
- `join_dati.py` — joins the three tables (detail rows, aggregate revenue, grand total)

```bash
python examples/demo_prodotti_vendite_negozi.py
python examples/mostra_dati.py
python examples/join_dati.py
```

## Development

```bash
pip install -e ".[dev]"
pytest tests/
```

Tests use [moto](https://github.com/getmoto/moto) to mock S3 — no real AWS account needed. Because DuckDB's `httpfs` extension speaks raw HTTP directly to S3 (bypassing boto3/botocore), the fixtures run a real local `moto.server.ThreadedMotoServer` rather than the `@mock_aws` decorator, so both boto3 and DuckDB hit the same mock.
