# s3ql

A PEP 249-compliant Python SQL driver backed by **DuckDB** and **S3**.

Each table is stored as one or more Parquet files on S3. DuckDB is the query engine. The driver exposes a standard DB-API 2.0 interface so it works as a drop-in wherever a Python database driver is expected.

## How it works

```
Your code  →  PEP 249 driver  →  DuckDB (in-memory)  →  S3 (Parquet files)
```

- One **bucket** = one database
- One **table** = a directory on S3 with a `_meta.json` file and one or more Parquet data files
- Tables are discovered automatically at connect time
- DML is **buffered in memory** and written to S3 on `commit()`
- Concurrent writes are detected via **ETag check** on `_meta.json` at commit time

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

cur.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, item VARCHAR, amount DOUBLE)")
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

## How SELECT, INSERT and UPDATE work

Understanding what happens under the hood helps choose the right index and vacuum strategy.

### S3 layout

Every table lives in a directory on S3:

```
s3://bucket/prefix/orders/
  _meta.json              ← schema, index info, list of data files
  data/
    part-a1b2c3.parquet   ← immutable data file
    part-d4e5f6.parquet   ← immutable data file
    ...
```

`_meta.json` is the single source of truth. It lists every data file that belongs to the table, the schema, and any index configuration. Its ETag is the optimistic lock used to detect concurrent writes.

### SELECT

```
SELECT * FROM orders WHERE date > '2024-01-01'
```

1. DuckDB reads the DuckDB view registered for `orders`, which is backed by `read_parquet([file1, file2, ...])`
2. DuckDB opens each Parquet file and reads its **footer** — a compact block at the end of the file containing min/max statistics for every column in every row group (a row group is typically ~128k rows)
3. For each row group, if `max(date) ≤ '2024-01-01'`, DuckDB **skips it entirely** without reading the data — this is called predicate pushdown
4. Only the matching row groups are fetched from S3 via HTTP range requests — not the entire file

This means a 10 GB Parquet file with the right row-group statistics may require downloading only a few MB to answer a query. The more selective the filter and the better the data is physically sorted, the fewer row groups are read.

**Without a sort key** the data is written in insertion order. Row groups have overlapping min/max ranges for every column, so predicate pushdown is ineffective — most queries become full scans.

**With a sort key** the data is physically ordered, so row groups have tight, non-overlapping ranges. A `WHERE date = '2024-03-15'` query with 10 M rows and 80 row groups reads 1–2 row groups instead of 80.

### INSERT

```python
cur.execute("INSERT INTO orders VALUES (...)")
conn.commit()
```

1. During the transaction, new rows are buffered in a DuckDB in-memory table. **No existing data is loaded from S3** — only the schema is read from `_meta.json`
2. The DuckDB view for `orders` is updated to a `UNION ALL` of the existing S3 files and the in-memory buffer, so `SELECT` during the transaction sees all data correctly
3. At `commit()`: the driver reads the ETag of `_meta.json`, writes the new rows as a new immutable Parquet file (`data/part-{uuid}.parquet`), verifies the ETag is unchanged, then updates `_meta.json` to include the new file
4. If a concurrent writer changed `_meta.json` between step 3's ETag read and write, `OperationalError` is raised and the transaction remains active for rollback

The existing data files are never touched. Each commit appends a small file. Over time, many small files accumulate — use `vacuum()` to consolidate them.

### UPDATE and DELETE

```python
cur.execute("UPDATE orders SET amount = 0 WHERE id = 1")
conn.commit()
```

UPDATE and DELETE require identifying specific rows, which means all existing data must be loaded:

1. All data files listed in `_meta.json` are loaded into a DuckDB in-memory table
2. The DML is applied in memory
3. At `commit()`: the result is written as a **single new consolidated file**, the ETag of `_meta.json` is verified, then `_meta.json` is updated to reference only the new file
4. Old files are no longer referenced — they become orphans on S3, cleaned up by `vacuum()`

UPDATE and DELETE on large tables are therefore expensive: they read and rewrite the full dataset. Design schemas to minimise update-heavy workloads on large tables, or use `preload()` to amortise the load cost across multiple operations in the same transaction.

### Vacuum

As INSERT commits accumulate, the table grows from one file to many small files. `vacuum()` consolidates them:

```python
conn.vacuum("orders")
```

1. All data files are read and merged in memory
2. If a sort key is set, the merged data is sorted
3. The result is written as a single new file (or one file per partition for partitioned tables)
4. `_meta.json` is updated; old files are deleted from S3

Run vacuum during low-traffic windows. There is no auto-vacuum — call it explicitly when the file count in `_meta.json` grows large.

---

## Transactions

Writes are buffered in memory until `commit()` is called. At commit time the driver checks the ETag of `_meta.json` for each modified table. If the file was changed by a concurrent writer the commit raises `OperationalError` and the transaction remains active so the caller can roll back.

```python
try:
    cur.execute("UPDATE orders SET amount = 0.0 WHERE id = 1")
    conn.commit()
except s3ql.OperationalError:
    conn.rollback()  # discard in-memory buffer, no S3 write
```

**Known limitations:**

- DDL (`CREATE TABLE`, `DROP TABLE`) is immediate and not transactional
- ETags for every dirty table are verified before any table is written, so a conflict on one table blocks the whole commit — no table is left partially written. There is still no cross-object atomicity on S3 itself
- `rollback()` has no effect on DDL statements
- Two concurrent INSERT transactions where neither finds an existing `_meta.json` (first INSERT ever on a new table) have a small race window — the second writer may overwrite the first silently. This edge case requires S3 conditional writes to be fully eliminated; document your tables as single-writer at creation time if needed

---

## Indexes

Indexes control the physical layout of data files and the sort order within them. Each table supports one index. An index is persisted in `_meta.json` — no separate index files exist.

### Sort key

```sql
CREATE INDEX idx_date ON orders (date)
CREATE INDEX idx ON orders (region, date)   -- compound: sort by region first, then date
```

**What it optimises:** SELECT predicate pushdown. Data is written sorted by the index columns at commit time (for UPDATE/DELETE) or at vacuum time (for INSERT). DuckDB row-group statistics become tight and non-overlapping, so filters on the leading column(s) skip most row groups without reading them.

**Analogy:** a clustered index in a traditional database. The physical order of rows on disk (here: in the Parquet file) matches the index order.

The column order matters: `WHERE region='IT' AND date>'2024-01-01'` prunes well on a `(region, date)` sort key. `WHERE date>'2024-01-01'` alone does not prune on the `region` level.

### Partitioned index

```sql
CREATE INDEX idx_region ON orders (region) PARTITIONED
CREATE INDEX idx ON orders (region, year) PARTITIONED   -- two-level partition
```

**What it optimises:** file-level pruning on SELECT, and write locality on INSERT (rows for the same partition go to the same file). Each distinct combination of partition column values is stored in a separate Parquet file. A query with `WHERE region='IT'` reads only the files whose metadata records `region=IT` — files for other regions are never opened.

**Use for:** low-cardinality columns (region, status, year, category). High-cardinality columns (id, timestamp, email) produce one file per value — catastrophic for both S3 costs and query performance.

### Primary key → automatic sort key

```sql
CREATE TABLE orders (id INTEGER PRIMARY KEY, item VARCHAR, amount DOUBLE)
```

A `PRIMARY KEY` declaration automatically sets the sort key to the PK columns. No separate `CREATE INDEX` is needed. Lookup queries `WHERE id = 42` benefit immediately from row-group predicate pushdown after the first vacuum.

Note: uniqueness is enforced by DuckDB within a single transaction but **not across commits**. Two separate commits can insert the same `id` value. If uniqueness across commits is required, enforce it at the application layer.

### DDL reference

```sql
-- Sort key
CREATE INDEX idx_name ON table (col1, col2)
CREATE INDEX IF NOT EXISTS idx_name ON table (col)

-- Partitioned
CREATE INDEX idx_name ON table (col1) PARTITIONED
CREATE INDEX idx_name ON table (col1, col2) PARTITIONED

-- Drop (merges all files back into one flat file for partitioned tables)
DROP INDEX idx_name
DROP INDEX IF EXISTS idx_name
```

`DROP TABLE` on an indexed table removes all data files and `_meta.json`. `DROP INDEX` on a partitioned table consolidates all partition files into a single flat file.

### Limitations

- One index per table
- `CREATE INDEX` on a non-empty table rewrites the data immediately (outside the transaction buffer) — run during low-traffic windows
- Partition values containing `/` or `=` are not supported
- Partition pruning is metadata-driven (file list filtered before passing to DuckDB), not Hive-style glob — file-level pruning works correctly but DuckDB cannot do further intra-file pruning based on the partition column alone
- Range partitioning (e.g. partition by month from a daily timestamp) is not yet supported — add a derived column and partition on that

---

## In-memory cache: preload and unload

For read-heavy workloads, tables can be explicitly loaded into memory to avoid repeated S3 round trips. Once preloaded, every SELECT on that table reads from memory — zero HTTP requests to S3.

```python
conn.preload("orders", "customers")   # load into memory once
cur.execute("SELECT ...")             # → memory
cur.execute("SELECT ...")             # → memory
conn.close()                          # memory freed, nothing written to S3
```

Tables not preloaded retain the default DuckDB behaviour: predicate pushdown and column projection directly against S3, with HTTP range requests fetching only the needed row groups and columns.

Both patterns can coexist in the same session:

```python
conn.preload("customers")             # lookup table — many reads, load once
cur.execute("INSERT INTO orders ...")  # orders: only new rows buffered, no full load
conn.commit()
```

To release a preloaded table and restore S3 pushdown:

```python
conn.unload("orders")   # drops temp table, view points back to S3
```

`unload` raises `ProgrammingError` if the table has uncommitted changes.

**No local writes** — preloaded data lives exclusively in DuckDB's in-memory buffer. `SET temp_directory=''` is set at connect time so DuckDB raises `OutOfMemoryError` rather than spilling to disk.

---

## Supported SQL

Anything DuckDB understands — window functions, CTEs, aggregates, joins across tables in the same bucket.

```sql
SELECT o.item, SUM(o.amount) AS total
FROM orders o
JOIN customers c ON o.customer_id = c.id
GROUP BY o.item
HAVING total > 100
```

---

## Command line

`pip install -e .` registers an `s3ql` command: an interactive SQL shell (arrow-key line editing, persistent history in `~/.s3ql_history`) or a one-shot query runner.

### Connection parameters

Resolved per-parameter in this order — each parameter is independent, so mixed configurations are valid:

1. **CLI flags** — `--bucket`, `--id`, `--secret`, `--region`, `--prefix`, `--endpoint-url`
2. **Environment variables** — `BUCKET`, `ID`, `SECRET`, `REGION`, `PREFIX`, `URL`
3. **`.env` file** — same keys, looked up in the **current working directory** by default; override with `--env-file`

```bash
s3ql                                              # interactive shell, params from .env
s3ql "SELECT * FROM orders"                       # one-shot query
s3ql ".status"                                    # dot command one-shot
s3ql --bucket other-bucket "SELECT * FROM orders" # override only bucket, rest from .env
BUCKET=test s3ql "SELECT * FROM orders"           # override via env var
```

### Dot commands

| Command | Description |
|---|---|
| `.help` | list all dot commands |
| `.tables` | list tables discovered in the bucket |
| `.schema <table>` | show column names and types |
| `.preload <table> [...]` | load tables into memory |
| `.unload <table> [...]` | release tables from memory |
| `.vacuum <table>` | compact data files, apply sort key |
| `.status` | show bucket, prefix, endpoint, transaction state |
| `.exit` / `.quit` | close the shell |

### Output format

| Statement | Output |
|---|---|
| `SELECT` | aligned table with header and separator |
| `INSERT` / `UPDATE` / `DELETE` | `OK (N righe modificate)` — autocommit applied |
| `CREATE` / `DROP` | `OK` |
| Error | error message printed, shell continues |

---

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

---

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

---

## Development

```bash
pip install -e ".[dev]"
pytest tests/
```

Tests use [moto](https://github.com/getmoto/moto) to mock S3 — no real AWS account needed. Because DuckDB's `httpfs` extension speaks raw HTTP directly to S3 (bypassing boto3/botocore), the fixtures run a real local `moto.server.ThreadedMotoServer` rather than the `@mock_aws` decorator, so both boto3 and DuckDB hit the same mock.
