# bucketdb

[![PyPI](https://img.shields.io/pypi/v/bucketdb)](https://pypi.org/project/bucketdb/)

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
pip install bucketdb
```

## Usage

```python
import bucketdb

conn = bucketdb.connect(
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
with bucketdb.connect(bucket="my-bucket", ...) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM orders")
        print(cur.fetchone())
```

## How SELECT, INSERT, UPDATE and DELETE work

Understanding what happens under the hood helps choose the right index strategy.

### S3 layout

Every table lives in a directory on S3:

```
s3://bucket/prefix/orders/
  _meta.json              ← schema, index info, ordered list of data files
  data/
    part-a1b2c3.parquet   ← immutable data file
    part-d4e5f6.parquet   ← immutable data file
    ...
```

`_meta.json` is the single source of truth. It lists every data file that belongs to the table, the schema, and any index configuration. Its ETag is the optimistic lock used to detect concurrent writes. Data files are immutable — writes always produce new files; old files become orphans cleaned up by `vacuum()`.

---

### SELECT

DuckDB reads directly from S3 via HTTP range requests. Each Parquet file has a **footer** containing min/max statistics for every column in every row group (~128 k rows each). DuckDB uses these statistics to skip entire row groups without downloading them — this is called **predicate pushdown**.

| Configuration | S3 reads | Notes |
|---|---|---|
| **No index** | all files, all row groups (full scan) | data in insertion order, overlapping min/max |
| **Sort key** | all files, few row groups | tight non-overlapping min/max on leading column(s); a point query on 10 M rows reads 1–2 row groups instead of 80 |
| **Partitioned** | only files for matching partition values | file-level pruning via `_meta.json`; files for other partitions are never opened |
| **Partitioned + sort key** | only matching partition files, few row groups within them | two-level pruning: file-level first, then row-group within each file |

**Example** — `WHERE region='IT' AND date>'2024-01-01'` on a table partitioned by `region` with sort key `date`:
1. `_meta.json` is read; only the IT partition file is passed to DuckDB
2. DuckDB reads the IT file footer and skips row groups where `max(date) ≤ '2024-01-01'`
3. Only the qualifying row groups are fetched via HTTP range requests

---

### INSERT

New rows are never merged with existing data at write time.

| Configuration | S3 reads at commit | S3 writes at commit | Notes |
|---|---|---|---|
| **No index** | none | 1 new file | rows appended as-is |
| **Sort key** | none | 1 new file | new rows sorted before writing |
| **Partitioned** | none | 1 new file per distinct partition value in the new rows | existing files untouched |
| **Partitioned + sort key** | none | 1 new file per partition, sorted by sort key within each | existing files untouched |

Each commit appends one or more small files. Over time files accumulate — use `vacuum()` to consolidate. During the transaction the view is `existing S3 files UNION ALL in-memory buffer`, so SELECT sees the full picture.

---

### UPDATE and DELETE

UPDATE and DELETE require identifying and modifying specific rows. The amount of data read from S3 depends on how precisely the WHERE clause maps to the physical file layout.

| Configuration | S3 reads | S3 writes | Notes |
|---|---|---|---|
| **No index** | all files (full scan) | 1 new consolidated file | full rewrite; side-effect: compacts all INSERT deltas |
| **Sort key** | all files (full scan) | 1 new consolidated file | sort key helps SELECT but not UPDATE/DELETE reads |
| **Partitioned** — exact filter on all partition columns | only matching partition files | 1 new file per affected partition | unaffected partition files kept as-is on S3 |
| **Partitioned** — no exact partition filter | all files (full scan) | 1 new file per partition | falls back to full load |
| **Partitioned + sort key** — exact filter | only matching partition files | 1 new file per affected partition, sorted | unaffected files kept as-is |
| **Partitioned + sort key** — no exact filter | all files (full scan) | 1 new file per partition, sorted | falls back to full load |

**Partition filter extraction** is done by parsing the WHERE clause for equality conditions (`col = 'value'` or `col = 42`). Range conditions, `IN` lists, or expressions involving partition columns do not trigger partial load — the driver falls back to full load.

If two UPDATE/DELETE statements in the same transaction target different partitions, the driver upgrades to a full load on the second statement to guarantee correctness.

---

### Vacuum

As INSERT commits accumulate, the table grows from one file to many. `vacuum()` consolidates them:

```python
conn.vacuum("orders")
```

1. All data files are read and merged in memory
2. If a sort key is set, the merged data is sorted
3. The result is written as a single new file (or one file per partition for partitioned tables)
4. `_meta.json` is updated; old files are deleted from S3

| Configuration | After vacuum |
|---|---|
| **No index** | 1 file, insertion order |
| **Sort key** | 1 file, sorted by key columns |
| **Partitioned** | 1 file per distinct partition value |
| **Partitioned + sort key** | 1 file per partition, sorted within each |

Run vacuum during low-traffic windows. There is no auto-vacuum — call it explicitly when the file count in `_meta.json` grows large.

---

## Transactions

Writes are buffered in memory until `commit()` is called. At commit time the driver checks the ETag of `_meta.json` for each modified table. If the file was changed by a concurrent writer the commit raises `OperationalError` and the transaction remains active so the caller can roll back.

```python
try:
    cur.execute("UPDATE orders SET amount = 0.0 WHERE id = 1")
    conn.commit()
except bucketdb.OperationalError:
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

**UPDATE/DELETE optimisation:** when the `WHERE` clause contains exact equality filters on all partition columns, the driver loads and rewrites only the matching partition files. Files for other partitions are left untouched on S3.

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
- **Partition filter extraction is limited to simple equality conditions** (`col = 'value'` or `col = 42`) — `IN` lists, `BETWEEN`, `OR`, and compound expressions fall back to a full load of all partition files. This affects both SELECT (file-level pruning) and UPDATE/DELETE (partial load)

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

`pip install -e .` registers an `bucketdb` command: an interactive SQL shell (arrow-key line editing, persistent history in `~/.bucketdb_history`) or a one-shot query runner.

### Connection parameters

Resolved per-parameter in this order — each parameter is independent, so mixed configurations are valid:

1. **CLI flags** — `--bucket`, `--id`, `--secret`, `--region`, `--prefix`, `--endpoint-url`
2. **Environment variables** — `BUCKET`, `ID`, `SECRET`, `REGION`, `PREFIX`, `URL`
3. **`.env` file** — same keys, looked up in the **current working directory** by default; override with `--env-file`

```bash
bucketdb                                              # interactive shell, params from .env
bucketdb "SELECT * FROM orders"                       # one-shot query
bucketdb ".status"                                    # dot command one-shot
bucketdb --bucket other-bucket "SELECT * FROM orders" # override only bucket, rest from .env
BUCKET=test bucketdb "SELECT * FROM orders"           # override via env var
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

---

## Future improvements

- **Richer partition filter extraction** — the current WHERE clause parser recognises only simple equality conditions (`col = value`). Extending it to handle `col IN (...)`, conjunctions (`col1 = v1 AND col2 = v2`), and basic range expressions would allow the driver to prune partition files in a much wider set of real-world queries, both for SELECT (file-level pruning against `_meta.json`) and for UPDATE/DELETE (partial load, avoiding a full table read when only a subset of partitions is affected)
