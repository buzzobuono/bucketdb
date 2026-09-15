"""
Esempio manuale (NON un test pytest): misura i tempi di un bulk insert su un
bucket S3 reale.

Crea una tabella `bench_events` con una colonna `type` usata come chiave di
partizionamento (`CREATE INDEX ... PARTITIONED`), genera un record set e lo
distribuisce su N valori di `type` (una parquet per partizione), poi inserisce
tutti i record in un'unica transazione: tutte le INSERT restano bufferizzate
in memoria (bucketdb.transaction.Transaction) finché non si chiama
conn.commit(), che verifica gli ETag e scrive su S3 una sola volta.

Le credenziali si leggono dal file .env nella root del progetto (URL, ID,
SECRET, BUCKET, REGION, PREFIX) — vedi README, sezione "Examples".

Uso:
    python examples/benchmark_bulk_insert.py
    python examples/benchmark_bulk_insert.py --records 50000 --partitions 8
"""
from __future__ import annotations

import argparse
import datetime as dt
import random
import time
from pathlib import Path

import bucketdb

ENV_PATH = Path(__file__).resolve().parents[1] / ".env"

TABLE = "bench_events"


def load_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        raise SystemExit(f"File .env non trovato: {path}")
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def build_config(env: dict[str, str]) -> dict:
    required = ["URL", "ID", "SECRET", "BUCKET"]
    missing = [k for k in required if not env.get(k)]
    if missing:
        raise SystemExit(
            f"Valori mancanti in .env: {', '.join(missing)}. "
            "Compila .env con le credenziali reali prima di lanciare l'esempio."
        )
    return dict(
        bucket=env["BUCKET"],
        aws_access_key_id=env["ID"],
        aws_secret_access_key=env["SECRET"],
        aws_region=env.get("REGION") or "us-east-1",
        prefix=env.get("PREFIX", ""),
        endpoint_url=env["URL"],
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--records", type=int, default=10_000, help="numero totale di righe da inserire")
    parser.add_argument("--partitions", type=int, default=5, help="numero di valori distinti di 'type'")
    return parser.parse_args()


def create_schema(cur) -> None:
    cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
    cur.execute(
        f"CREATE TABLE {TABLE} ("
        "id INTEGER, "
        "type VARCHAR, "
        "payload VARCHAR, "
        "created_at TIMESTAMP"
        ")"
    )
    # Partiziona i file su S3 per valore di 'type' (una parquet per partizione).
    cur.execute(f"CREATE INDEX idx_{TABLE}_type ON {TABLE} (type) PARTITIONED")


def build_records(n_records: int, n_partitions: int) -> list[tuple]:
    types = [f"type_{i}" for i in range(n_partitions)]
    now = dt.datetime(2026, 1, 1)
    records = []
    for i in range(n_records):
        record_type = types[i % n_partitions]  # distribuzione uniforme sulle partizioni
        records.append((i, record_type, f"payload-{i}", now))
    random.Random(42).shuffle(records)  # evita che l'ordine coincida con l'ordine delle partizioni
    return records


def main() -> None:
    args = parse_args()
    env = load_env(ENV_PATH)
    config = build_config(env)

    with bucketdb.connect(**config) as conn:
        cur = conn.cursor()
        create_schema(cur)
        conn.commit()

        t0 = time.perf_counter()
        records = build_records(args.records, args.partitions)
        t1 = time.perf_counter()

        # Tutte le INSERT finiscono nella stessa transazione bufferizzata:
        # niente commit() fino a che i 10000 record non sono stati applicati.
        cur.executemany(f"INSERT INTO {TABLE} VALUES (?, ?, ?, ?)", records)
        t2 = time.perf_counter()

        conn.commit()
        t3 = time.perf_counter()

        gen_s = t1 - t0
        insert_s = t2 - t1
        commit_s = t3 - t2
        total_s = t3 - t0

        print(f"Record totali:      {args.records}")
        print(f"Partizioni ('type'): {args.partitions}")
        print(f"Generazione dati:    {gen_s:.3f}s")
        print(f"executemany (buffer): {insert_s:.3f}s  ({args.records / insert_s:,.0f} rec/s)")
        print(f"commit (flush su S3): {commit_s:.3f}s  ({args.records / commit_s:,.0f} rec/s)")
        print(f"Totale (senza gen):  {(insert_s + commit_s):.3f}s  ({args.records / (insert_s + commit_s):,.0f} rec/s)")
        print(f"Totale complessivo:  {total_s:.3f}s")


if __name__ == "__main__":
    main()
