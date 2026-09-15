"""
Esempio manuale (NON un test pytest): crea uno schema con tre tabelle che si
joinano tra loro — negozi, prodotti, vendite — su un bucket S3 reale, carica
un volume di dati considerevole e lancia una query di aggregazione tra le tre.

Indici:
- negozi e prodotti sono tabelle dimensione piccole: `id` è PRIMARY KEY, quindi
  bucketdb le mantiene fisicamente ordinate per id (meta.sort_by).
- vendite è la tabella fatti, quella che cresce con il volume dei dati: ha un
  indice PARTITIONED su `negozio_id`, così i file Parquet sono partizionati
  per negozio e le query filtrate su un singolo negozio_id leggono solo la
  sua partizione invece dell'intera tabella.

Le credenziali e i parametri di connessione si leggono dal file .env nella
root del progetto (URL, ID, SECRET, BUCKET, REGION, PREFIX).

Uso:
    python examples/demo_prodotti_vendite_negozi.py
    python examples/demo_prodotti_vendite_negozi.py --negozi 500 --prodotti 1000 --vendite 500000

NON è idempotente: ogni esecuzione fa INSERT (non upsert), quindi rilanciarlo
duplica i dati. Con --vendite alto il caricamento può richiedere diversi
minuti: bucketdb bufferizza le INSERT in una tabella DuckDB in memoria e le
scrive su S3 in un unico round di scrittura al commit(), ma ogni riga passa
comunque per una singola INSERT DuckDB (vedi examples/benchmark_bulk_insert.py
per la misura dettagliata dei tempi di bulk insert).
"""
from __future__ import annotations

import argparse
import datetime as dt
import random
import time
from pathlib import Path

import bucketdb

ENV_PATH = Path(__file__).resolve().parents[1] / ".env"

CITTA = [
    "Milano", "Torino", "Napoli", "Roma", "Bologna", "Firenze",
    "Bari", "Palermo", "Genova", "Venezia", "Verona", "Catania",
]
CATEGORIE_PRODOTTO = [
    "Caffè", "Cornetto", "Spremuta", "Panino", "Insalata", "Pizza",
    "Gelato", "Tè", "Acqua", "Bibita",
]


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
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--negozi", type=int, default=200, help="numero di negozi da generare")
    parser.add_argument("--prodotti", type=int, default=500, help="numero di prodotti da generare")
    parser.add_argument("--vendite", type=int, default=100_000, help="numero di righe vendite da generare")
    parser.add_argument("--seed", type=int, default=42, help="seed per la generazione dei dati casuali")
    return parser.parse_args()


def create_schema(cur) -> None:
    cur.execute(
        "CREATE TABLE IF NOT EXISTS negozi "
        "(id INTEGER PRIMARY KEY, nome VARCHAR, citta VARCHAR)"
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS prodotti "
        "(id INTEGER PRIMARY KEY, nome VARCHAR, prezzo DOUBLE)"
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS vendite "
        "(id INTEGER, negozio_id INTEGER, prodotto_id INTEGER, quantita INTEGER, data DATE)"
    )
    # Tabella fatti: partizionata per negozio_id, così le query per singolo
    # negozio leggono solo la sua partizione invece di tutta la tabella.
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_vendite_negozio ON vendite (negozio_id) PARTITIONED"
    )


def build_negozi(n: int, rng: random.Random) -> list[tuple]:
    return [(i, f"Negozio {i}", rng.choice(CITTA)) for i in range(1, n + 1)]


def build_prodotti(n: int, rng: random.Random) -> list[tuple]:
    return [
        (i, f"{rng.choice(CATEGORIE_PRODOTTO)} {i}", round(rng.uniform(0.5, 25.0), 2))
        for i in range(1, n + 1)
    ]


def build_vendite(n: int, n_negozi: int, n_prodotti: int, rng: random.Random) -> list[tuple]:
    start = dt.date(2025, 1, 1)
    giorni_totali = 730  # due anni di storico
    records = []
    for i in range(1, n + 1):
        negozio_id = rng.randint(1, n_negozi)
        prodotto_id = rng.randint(1, n_prodotti)
        quantita = rng.randint(1, 30)
        data = start + dt.timedelta(days=rng.randint(0, giorni_totali - 1))
        records.append((i, negozio_id, prodotto_id, quantita, data))
    return records


def seed_data(cur, args: argparse.Namespace) -> None:
    rng = random.Random(args.seed)

    negozi = build_negozi(args.negozi, rng)
    prodotti = build_prodotti(args.prodotti, rng)
    vendite = build_vendite(args.vendite, args.negozi, args.prodotti, rng)

    print(f"Generati: {len(negozi)} negozi, {len(prodotti)} prodotti, {len(vendite)} vendite")

    # Tutte le INSERT restano bufferizzate nella stessa transazione finché
    # non si chiama conn.commit(): un solo round di scrittura su S3 per le
    # tre tabelle, con verifica degli ETag prima di scrivere.
    t0 = time.perf_counter()
    cur.executemany("INSERT INTO negozi VALUES (?, ?, ?)", negozi)
    cur.executemany("INSERT INTO prodotti VALUES (?, ?, ?)", prodotti)
    cur.executemany("INSERT INTO vendite VALUES (?, ?, ?, ?, ?)", vendite)
    t1 = time.perf_counter()
    print(f"INSERT bufferizzate in {t1 - t0:.1f}s")


def run_join_query(cur) -> None:
    cur.execute(
        """
        SELECT n.nome AS negozio,
               p.nome AS prodotto,
               SUM(v.quantita) AS quantita_totale,
               ROUND(SUM(v.quantita * p.prezzo), 2) AS fatturato
        FROM vendite v
        JOIN negozi n ON v.negozio_id = n.id
        JOIN prodotti p ON v.prodotto_id = p.id
        GROUP BY n.nome, p.nome
        ORDER BY fatturato DESC
        LIMIT 20
        """
    )
    print(f"\nTop 20 combinazioni negozio/prodotto per fatturato:")
    print(f"{'Negozio':<18}{'Prodotto':<20}{'Quantità':<10}{'Fatturato':>10}")
    for negozio, prodotto, quantita, fatturato in cur.fetchall():
        print(f"{negozio:<18}{prodotto:<20}{quantita:<10}{fatturato:>10.2f}")


def run_partition_pruning_query(cur) -> None:
    """Dimostra il vantaggio dell'indice PARTITIONED su vendite(negozio_id):
    questa query legge solo la partizione del negozio 1, non l'intera tabella."""
    cur.execute("SELECT COUNT(*), SUM(quantita) FROM vendite WHERE negozio_id = 1")
    count, quantita_totale = cur.fetchone()
    print(f"\nVendite del negozio 1 (letta solo la sua partizione): "
          f"{count} righe, {quantita_totale or 0} unità totali")


def main() -> None:
    args = parse_args()
    env = load_env(ENV_PATH)
    config = build_config(env)

    with bucketdb.connect(**config) as conn:
        cur = conn.cursor()
        create_schema(cur)
        conn.commit()

        seed_data(cur, args)
        t0 = time.perf_counter()
        conn.commit()
        t1 = time.perf_counter()
        print(f"commit (flush su S3) in {t1 - t0:.1f}s")

        run_join_query(cur)
        run_partition_pruning_query(cur)


if __name__ == "__main__":
    main()
