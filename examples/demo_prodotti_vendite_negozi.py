"""
Esempio manuale (NON un test pytest): crea un piccolo schema con tre tabelle
che si joinano tra loro — negozi, prodotti, vendite — su un bucket S3 reale,
e lancia una query di aggregazione tra le tre.

Le credenziali e i parametri di connessione si leggono dal file .env nella
root del progetto (URL, ID, SECRET, BUCKET, REGION, PREFIX).

Uso:
    python examples/demo_prodotti_vendite_negozi.py
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path

import s3ql

ENV_PATH = Path(__file__).resolve().parents[1] / ".env"


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


def create_schema(cur) -> None:
    cur.execute("CREATE TABLE IF NOT EXISTS negozi (id INTEGER, nome VARCHAR, citta VARCHAR)")
    cur.execute("CREATE TABLE IF NOT EXISTS prodotti (id INTEGER, nome VARCHAR, prezzo DOUBLE)")
    cur.execute(
        "CREATE TABLE IF NOT EXISTS vendite "
        "(id INTEGER, negozio_id INTEGER, prodotto_id INTEGER, quantita INTEGER, data DATE)"
    )


def seed_data(cur) -> None:
    negozi = [
        (1, "Negozio Centro", "Milano"),
        (2, "Negozio Nord", "Torino"),
        (3, "Negozio Sud", "Napoli"),
    ]
    prodotti = [
        (1, "Caffè", 3.50),
        (2, "Cornetto", 1.20),
        (3, "Spremuta", 2.80),
    ]
    vendite = [
        (1, 1, 1, 10, dt.date(2026, 1, 5)),
        (2, 1, 2, 15, dt.date(2026, 1, 5)),
        (3, 2, 1, 7, dt.date(2026, 1, 6)),
        (4, 2, 3, 5, dt.date(2026, 1, 6)),
        (5, 3, 2, 20, dt.date(2026, 1, 7)),
        (6, 3, 3, 8, dt.date(2026, 1, 7)),
    ]

    cur.executemany("INSERT INTO negozi VALUES (?, ?, ?)", negozi)
    cur.executemany("INSERT INTO prodotti VALUES (?, ?, ?)", prodotti)
    cur.executemany("INSERT INTO vendite VALUES (?, ?, ?, ?, ?)", vendite)


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
        """
    )
    print(f"{'Negozio':<18}{'Prodotto':<12}{'Quantità':<10}{'Fatturato':>10}")
    for negozio, prodotto, quantita, fatturato in cur.fetchall():
        print(f"{negozio:<18}{prodotto:<12}{quantita:<10}{fatturato:>10.2f}")


def main() -> None:
    env = load_env(ENV_PATH)
    config = build_config(env)

    with s3ql.connect(**config) as conn:
        cur = conn.cursor()
        create_schema(cur)
        conn.commit()

        seed_data(cur)
        conn.commit()

        run_join_query(cur)


if __name__ == "__main__":
    main()
