"""
Esempio manuale (NON un test pytest): esegue una JOIN tra le tre tabelle
vendite, negozi e prodotti e mostra il dettaglio riga per riga più i totali.

Uso:
    python examples/join_dati.py
"""
from __future__ import annotations

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


def print_rows(columns, rows) -> None:
    widths = [max(len(col), *(len(str(row[i])) for row in rows), 8) if rows else len(col) + 2
              for i, col in enumerate(columns)]
    header = "  ".join(col.ljust(w) for col, w in zip(columns, widths))
    print(header)
    print("-" * len(header))
    for row in rows:
        print("  ".join(str(v).ljust(w) for v, w in zip(row, widths)))


def run_detail_join(cur) -> None:
    cur.execute(
        """
        SELECT v.data,
               n.nome AS negozio,
               n.citta AS citta,
               p.nome AS prodotto,
               v.quantita,
               p.prezzo,
               ROUND(v.quantita * p.prezzo, 2) AS totale_riga
        FROM vendite v
        JOIN negozi n ON v.negozio_id = n.id
        JOIN prodotti p ON v.prodotto_id = p.id
        ORDER BY v.data, n.nome, p.nome
        """
    )
    rows = cur.fetchall()
    columns = [col[0] for col in cur.description]
    print(f"\n=== Dettaglio vendite ({len(rows)} righe) ===")
    print_rows(columns, rows)


def run_aggregate_join(cur) -> None:
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
    rows = cur.fetchall()
    columns = [col[0] for col in cur.description]
    print(f"\n=== Fatturato per negozio/prodotto ===")
    print_rows(columns, rows)


def run_grand_total(cur) -> None:
    cur.execute(
        """
        SELECT ROUND(SUM(v.quantita * p.prezzo), 2) AS fatturato_totale
        FROM vendite v
        JOIN prodotti p ON v.prodotto_id = p.id
        """
    )
    totale = cur.fetchone()[0]
    print(f"\nFatturato totale: {totale}")


def main() -> None:
    env = load_env(ENV_PATH)
    config = build_config(env)

    with s3ql.connect(**config) as conn:
        cur = conn.cursor()
        run_detail_join(cur)
        run_aggregate_join(cur)
        run_grand_total(cur)


if __name__ == "__main__":
    main()
