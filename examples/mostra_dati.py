"""
Esempio manuale (NON un test pytest): si connette al bucket S3 configurato in
.env e mostra il contenuto delle tabelle scoperte (es. negozi, prodotti,
vendite creati da demo_prodotti_vendite_negozi.py).

Uso:
    python examples/mostra_dati.py
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


def print_table(cur, table_name: str) -> None:
    cur.execute(f'SELECT * FROM "{table_name}"')
    rows = cur.fetchall()
    columns = [col[0] for col in cur.description]

    widths = [max(len(col), *(len(str(row[i])) for row in rows), 8) if rows else len(col) + 2
              for i, col in enumerate(columns)]

    header = "  ".join(col.ljust(w) for col, w in zip(columns, widths))
    print(f"\n=== {table_name} ({len(rows)} righe) ===")
    print(header)
    print("-" * len(header))
    for row in rows:
        print("  ".join(str(v).ljust(w) for v, w in zip(row, widths)))


def main() -> None:
    env = load_env(ENV_PATH)
    config = build_config(env)

    with s3ql.connect(**config) as conn:
        tables = sorted(conn.registry.tables)
        if not tables:
            print("Nessuna tabella trovata nel bucket/prefix configurato.")
            return

        cur = conn.cursor()
        for table_name in tables:
            print_table(cur, table_name)


if __name__ == "__main__":
    main()
