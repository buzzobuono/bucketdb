"""
Editor SQL interattivo (TUI) per s3ql: `s3ql` da riga di comando.

I parametri di connessione si risolvono in quest'ordine di priorità:
1. opzioni da riga di comando (--bucket, --access-key-id, ...)
2. variabili d'ambiente (URL, ID, SECRET, BUCKET, REGION, PREFIX)
3. file .env nella directory corrente (stesse chiavi), o quello indicato con --env-file
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

import s3ql
from s3ql.exceptions import Error

try:
    import readline  # abilita frecce </> e cronologia su/giù in input()
except ImportError:
    readline = None  # non disponibile (es. Windows senza pyreadline3)

HISTORY_PATH = Path.home() / ".s3ql_history"
HISTORY_MAX_LINES = 1000

EXIT_COMMANDS = {".exit", ".quit", "exit", "quit"}
AUTOCOMMIT_KEYWORDS = ("INSERT", "UPDATE", "DELETE")

ENV_KEYS = {
    "bucket": "BUCKET",
    "access_key_id": "ID",
    "secret_access_key": "SECRET",
    "region": "REGION",
    "prefix": "PREFIX",
    "endpoint_url": "URL",
}


def _parse_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def build_config(args: argparse.Namespace) -> dict:
    env_file_values = _parse_env_file(Path(args.env_file))

    def resolve(cli_value: str | None, env_key: str) -> str | None:
        if cli_value:
            return cli_value
        if os.environ.get(env_key):
            return os.environ[env_key]
        return env_file_values.get(env_key)

    bucket = resolve(args.bucket, ENV_KEYS["bucket"])
    access_key_id = resolve(args.access_key_id, ENV_KEYS["access_key_id"])
    secret_access_key = resolve(args.secret_access_key, ENV_KEYS["secret_access_key"])
    region = resolve(args.region, ENV_KEYS["region"]) or "us-east-1"
    prefix = resolve(args.prefix, ENV_KEYS["prefix"]) or ""
    endpoint_url = resolve(args.endpoint_url, ENV_KEYS["endpoint_url"])

    missing = [
        name
        for name, value in [
            ("bucket", bucket),
            ("access-key-id", access_key_id),
            ("secret-access-key", secret_access_key),
        ]
        if not value
    ]
    if missing:
        raise SystemExit(
            f"Parametri di connessione mancanti: {', '.join(missing)}. "
            f"Passali come opzione (--bucket, ...), variabile d'ambiente, "
            f"o in un file .env (cercato in: {args.env_file})."
        )

    return dict(
        bucket=bucket,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        aws_region=region,
        prefix=prefix,
        endpoint_url=endpoint_url,
    )


def print_rows(columns, rows) -> None:
    if not rows:
        print("(nessuna riga)")
        return
    widths = [max(len(col), *(len(str(row[i])) for row in rows)) for i, col in enumerate(columns)]
    header = "  ".join(col.ljust(w) for col, w in zip(columns, widths))
    print(header)
    print("-" * len(header))
    for row in rows:
        print("  ".join(str(v).ljust(w) for v, w in zip(row, widths)))


def run_query(conn, cur, sql: str) -> None:
    keyword = sql.strip().split()[0].upper() if sql.strip() else ""
    cur.execute(sql)

    if keyword in AUTOCOMMIT_KEYWORDS:
        conn.commit()
        print(f"OK ({cur.rowcount} righe modificate)")
    elif cur.description is None:
        print("OK")
    else:
        columns = [col[0] for col in cur.description]
        print_rows(columns, cur.fetchall())


def load_history() -> None:
    if readline is None:
        return
    readline.set_history_length(HISTORY_MAX_LINES)
    try:
        readline.read_history_file(HISTORY_PATH)
    except FileNotFoundError:
        pass


def save_history() -> None:
    if readline is None:
        return
    try:
        readline.write_history_file(HISTORY_PATH)
    except OSError:
        pass


def interactive_loop(conn, cur) -> None:
    print("s3ql — digita una query SQL, '.tables' per elencare le tabelle, 'exit' per uscire.")
    load_history()
    try:
        while True:
            try:
                sql = input("sql> ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                break

            if not sql:
                continue
            if sql.lower() in EXIT_COMMANDS:
                break
            if sql == ".tables":
                for name in sorted(conn.registry.tables):
                    print(name)
                continue

            try:
                run_query(conn, cur, sql)
            except Error as exc:
                print(f"Errore: {exc}")
    finally:
        save_history()


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="s3ql", description="Editor SQL interattivo per database s3ql (DuckDB + S3)."
    )
    parser.add_argument(
        "query", nargs="*",
        help="Query SQL da eseguire una tantum; se assente apre la shell interattiva",
    )
    parser.add_argument(
        "--env-file", default=".env",
        help="File .env da cui leggere i parametri di connessione (default: .env nella directory corrente)",
    )
    parser.add_argument("--bucket")
    parser.add_argument("--access-key-id")
    parser.add_argument("--secret-access-key")
    parser.add_argument("--region")
    parser.add_argument("--prefix")
    parser.add_argument("--endpoint-url")
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_arg_parser().parse_args(argv)
    config = build_config(args)

    with s3ql.connect(**config) as conn:
        cur = conn.cursor()
        if args.query:
            sql = " ".join(args.query)
            if sql == ".tables":
                for name in sorted(conn.registry.tables):
                    print(name)
                return
            try:
                run_query(conn, cur, sql)
            except Error as exc:
                raise SystemExit(f"Errore: {exc}")
        else:
            interactive_loop(conn, cur)


if __name__ == "__main__":
    main()
