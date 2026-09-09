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
from s3ql.exceptions import Error, ProgrammingError

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

DOT_HELP = """\
Dot command disponibili:
  .help                      mostra questo messaggio
  .tables                    elenca le tabelle nel bucket
  .schema <table>            mostra colonne e tipi della tabella
  .preload <table> [...]     carica tabelle in memoria (cache)
  .unload <table> [...]      scarica tabelle dalla memoria
  .status                    mostra stato connessione e transazione
  .exit / .quit              chiude la shell"""


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


def run_dot(conn, cur, line: str) -> None:
    parts = line.split()
    cmd = parts[0].lower()
    args = parts[1:]

    if cmd == ".help":
        print(DOT_HELP)

    elif cmd == ".tables":
        names = sorted(conn.registry.tables)
        if names:
            for name in names:
                print(name)
        else:
            print("(nessuna tabella)")

    elif cmd == ".schema":
        if not args:
            print("Uso: .schema <table>")
            return
        table = args[0]
        if not conn.registry.exists(table):
            print(f"Tabella '{table}' non trovata.")
            return
        cur.execute(f'SELECT * FROM "{table}" LIMIT 0')
        print(f"Tabella: {table}")
        print(f"{'Colonna':<30}  Tipo")
        print("-" * 50)
        for col in cur.description:
            name = col[0]
            type_code = col[1] if col[1] is not None else "?"
            print(f"{name:<30}  {type_code}")

    elif cmd == ".preload":
        if not args:
            print("Uso: .preload <table> [table...]")
            return
        try:
            conn.preload(*args)
            print(f"Preloaded: {', '.join(args)}")
        except ProgrammingError as exc:
            print(f"Errore: {exc}")

    elif cmd == ".unload":
        if not args:
            print("Uso: .unload <table> [table...]")
            return
        try:
            conn.unload(*args)
            print(f"Unloaded: {', '.join(args)}")
        except (ProgrammingError, Exception) as exc:
            print(f"Errore: {exc}")

    elif cmd == ".status":
        cfg = conn.config
        print(f"Bucket:   {cfg.bucket}")
        print(f"Prefix:   {cfg.prefix or '(none)'}")
        print(f"Endpoint: {cfg.endpoint_url or 'AWS S3'}")
        print(f"Tabelle:  {len(conn.registry.tables)}")
        tx = conn._tx
        if tx is None:
            print("Tx:       nessuna transazione attiva")
        else:
            loaded = tx.preloaded
            modified = tx.modified
            print(f"Tx:       attiva")
            print(f"  preloaded: {', '.join(loaded) if loaded else '(nessuna)'}")
            print(f"  modified:  {', '.join(modified) if modified else '(nessuna)'}")

    else:
        print(f"Comando sconosciuto: '{cmd}'. Digita .help per la lista.")


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
    print("s3ql — digita .help per i comandi disponibili, exit per uscire.")
    load_history()
    try:
        while True:
            try:
                line = input("sql> ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                break

            if not line:
                continue
            if line.lower() in EXIT_COMMANDS:
                break

            try:
                if line.startswith("."):
                    run_dot(conn, cur, line)
                else:
                    run_query(conn, cur, line)
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
        help="Query SQL o dot command da eseguire una tantum; se assente apre la shell interattiva",
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
            line = " ".join(args.query)
            try:
                if line.startswith("."):
                    run_dot(conn, cur, line)
                else:
                    run_query(conn, cur, line)
            except Error as exc:
                raise SystemExit(f"Errore: {exc}")
        else:
            interactive_loop(conn, cur)


if __name__ == "__main__":
    main()
