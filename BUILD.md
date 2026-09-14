# Build e pubblicazione su PyPI

## Prerequisiti

```bash
pip install build twine
```

## Build

```bash
python -m build
```

Produce in `dist/`:
- `bucketdb-0.1.0.tar.gz` — source distribution
- `bucketdb-0.1.0-py3-none-any.whl` — wheel

## Pubblicazione su PyPI

Serve un API token da pypi.org → Account settings → API tokens.

```bash
twine upload dist/*
```

Credenziali:
- username: `__token__`
- password: `pypi-...` (il token)

## Aggiornare la versione

1. Modificare `version` in `pyproject.toml`
2. Rifare build: `rm -rf dist/ && python -m build`
3. Caricare: `twine upload dist/*`

## Verifica locale prima di pubblicare

```bash
pip install dist/bucketdb-0.1.0-py3-none-any.whl
python -c "import bucketdb; print(bucketdb.apilevel)"
```
