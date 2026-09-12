from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from typing import TYPE_CHECKING

from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from .config import S3Config

_META_KEY_NAME = "_s3ql_meta.json"


@dataclass
class IndexDef:
    name: str
    table: str
    columns: list[str]
    partitioned: bool
    schema: list[list[str]] | None = None  # [[col_name, duckdb_type], ...] for partitioned tables


class IndexStore:
    def __init__(self, config: "S3Config", s3_client):
        self._config = config
        self._s3 = s3_client
        self._indexes: dict[str, IndexDef] = {}  # index_name → IndexDef

    def load(self):
        key = self._config.prefix + _META_KEY_NAME
        try:
            resp = self._s3.get_object(Bucket=self._config.bucket, Key=key)
            data = json.loads(resp["Body"].read())
            for name, d in data.items():
                self._indexes[name] = IndexDef(**d)
        except ClientError as exc:
            if exc.response["Error"]["Code"] not in ("NoSuchKey", "404"):
                raise

    def save(self):
        key = self._config.prefix + _META_KEY_NAME
        data = {name: asdict(idef) for name, idef in self._indexes.items()}
        self._s3.put_object(
            Bucket=self._config.bucket, Key=key, Body=json.dumps(data).encode()
        )

    def add(self, idef: IndexDef):
        self._indexes[idef.name] = idef
        self.save()

    def remove_by_name(self, name: str) -> IndexDef | None:
        idef = self._indexes.pop(name, None)
        if idef:
            self.save()
        return idef

    def remove_for_table(self, table: str) -> IndexDef | None:
        found = None
        for n, d in list(self._indexes.items()):
            if d.table == table:
                found = d
                del self._indexes[n]
        if found:
            self.save()
        return found

    def get_by_name(self, name: str) -> IndexDef | None:
        return self._indexes.get(name)

    def get_for_table(self, table: str) -> IndexDef | None:
        for idef in self._indexes.values():
            if idef.table == table:
                return idef
        return None

    def is_partitioned(self, table: str) -> bool:
        idef = self.get_for_table(table)
        return idef is not None and idef.partitioned

    def partition_columns(self, table: str) -> list[str] | None:
        idef = self.get_for_table(table)
        return idef.columns if (idef and idef.partitioned) else None

    def sort_columns(self, table: str) -> list[str] | None:
        idef = self.get_for_table(table)
        return idef.columns if (idef and not idef.partitioned) else None

    @property
    def all_indexes(self) -> list[IndexDef]:
        return list(self._indexes.values())
