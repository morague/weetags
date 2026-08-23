from __future__ import annotations

from sqlalchemy import Table, Column, func
from inspect import signature

from typing import Any

class TreeField(Column):
    inherit_cache = True

    @classmethod
    def from_column(cls, column: Column) -> TreeField:
        t = cls(**{k:v for k,v in column.__dict__.items() if k in signature(Column.__init__).parameters.keys()})
        t.table = column.table
        return  t

    def for_each(self, path: str | None = None):
        sub = func.for_each(self, path)
        return sub

class TreeFieldsCollection(dict):
    def __init__(self, map: dict[str, Any]) -> None:
        super().__init__(**map)

    def __getattr__(self, name: str):
        return self[name]

    @classmethod
    def from_table(cls, table: Table) -> TreeFieldsCollection:
        return cls({k:TreeField.from_column(c) for k,c in table.c.items()})

