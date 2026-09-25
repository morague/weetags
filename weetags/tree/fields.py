from __future__ import annotations

import inspect
from functools import wraps
from sqlalchemy import Table, Column, ColumnElement, func, TableValuedAlias
from sqlalchemy.schema import SchemaConst
from sqlalchemy.sql._typing import _InfoType
from sqlalchemy.sql.base import _NoArg, SchemaEventTarget as SchemaEventTarget
from sqlalchemy.sql.elements import KeyedColumnElement
from inspect import signature

from typing import Any, Callable, Literal



def _tabled_operator(f):
    @wraps(f)
    def wrapped(instance, *args, **kwargs):
        result = f(instance, *args, **kwargs)
        result.t = instance.t
        return result
    return wrapped


class TreeField(Column):
    inherit_cache = True
    t: Table | TableValuedAlias | None

    def __init__(self, *args, **kwargs):
        self.t = kwargs.pop("t", None)
        super().__init__(*args, **kwargs)
        for name, fn in inspect.getmembers(Column, inspect.isfunction):
            if inspect.signature(fn).return_annotation == "ColumnOperators":
                setattr(Column, name, _tabled_operator(fn))

    @classmethod
    def from_column(cls, column: Column | KeyedColumnElement) -> TreeField:
        t = cls(t=column.table, **{k:v for k,v in column.__dict__.items() if k in signature(Column.__init__).parameters.keys()})
        return  t

    def json_each(self, path: str | None = None) -> TreeField:
        if path is None:
            t = func.json_each(self).table_valued('value', joins_implicitly=True)
        else:
            t = func.json_each(self, path).table_valued('value', joins_implicitly=True)
        return TreeField.from_column(t.c.value)



    

class TreeFieldsCollection(dict):
    def __init__(self, map: dict[str, Any]) -> None:
        super().__init__(**map)

    def __getattr__(self, name: str):
        return self[name]

    @classmethod
    def from_table(cls, table: Table) -> TreeFieldsCollection:
        return cls({k:TreeField.from_column(c) for k,c in table.c.items()})

