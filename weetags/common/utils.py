from __future__ import annotations


import json
import hashlib
from pathlib import Path
from sqlalchemy import Table
from datetime import datetime
from attrs import define, field

from typing import Any, Callable, Type

OP = {
    "=": "__eq__",
    "!=": "__ne__",
    ">": "__gt__",
    ">=": "__ge__",
    "<": "__lt__",
    "<=": "__le__",
    "like": "like",
    "ilike": "ilike",
    "not_like": "notlike",
    "not_ilike": "notilike",
    "in": "in_",
    "not_in": "not_in",
    "is": "is_",
    "is_not": "is_not",
    "json_each": "json_each"
}


class Json:
    pass

def translate_sqlalchemy_sqltype(dtype: Any) -> Type:
    match str(dtype):
        case "INTEGER":
            return int
        case "TEXT":
            return str
        case "BOOLEAN":
            return bool
        case "JSON":
            return Json
        case _:
            raise ValueError("Non handled sql type")
        

def translate_keys(data: dict[str, Any], keymap: dict[str, Any] | None = None) -> dict[str, Any]:
    if keymap is None:
        return data
    
    for k,v in keymap.items():
        if k in data.keys():
            data[v] = data.pop(k)
    return data

def convert_buffer(value: Any) -> str | None:
    if value is None:
        return None
    elif isinstance(value, str):
        return value
    elif isinstance(value, bytes):
        return value.decode()
    else:
        raise TypeError("buffer must be a str or utf-8 encoded bytes.")

def path_converter(value: Any) -> Path | None:
    if value is None:
        return None
    elif isinstance(value, str):
        return Path(value)
    elif isinstance(value, Path):
        return value
    else:
        raise TypeError("path must be a str or a Path")





def field_exist(field: str, metadata: Table) -> bool:
    if field in metadata.columns.keys():
        raise KeyError(f"Field name: {field} does already exist")
    return True

def field_not_exist(field: str, metadata: Table) -> bool:
    if field not in metadata.columns.keys():
        raise KeyError(f"Field name: {field} doesn't exist")
    return True

def field_non_nullable(nullable: bool, default: Any) -> bool:
    if nullable is False and default is None:
        raise ValueError("New fields require either to be nullable or to have a default value")
    return True

def psql_required(dialect: str) -> bool:
    if dialect != "postgres":
        raise ValueError("Unique constraint manipulation after tree creation is only available for psql dialect.")
    return True

def require_sqlite_version() -> bool:
    """
        if self._engine.uri.dialect == "sqlite":
            version = self._engine.engine.dialect.server_version_info
            if version is not None and version[0] >= 3 and version[1] >= 53:
                pass
            else:
                raise ValueError("Nullable constraint manipulation require sqlite engine version >= 3.53.0")
        elif self._engine.uri.dialect != "postgres":
            raise ValueError("Unique constraint manipulation after tree creation is only available for psql dialect.")
    """
    return True

def get_argument(source: tuple[tuple, dict],  name: str, index: int) -> Any:
    args, kwargs = source

    value = kwargs.get(name, None)
    if value is None and len(args) >= index + 1:
        value = args[index]
    return value

@define(frozen=True)
class Signature:
    callable: Callable = field()
    args: tuple[Any] = field()
    kwargs: dict[str, Any] = field()

    @property
    def sha1_digest(self) -> str:
        args = json.dumps([self._serialize(v) for v in self.args])
        kwargs = json.dumps({k:self._serialize(v) for k,v in self.kwargs.items()})
        name = self.callable.__name__
        return hashlib.sha1(f"{name}_{args}_{kwargs}".encode()).hexdigest()

    def _serialize(self, value: Any) -> Any:
        if isinstance(value, datetime):
            return value.isoformat()
        elif isinstance(value, (list, tuple)):
            return [self._serialize(v) for v in value]
        elif isinstance(value, dict):
            return {k:self._serialize(v) for k,v in value.items()}
        else:
            return value
