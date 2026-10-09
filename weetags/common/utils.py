from __future__ import annotations


import json
import hashlib
import inspect
from pathlib import Path
from sqlalchemy import Table, exc
from datetime import datetime
from attrs import define, field

from typing import Any, Callable, Type, get_args

import weetags.common.types as ty
import weetags.common.exceptions as excp

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
}

SQLF = {
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
            raise excp.LiteralError("Field type", get_args(ty.AcceptedFieldType.__value__))
        

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
        raise excp.ParsedTypeError("buffer", "str | utf-8 encoded bytes")

def path_converter(value: Any) -> Path | None:
    if value is None:
        return None
    elif isinstance(value, str):
        return Path(value)
    elif isinstance(value, Path):
        return value
    else:
        raise excp.ParsedTypeError("path", "str | Path")





def field_exist(field: str, metadata: Table) -> bool:
    if field in metadata.columns.keys():
        raise excp.ExistingFieldNameError(field)
    return True

def field_not_exist(field: str, metadata: Table) -> bool:
    if field not in metadata.columns.keys():
            raise excp.FieldError(field)
    return True

def field_non_nullable(nullable: bool, default: Any) -> bool:
    if nullable is False and default is None:
        raise excp.FieldConstrainError("Newly added fields must be: `nullable` or have `default` value.")
    return True

def psql_required(dialect: str) -> bool:
    if dialect != "postgres":
        raise excp.TreeEngineDialectError("Constraint alterations require a `psql` database.")
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


@define
class Signature:
    f: Callable = field()
    args: tuple[Any] = field()
    kwargs: dict[str, Any] = field()
    parameters: dict[str, Any] = field(factory=dict, init=False)

    def __attrs_post_init__(self) -> None:
        signature = inspect.signature(self.f)
        parameters = {k:param.default for k,param in signature.parameters.items()}
        parameters.update(self.kwargs)

        ordered_keys = list([k for k in signature.parameters.keys() if k != "self"])
        [parameters.update({ordered_keys[i]:v}) for i,v in enumerate(self.args)]
        self.parameters = parameters

    @property
    def sha1_digest(self) -> str:
        args = json.dumps([self._serialize(v) for v in self.args])
        kwargs = json.dumps({k:self._serialize(v) for k,v in self.kwargs.items()})
        name = self.f.__name__
        return hashlib.sha1(f"{name}_{args}_{kwargs}".encode()).hexdigest()

    def get_parameter(self, key: str, default: Any = None, or_raise: bool = False) -> Any:
        if key not in self.parameters.keys() and or_raise:
            raise excp.UnknownArgumentError(key)
        return self.parameters.get(key, default)

    def _serialize(self, value: Any) -> Any:
        if isinstance(value, datetime):
            return value.isoformat()
        elif isinstance(value, (list, tuple)):
            return [self._serialize(v) for v in value]
        elif isinstance(value, dict):
            return {k:self._serialize(v) for k,v in value.items()}
        elif isinstance(value, (bool, str, int)):
            return value
        else:
            return str(value)
