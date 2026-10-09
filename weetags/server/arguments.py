from __future__ import annotations

import re
import inspect
from abc import ABC
from collections import ChainMap, defaultdict
from urllib.parse import unquote
import attr
from sanic import Request
from functools import wraps
from attrs import define, field, validators, Attribute

from typing import Any, Callable, Sequence, Type, get_args

import weetags.common.types as ty
import weetags.common.exceptions as excp
from weetags.common.conditions import Condition, convert_cmp_conditions, convert_rel_conditions

def int_converter(value: Any) -> int:
    if isinstance(value, int):
        return value
    elif isinstance(value, str) and value.isnumeric():
        return int(value)
    else:
        raise excp.ConvertTypeError(str(value), "int")
        
def list_or_none_converter(value: Any) -> list[Any] | None:
    if value is None:
        return value
    if isinstance(value, list):
        return value
    elif isinstance(value, str):
        return [s.strip() for s in value.split(",")]
    else:
        raise excp.ConvertTypeError(str(value), "list[str]", "A list or a comma separated string are is expected.")

def bool_converter(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return value

    if isinstance(value, list) and len(value) == 1:
        value = value[0]

    true_pattern = re.compile(r"True|TRUE|true|on|1")
    falst_pattern = re.compile(r"False|FALSE|false|off|0")
    error = excp.ConvertTypeError(str(value), "bool")
    if isinstance(value, str) and value == "":
        return True
    elif isinstance(value, str):
        is_true = re.findall(true_pattern, value)
        is_false = re.findall(falst_pattern, value)
        if len(is_true) > 0 and len(is_false) > 0:
            raise error
        elif len(is_true) > 1 or len(is_false) > 1:
            raise error
        elif len(is_true) == 1:
            return True
        elif len(is_false) == 1:
            return False
    elif isinstance(value, int) and value == 1:
        return True
    elif isinstance(value, int) and value == 0:
        return False
    else:
        raise error

    return False

def str_or_none(instance: Type, attribute: Attribute, value: Any):
    if not isinstance(value, str) and value is not None:
        raise excp.ParsedTypeError(attribute.name, "str | None")

def int_or_none(instance: Type, attribute: Attribute, value: Any):
    if not isinstance(value, int) and value is not None:
        raise excp.ParsedTypeError(attribute.name, "int | None")

def dict_or_none(instance: Type, attribute: Attribute, value: Any):
    if not isinstance(value, dict) and value is not None:
        raise excp.ParsedTypeError(attribute.name, "dict | None")

def is_colision(instance: Type, attribute: Attribute, value: Any):
    if value not in get_args(ty.OnCollision.__value__):
        raise excp.LiteralError(attribute.name, get_args(ty.OnCollision.__value__))

def validate_list_or_none(instance: Type, attribute: Attribute, value: Any):
    if value is None:
        return
    error = excp.ParsedTypeError(attribute.name, "list[str]")
    if not isinstance(value, list):
        raise error
    elif not all([isinstance(e, str) for e in value]):
        raise error


def parser(model: Type[ArgumentParser]):
    def decorator(f):
        @wraps(f)
        async def wrapper(request, *args, **kwargs):
            arguments = model.from_request(request)
            response = await f(request, arguments)
            return response
        return wrapper
    return decorator

def validate_relation(instance: Type, attribute: Attribute, value: Any) -> None:
    if value not in get_args(ty.BaseRelations.__value__):
        raise excp.LiteralError(attribute.name, get_args(ty.BaseRelations.__value__))

def base_query_args(request: Request) -> dict[str, Any]:
    _map = defaultdict(list)
    [_map[k].append(v) for k,v in request.get_query_args(keep_blank_values=True)]
    args = {k:(v if len(v) > 1 else v[0]) for k,v in _map.items()}
    return args

def query_args_with_conditions(request: Request) -> dict[str, Any]:
    _map = defaultdict(list)
    [_map[k].append(v) for k,v in request.get_query_args(keep_blank_values=True)]

    args = {}
    for k,v in _map.items():
        if k in ["q", "r"]:
            args.update({k:v})
        elif len(v) > 1:
            args.update({k:v})
        else:
            args.update({k:v[0]})
    return args

class ArgumentParser(ABC):
    tree_name: str = field(validator=validators.instance_of(str))

    @classmethod
    def from_request(cls, request: Request) -> ArgumentParser:
        url_args = {k: (unquote(v) if isinstance(v, str) else v) for k, v in request.match_info.items()} or {}
        query_args = cls.query_args(request)
        payload = request.json or {}
        params: dict[str, Any] = dict(ChainMap(payload, url_args, query_args))
        return cls(**params)
    
    def get_kwargs(self, callable: Callable) -> dict[str, Any]:
        return {k:getattr(self, k, None) for k in inspect.getfullargspec(callable).args if getattr(self, k, None) is not None and k != "self"}

    @staticmethod
    def query_args(request: Request) -> dict[str, Any]:
        return base_query_args(request)


@define
class BaseTreeArguments(ArgumentParser):
    tree_name: str = field(validator=validators.instance_of(str))

@define
class NodesArguments(ArgumentParser):
    tree_name: str = field(validator=validators.instance_of(str))
    fields: list[str] | None = field(default=None, converter=list_or_none_converter, validator=validate_list_or_none)
    q: Sequence[Condition] | None = field(default=None, converter=convert_cmp_conditions)
    r: Sequence[Condition] | None = field(default=None, converter=convert_rel_conditions)
    page: int = field(default=0, converter=int_converter, validator=validators.instance_of(int))
    page_size: int = field(default=100, converter=int_converter, validator=validators.instance_of(int))
    
    @staticmethod
    def query_args(request: Request) -> dict[str, Any]:
        return query_args_with_conditions(request)


@define
class ClosestNodesArguments(ArgumentParser):
    tree_name: str = field(validator=validators.instance_of(str))
    node_name: str = field(validator=validators.instance_of(str))
    fields: list[str] | None = field(default=None, converter=list_or_none_converter, validator=validate_list_or_none)
    q: Sequence[Condition] | None = field(default=None, converter=convert_cmp_conditions)
    r: Sequence[Condition] | None = field(default=None, converter=convert_rel_conditions)

    @staticmethod
    def query_args(request: Request) -> dict[str, Any]:
        return query_args_with_conditions(request)

@define
class RootArguments(ArgumentParser):
    tree_name: str = field(validator=validators.instance_of(str))
    fields: list[str] | None = field(default=None, converter=list_or_none_converter, validator=validate_list_or_none)


@define
class NodeArguments(ArgumentParser):
    tree_name: str = field(validator=validators.instance_of(str))
    node_name: str = field(validator=validators.instance_of(str))
    fields: list[str] | None = field(default=None, converter=list_or_none_converter, validator=validate_list_or_none)

@define
class RelationArguments(ArgumentParser):
    tree_name: str = field(validator=validators.instance_of(str))
    node_name: str = field(validator=validators.instance_of(str))
    relation: ty.BaseRelations | str = field(validator=validate_relation)
    fields: list[str] | None = field(default=None, converter=list_or_none_converter, validator=validate_list_or_none)

@define
class Relation1Arguments(ArgumentParser):
    tree_name: str = field(validator=validators.instance_of(str))
    node_name: str = field(validator=validators.instance_of(str))
    relation: ty.BaseRelations | str = field(validator=validate_relation)
    fields: list[str] | None = field(default=None, converter=list_or_none_converter, validator=validate_list_or_none)
    include_self: bool = field(default=False, converter=bool_converter)

@define
class CmpUtilsArguments(ArgumentParser):
    tree_name: str = field(validator=validators.instance_of(str))
    node1: str = field(validator=validators.instance_of(str))
    node2: str = field(validator=validators.instance_of(str))
    fields: list[str] | None = field(default=None, converter=list_or_none_converter, validator=validate_list_or_none)



@define
class Fieldrguments(ArgumentParser):
    tree_name: str = field(validator=validators.instance_of(str))
    field_name: str = field(validator=validators.instance_of(str))
    distinct: bool = field(default=False, converter=bool_converter, validator=validators.instance_of(bool))

@define
class DrawArguments(ArgumentParser):
    tree_name: str = field(validator=validators.instance_of(str))
    subtree: str | None = field(default=None)
    style: str = field(default="ascii")
    extra_spacing: bool = field(default=False)


@define
class ExplorerArguments(ArgumentParser):
    tree_name: str = field(validator=validators.instance_of(str))
    page: int = field(default=0, converter=int_converter, validator=validators.instance_of(int))
    page_size: int = field(default=100, converter=int_converter, validator=validators.instance_of(int))
    conditions: list | None = field(default=None)


@define
class LoginArguments(ArgumentParser):
    message: str | None = field(default=None, validator=str_or_none)
    level: str | None = field(default=None, validator=str_or_none)



@define
class AuthArguments(ArgumentParser):
    username: str = field(validator=validators.instance_of(str))
    password: str = field(validator=validators.instance_of(str))
    set_cookie: bool = field(default=False, converter=bool_converter, validator=validators.instance_of(bool))
    redirect: bool = field(default=False, converter=bool_converter, validator=validators.instance_of(bool))
    message: str | None = field(default=None, validator=str_or_none)
    level: str | None = field(default=None, validator=str_or_none)

    @classmethod
    def from_request(cls, request: Request) -> ArgumentParser:
        if "application/x-www-form-urlencoded" in request.content_type:
            url_args = {k: (unquote(v) if isinstance(v, str) else v) for k, v in request.match_info.items()} or {}
            query_args = {k.replace("-", "_"): v for k, v in request.get_query_args(keep_blank_values=True)} or {}
            form = request.form
            assert form is not None
            payload = {k:v[0] for k,v in form.items()}
            params: dict[str, Any] = dict(ChainMap(payload, url_args, query_args))
        else:
            url_args = {k: (unquote(v) if isinstance(v, str) else v) for k, v in request.match_info.items()} or {}
            query_args = {k.replace("-", "_"): v for k, v in request.get_query_args(keep_blank_values=True)} or {}
            payload = request.json or {}
            params: dict[str, Any] = dict(ChainMap(payload, url_args, query_args))

        c = params.get("set_cookie", False)
        if c == "set_cookie":
            params["set_cookie"] = True
        c = params.get("redirect", False)
        if c == "redirect":
            params["redirect"] = True
        return cls(**params)

@define
class AddNodeArguments(ArgumentParser):
    tree_name: str = field(validator=validators.instance_of(str))
    node_name: str = field(validator=validators.instance_of(str))
    parent: str = field(validator=validators.instance_of(str))
    metadata: dict[str, Any] = field(validator=validators.instance_of(dict))

@define
class AddNodesArguments(ArgumentParser):
    tree_name: str = field(validator=validators.instance_of(str))
    nodes: list[dict[str, Any]] = field(validator=validators.instance_of(list))
    keymap: dict[str, Any] | None = field(default=None, validator=dict_or_none)
    on_collision: ty.OnCollision = field(default="raise", validator=is_colision) # pyright: ignore

@define
class RemoveNodeArguments(ArgumentParser):
    tree_name: str = field(validator=validators.instance_of(str))
    node_name: str = field(validator=validators.instance_of(str))
    force: bool = field(default=False, converter=bool_converter, validator=validators.instance_of(bool))

@define
class RemoveNodesArguments(ArgumentParser):
    tree_name: str = field(validator=validators.instance_of(str))
    q: Sequence[Condition] | None = field(default=None, converter=convert_cmp_conditions)
    r: Sequence[Condition] | None = field(default=None, converter=convert_rel_conditions)
    force: bool = field(default=False, converter=bool_converter, validator=validators.instance_of(bool))

    @staticmethod
    def query_args(request: Request) -> dict[str, Any]:
        return query_args_with_conditions(request)

@define
class PruneArguments(ArgumentParser):
    tree_name: str = field(validator=validators.instance_of(str))
    subtree: str = field(validator=validators.instance_of(str))

@define
class ExportArguments(ArgumentParser):
    tree_name: str = field(validator=validators.instance_of(str))
    subtree: str | None = field(default=None, validator=str_or_none)

@define
class UpdateNodesArguments(ArgumentParser):
    tree_name: str = field(validator=validators.instance_of(str))
    values: dict[str, Any] = field(validator=validators.instance_of(dict))
    q: Sequence[Condition] | None = field(default=None, converter=convert_cmp_conditions)
    r: Sequence[Condition] | None = field(default=None, converter=convert_rel_conditions)

    @staticmethod
    def query_args(request: Request) -> dict[str, Any]:
        return query_args_with_conditions(request)

@define
class UpdateNodeArguments(ArgumentParser):
    tree_name: str = field(validator=validators.instance_of(str))
    node_name: str = field(validator=validators.instance_of(str))
    values: dict[str, Any] = field(validator=validators.instance_of(dict))

@define
class AppendListArguments(ArgumentParser):
    tree_name: str = field(validator=validators.instance_of(str))
    node_name: str = field(validator=validators.instance_of(str))
    key: str = field(validator=validators.instance_of(str))
    value: Any = field()

@define
class ExtendListArguments(ArgumentParser):
    tree_name: str = field(validator=validators.instance_of(str))
    node_name: str = field(validator=validators.instance_of(str))
    key: str = field(validator=validators.instance_of(str))
    value: list[Any] = field(validator=validators.instance_of(list))

@define
class PopListArguments(ArgumentParser):
    tree_name: str = field(validator=validators.instance_of(str))
    node_name: str = field(validator=validators.instance_of(str))
    key: str = field(validator=validators.instance_of(str))
    index: int = field(validator=validators.instance_of(int))

@define
class SetObjectKeyArguments(ArgumentParser):
    tree_name: str = field(validator=validators.instance_of(str))
    node_name: str = field(validator=validators.instance_of(str))
    path: str = field(validator=validators.instance_of(str))
    value: Any = field()

@define
class PopObjectKeyArguments(ArgumentParser):
    tree_name: str = field(validator=validators.instance_of(str))
    node_name: str = field(validator=validators.instance_of(str))
    path: str = field(validator=validators.instance_of(str))

@define
class ExtendObjectKeyArguments(ArgumentParser):
    tree_name: str = field(validator=validators.instance_of(str))
    node_name: str = field(validator=validators.instance_of(str))
    path: str = field(validator=validators.instance_of(str))
    value: list[Any] = field(validator=validators.instance_of(list))

@define
class PopListObjectArguments(ArgumentParser):
    tree_name: str = field(validator=validators.instance_of(str))
    node_name: str = field(validator=validators.instance_of(str))
    path: str = field(validator=validators.instance_of(str))
    index: int = field(validator=validators.instance_of(int))