from __future__ import annotations

from math import e
import re
import inspect
from abc import ABC
from collections import ChainMap
from urllib.parse import unquote
from sanic import Request
from functools import wraps
from attrs import define, field, validators, Attribute

from typing import Any, Callable, Type, get_args

import weetags.common.types as ty

def int_converter(value: Any) -> int:
    if isinstance(value, int):
        return value
    elif isinstance(value, str) and value.isnumeric():
        return int(value)
    else:
        raise TypeError(f"Unable to convert value {value} to int.")

def list_or_none_converter(value: Any) -> list[Any] | None:
    if value is None:
        return value
    if isinstance(value, list):
        return value
    elif isinstance(value, str):
        return [s.strip() for s in value.split(",")]
    else:
        raise TypeError("a list or a comma seperated string is expected.")

def bool_converter(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return value

    if isinstance(value, list) and len(value) == 1:
        value = value[0]

    true_pattern = re.compile(r"True|TRUE|true|on|1")
    falst_pattern = re.compile(r"False|FALSE|false|off|0")

    if isinstance(value, str) and value == "":
        return True
    elif isinstance(value, str):
        is_true = re.findall(true_pattern, value)
        is_false = re.findall(falst_pattern, value)
        if len(is_true) > 0 and len(is_false) > 0:
            raise ValueError(f"ambiguous bool value {str(value)}.")
        elif len(is_true) > 1 or len(is_false) > 1:
            raise ValueError(f"ambiguous bool value {str(value)}.")
        elif len(is_true) == 1:
            return True
        elif len(is_false) == 1:
            return False
    elif isinstance(value, int) and value == 1:
        return True
    elif isinstance(value, int) and value == 0:
        return False
    else:
        raise ValueError(f"Unable to convert {value} into boolean.")

    return False

def validate_list_or_none(instance: Type, attribute: Attribute, value: Any):
    if value is None:
        return
    if not isinstance(value, list):
        raise ValueError(f"Argument {attribute} is not of type: list[str]")
    elif not all([isinstance(e, str) for e in value]):
        raise ValueError(f"Argument {attribute} is not of type: list[str]")


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
        raise ValueError(f"Relation must be one of: {get_args(ty.BaseRelations.__value__)}.")

class ArgumentParser(ABC):
    tree_name: str = field(validator=validators.instance_of(str))

    @classmethod
    def from_request(cls, request: Request) -> ArgumentParser:
        url_args = {k: (unquote(v) if isinstance(v, str) else v) for k, v in request.match_info.items()} or {}
        query_args = {k.replace("-", "_"): v for k, v in request.get_query_args(keep_blank_values=True)} or {}
        payload = request.json or {}
        params: dict[str, Any] = dict(ChainMap(payload, url_args, query_args))
        return cls(**params)
    
    def get_kwargs(self, callable: Callable) -> dict[str, Any]:
        return {k:getattr(self, k, None) for k in inspect.getfullargspec(callable).args if getattr(self, k, None) is not None and k != "self"}

@define
class BaseTreeArguments(ArgumentParser):
    tree_name: str = field(validator=validators.instance_of(str))

@define
class NodesArguments(ArgumentParser):
    tree_name: str = field(validator=validators.instance_of(str))
    fields: list[str] | None = field(default=None, converter=list_or_none_converter, validator=validate_list_or_none)
    q: list | None = field(default=None)
    page: int = field(default=0, converter=int_converter, validator=validators.instance_of(int))
    page_size: int = field(default=100, converter=int_converter, validator=validators.instance_of(int))


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