from __future__ import annotations

from enum import StrEnum
from ast import literal_eval
from attrs import define, field, Attribute, validators

from typing import Any, Literal, Callable

from weetags.exceptions import ParsingError, CoversionError

Conditions = list[list[tuple[str, str, Any] | str] | str]
Relations = Literal["parent", "children", "siblings", "ancestors", "descendants"]
Style = Literal["ascii", "ascii-ex", "ascii-exr", "ascii-emh", "ascii-emv", "ascii-em"]

class _Relations(StrEnum):
    PARENT = "parent"
    CHILDREN = "children"
    SIBLINGS = "siblings"
    ANCESTORS = "ancestors"
    DESCENDANTS = "descendants"

    def values():
        return [s.value for s in _Relations]
    
class _Styles(StrEnum):
    ASCII = "ascii",
    ASCII_EX = "ascii-ex"
    ASCII_EXR = "ascii-exr"
    ASCII_EMH = "ascii-emh"
    ASCII_EMV = "ascii-emv"
    ASCII_EM = "ascii-em"

    def values():
        return [s.value for s in _Styles]

def styleOrNone(instance: ParamParser, attribute: Attribute, value: Any) -> None:
    if value is None:
        return     
    elif value not in _Styles.values():
        raise ValueError(f"possible styles: {_Styles.values()}")

def relationOrNone(instance: ParamParser, attribute: Attribute, value: Any) -> None:
    if value is None:
        return     
    elif value not in _Relations.values():
        raise ValueError(f"possible relations: {_Relations.values()}")

def intOrNone(instance: ParamParser, attribute: Attribute, value: Any) -> None:
    if value is None:
        return 
    elif not isinstance(value, int):
        raise ParsingError(attribute, value, "int | None")

def strOrNone(instance: ParamParser, attribute: Attribute, value: Any) -> None:
    if value is None:
        return
    if not isinstance(value, str):
        raise ParsingError(attribute, value, "str | None")

def listOrNone(instance: ParamParser, attribute: Attribute, value: Any) -> None:
    if value is None:
        return
    if not (isinstance(value, list) or isinstance(value, tuple)):
        raise ParsingError(attribute, value, "list | None")
    
def boolOrNone(instance: ParamParser, attribute: Attribute, value: Any) -> None:
    if value is None:
        return
    if not isinstance(value, bool):
        raise ParsingError(attribute, value, "bool | None")

def dictOrNone(instance: ParamParser, attribute: Attribute, value: Any) -> None:
    if value is None:
        return
    if not isinstance(value, dict):
        raise ParsingError(attribute, value, "dict[str, Any] | None")

def list_converter(value: Any) -> list[Any]:
    if value is None:
        return None

    if isinstance(value, list):
        return value
    elif isinstance(value, str):
        try:    
            ast = literal_eval(value)
            if isinstance(ast, tuple):
                ast = list(ast)
            return ast
        except Exception:
            return [v.strip() for v in value.split(",")]
    else:
        raise TypeError(f"Only comma separated strings can be converted into list")
    
def int_converter(value: Any) -> int | None:
    if value is None:
        return None
    
    if (isinstance(value, int) or isinstance(value, str)) is False:
        raise CoversionError(value, "int")
    return int(value)

def bool_converter(value: Any) -> bool | None:
    if value is None:
        return None
    
    if isinstance(value, int) and value in [0,1]:
        value = bool(value)
    elif isinstance(value, str) and value.lower() == "true":
        value = True
    elif isinstance(value, str) and value.lower() == "false":
        value = False

    if isinstance(value, str):
        try:    
            value = literal_eval(value)
        except Exception:
            raise CoversionError(value, "bool")
    return value    

def simple_ast(value: Any) -> Any:
    if value is None:
        return None
    
    if isinstance(value, str):
        try:    
            value = literal_eval(value)
        except Exception:
            raise CoversionError(value, "Any")
    return value

"""
TO BE ADDED:
    - args for tree exportation.
"""

@define(slots=False, kw_only=True)
class ParamParser:

    # nid (str | None): Define the node_id.
    nid: str | None = field(default=None, validator=[strOrNone])

    # fields (list[str] | None). list of fields to return.
    fields: list[str] | None = field(default=None, converter=list_converter, validator=[listOrNone])

    # order_by (list[str] | None). list of fields used for ordering the records.
    order_by: list[str] | None = field(default=None, converter=list_converter, validator=[listOrNone])

    # axis (int). 1: ASC, 0: DESC. define whether the records list is reversed or not.
    axis: int = field(default=1, converter=int_converter, validator=[validators.instance_of(int)])

    # limit (int | None). Define the number of records to be return. By default all complying records are returned.
    limit:  int | None = field(default=None, converter=int_converter, validator=[intOrNone])

    # conditions (Conditions | None). list of all sets of conditions to be applied during the search.
    conditions: Conditions | None = field(default=None, converter=list_converter, validator=[listOrNone])

    # relation (Relations | None). Define the researched relation from a based node or nodes.
    relation: Relations | None = field(default=None, validator=[relationOrNone])

    # include_base (bool | None). for relation where search. Define whether you keep the originating node.
    include_base: bool | None = field(default=None, converter=bool_converter, validator=[boolOrNone])

    # check_siblings (bool | None). when questioning 2 nodes relations. allow to check for siblings relations or only branches relations.
    check_siblings: bool | None = field(default=None, converter=bool_converter, validator=[boolOrNone])

    # nid0 & nid1 (str | None). define 2 nodes to be compared.
    nid0: str | None = field(default=None, validator=[strOrNone])
    nid1: str | None = field(default=None, validator=[strOrNone])
    
    # to (str | None). define the destination node when searching for a path between 2 nodes.
    to: str | None = field(default=None, validator=[strOrNone])

    node: dict[str, Any] | None = field(default=None, converter=simple_ast, validator=[dictOrNone])
    set_values: list[tuple[str, Any]] | None = field(default=None, converter=list_converter, validator=[listOrNone])

    field_name: str | None = field(default=None, validator=[strOrNone])
    value: Any | None = field(default=None, converter=simple_ast)
    values: list[Any] | None = field(default=None, converter=list_converter, validator=[listOrNone])

    style: Style | None = field(default=None, validator=[styleOrNone])
    extra_space: bool | None = field(default=None, converter=bool_converter, validator=[boolOrNone])

    def get_kwargs(self, f: Callable) -> dict[str, Any]:
        """match function params with parsed params. Return all non null params used by the function."""
        return {k:getattr(self, k) for k,v in f.__annotations__.items() if getattr(self, k, None) is not None}