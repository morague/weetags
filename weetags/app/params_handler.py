from ast import literal_eval
from typing_extensions import Callable
from attrs import define, field, validators

from typing import Any

Payload = dict[str, Any]



def parse_str(value):
    if value is None:
        return None
    return str(value)

def validate_elm_type(instance, attribute, value):
    if value is not None and not all([isinstance(v, str) for v in value]):
        raise ValueError("not good element type")

def validate_conds(instance, attribute, value):
    if value is None:
        return

    for cond in value:
        if (
            not isinstance(cond, tuple)
            or len(cond) != 3
            or any([not isinstance(cond[i], str) for i in range(2)])
            ):
            raise ValueError("conditions must be list[tuple[str, str, Any]]")


def parse_int(value):
    if value is None:
        return None
    elif (isinstance(value, int) or isinstance(value, str)) is False:
        raise ValueError("cannot convert type into int.")
    return int(value)

def parse_list(value):
    if value is None:
        return None
    elif isinstance(value, list):
        return value
    try:
        parsed = literal_eval(value)
    except Exception:
        parsed = [v.strip() for v in value.split(",")]
    return parsed

def parse_bool(value):
    if value is None:
        return None
    elif isinstance(value, bool):
        return value
    elif isinstance(value, int) and value in [0,1]:
        return bool(value)

    elif isinstance(value, str) and value in ["true", "1"]:
        print("here")
        return True
    elif isinstance(value, str) and value in ["false", "0"]:
        return False
    else:
        raise ValueError("cannot convert type into bool")

@define(slots=False, kw_only=True)
class ParamsParser:
    nid: str | None = field(default=None, converter=parse_str)
    fields: list[str] | None = field(default=None, converter=parse_list, validator=[validate_elm_type])
    conds: list[tuple[str,str,Any]] | None = field(default=None, converter=parse_list, validator=[validate_conds])
    order: list[str] | None = field(default=None, converter=parse_list, validator=[validate_elm_type])
    axis: int | None = field(default=None, converter=parse_int)
    limit: int | None = field(default=None, converter=parse_int)
    depth: int | None = field(default=None, converter=parse_int)
    is_root: bool | None = field(default=None, converter=parse_bool)
    is_leaf: bool | None = field(default=None, converter=parse_bool)
    extra_space: bool | None = field(default=None, converter=parse_bool)
    style: str | None = field(default=None, converter=parse_str)

    #utils
    nid0: str | None = field(default=None, converter=parse_str)
    nid1: str | None = field(default=None, converter=parse_str)
    # writing
    node: dict[str, Any] | None = field(default=None)
    set_values: list[tuple[str, Any]] | None = field(default=None)

    def get_kwargs(self, f: Callable) -> dict[str, Any]:
        """match function params with parsed params. Return all non null params used by the function."""
        return {k:getattr(self, k) for k,v in f.__annotations__.items() if getattr(self, k, None) is not None}
