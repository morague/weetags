from ast import literal_eval
from attrs import define, field, validators

from typing import Any

Payload = dict[str, Any]
attr_field = field

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

@define(slots=True)
class ParamsHandler(object):
    # __slots__= ('_nid', 'field', "conds", "order", "axis", "limit", "depth", "is_root", "is_leaf")
    nid: str | None = attr_field(converter=parse_str)
    fields: list[str] | None = attr_field(converter=parse_list, validator=[validate_elm_type])
    conds: list[tuple[str,str,Any]] | None = attr_field(converter=parse_list, validator=[validate_conds])
    order: list[str] | None = attr_field(converter=parse_list, validator=[validate_elm_type])
    axis: int | None = attr_field(converter=parse_int)
    limit: int | None = attr_field(converter=parse_int)
    depth: int | None = attr_field(converter=parse_int)
    is_root: bool | None = attr_field(converter=parse_bool)
    is_leaf: bool | None = attr_field(converter=parse_bool)
    extra_space: bool | None = attr_field(converter=parse_bool)
    style: str | None = attr_field(converter=parse_str)
    
    def __init__(
        self,
        *,
        nid: str | None = None,
        fields: list[str] | None = None,
        conds: list[tuple[str,str,Any]] | None = None,
        order: list[str] | None = None,
        axis: int | None = None,
        limit: int | None = None,
        depth: int | None = None,
        is_root: bool | None = None,
        is_leaf: bool | None = None,
        extra_space: bool | None = None,
        style: str | None = None
        ) -> None:
        self.nid = nid
        self.fields = fields
        self.conds = conds
        self.order = order
        self.axis = axis
        self.limit= limit
        self.depth = depth
        self.is_root = is_root
        self.is_leaf = is_leaf
        self.extra_space = extra_space
        self.style = style
                        
    def to_payload(self) -> Payload:
        return {k:getattr(self, k) for k in self.__slots__ if getattr(self, k, None) not in [None, "__weakref__"]}


if __name__ == "__main__":
    # a = ParamsHandler(conds=[("un", "deux", 3), ("un", 3, 3)])
    pass