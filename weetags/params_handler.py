from ast import literal_eval
from attrs import define, field

from typing import Any

Payload = dict[str, Any]
attr_field = field

def parse_str(value):
    if value is None:
        return None
    return str(value)

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
        return value
    
    if isinstance(str, value) and value in ["true", "1"]:
        return True
    elif isinstance(str, value) and value in ["false", "0"]:
        return False
    else:
        raise ValueError("cannot convert type into bool")

@define(slots=True)
class ParamsHandler(object):
    # __slots__= ('_nid', 'field', "conds", "order", "axis", "limit", "depth", "is_root", "is_leaf")
    nid: str | None = attr_field(converter=parse_str)
    fields: list[Any] | None = attr_field(converter=parse_list)
    conds: list[tuple[str,str,Any]] | None = attr_field(converter=parse_list)
    order: list[str] | None = attr_field(converter=parse_list)
    axis: int | None = attr_field(converter=parse_int)
    limit: int | None = attr_field(converter=parse_int)
    depth: int | None = attr_field(converter=parse_int)
    is_root: bool | None = attr_field(converter=parse_bool)
    is_leaf: bool | None = attr_field(converter=parse_bool)
    
    def __init__(
        self,
        *,
        nid= None,
        fields= None,
        conds=None,
        order=None,
        axis=None,
        limit=None,
        depth=None,
        is_root=None,
        is_leaf=None
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
        
        # for k,v in kwargs.items():
        #     setattr(self, k, v)
                        
    def to_payload(self) -> Payload:
        return {k:getattr(self, k) for k in self.__slots__ if getattr(self, k, None) not in [None, "__weakref__"]}
