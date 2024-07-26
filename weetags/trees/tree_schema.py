from __future__ import annotations

from attrs import field, define
from weetags.database.table import Table, Field

@define(slots=False, repr=False)
class NodesTable(Table):
    table_name: str = field()
    id: Field = field(default=Field("id", "TEXT", pk=True))
    parent: Field = field(default=Field("parent", "TEXT"))
    children: Field = field(default=Field("children", "JSON"))

    def __repr__(self) -> str:
        return super().__repr__()
    
    @classmethod
    def initialize(cls, name:str, **fields:Field) -> NodesTable:
        table_name = f"{name}__nodes"
        table = cls(table_name)
        for k,v in fields.items():
            setattr(table, k, cls.validate_field(v))
        return table
    
    @classmethod
    def validate_field(cls, v: Field) -> Field:
        if isinstance(v, Field) is False:
            raise ValueError()
        return v
    
    @id.validator
    def validate_field_id(self, attr, v) -> None:
        if isinstance(v, Field) is False:
            raise ValueError()
        
        if v.name != "id":
            raise ValueError()
        
        if v.dtype != "TEXT":
            raise ValueError()
        
    @parent.validator
    def validate_field_parent(self, attr, v) -> None:
        if isinstance(v, Field) is False:
            raise ValueError()
        
        if v.name != "parent":
            raise ValueError()
        
        if v.dtype != "TEXT":
            raise ValueError()
        
    @children.validator
    def validate_field_children(self, attr, v) -> None:
        if isinstance(v, Field) is False:
            raise ValueError()
        
        if v.name != "children":
            raise ValueError()
        
        if v.dtype != "JSON":
            raise ValueError()


@define(slots=False)
class MetadataTable(Table):
    table_name: str = field()
    nid: Field = field(default=Field("nid", "TEXT", pk=True))
    depth: Field = field(default=Field("depth", "INTEGER"))
    is_root: Field = field(default=Field("is_root", "BOOL"))
    is_leaf: Field = field(default=Field("is_leaf", "BOOL"))

    def __repr__(self) -> str:
        return super().__repr__()

    @classmethod
    def initialize(cls, name: str) -> Table:
        nodes_table = f"{name}__nodes"
        return cls(
            table_name=f"{name}__metadata",
            nid=Field("nid", "TEXT", pk=True, fk=f"{nodes_table}.id")
        )

@define(slots=False)
class IndexTable(Table):
    table_name: str = field()
    nid: Field = field()
    value: Field = field()
    elm_idx: Field = field(default=Field("elm_idx", "INTEGER", pk=True))

    def __repr__(self) -> str:
        return super().__repr__()

    @classmethod
    def initialize(cls, name: str, field_name: str, field_dtype: str) -> Table:
        table_name=f"{name}__{field_name}"
        nodes_table = f"{name}__nodes"
        table = cls(
            table_name= table_name,
            nid= Field("nid", "TEXT", fk=f"{nodes_table}.id", pk=True),
            value= Field(field_name, field_dtype, pk=True)
        )
        return table
    
    @nid.validator
    def validate_field_nid(self, attr, v) -> None:
        if isinstance(v, Field) is False:
            raise ValueError()
        
        if v.name != "nid":
            raise ValueError()
        
        if v.dtype != "TEXT":
            raise ValueError()

    @elm_idx.validator
    def validate_field_idx(self, attr, v) -> None:
        if isinstance(v, Field) is False:
            raise ValueError()
        
        if v.name != "elm_idx":
            raise ValueError()
        
        if v.dtype != "INTEGER":
            raise ValueError()
        
    @value.validator
    def validate_field_value(self, attr, v) -> None:
        if isinstance(v, Field) is False:
            raise ValueError()    
    