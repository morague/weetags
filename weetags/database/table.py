from __future__ import annotations

from attrs import define, field, Attribute, validators, asdict
from typing import Optional, TypeVar, Generator, Any

# from weetags.database.db import _Db


class Field(object):
    def __init__(
        self,
        name: str,
        dtype: str,
        pk: bool =  False,
        fk: Optional[str] | None = None,
        nullable: bool = True,
        unique: bool = False,
        serial: bool = False
        ) -> None:
        self.name = name
        self.dtype = dtype
        self.pk = pk
        self.fk = fk
        self.nullable = nullable
        self.unique = unique
        self.serial = serial

    def __repr__(self) -> str:
        return f"Field({self.name}, {self.dtype})"

    def to_sql(self) -> str:
        return f"{self.name} {self.dtype} {self._serial()} {self._unique()} {self._non_null()}"

    def to_fk(self) -> str:
        table_name, field_name = self.fk.split('.')
        return f"FOREIGN KEY ({self.name}) REFERENCES {table_name}({field_name}) ON DELETE CASCADE"

    def _non_null(self) -> str:
        s = ""
        if self.nullable is False:
            s = "NOT NULL"
        return s

    def _unique(self) -> str:
        s = ""
        if self.unique:
            s = "UNIQUE"
        return s

    def _serial(self) -> str:
        if self.serial and self.dtype != "INTEGER":
            raise ValueError()
        s = ""
        if self.serial:
            s = "AUTOINCREMENT"
        return s

@define(slots=False)
class Table(object):
    table_name: str = field()

    def __repr__(self) -> str:
        fields = "\n  ".join([str(f) for f in self.__dict__.values() if isinstance(f, Field)])
        return f"Table({self.table_name}\n  {fields})"

    @classmethod
    def initialize(cls):
        return cls()

    @classmethod
    def from_pragma(
        cls,
        table_name: str,
        table_info: list[tuple],
        fk_info: list[tuple]
        ) -> Table:
        fks = {f[3]:f"{f[2]}.{f[4]}" for f in fk_info}
        table = cls(table_name=table_name)
        for field in table_info:
            setattr(table, field[1], Field(field[1], field[2],pk=bool(field[5]),fk=fks.get(field[1], None)))
        return table

    def validate_node(self, node: dict[str, Any]) -> dict[str, Any]:
        type_map = {"TEXT": str, "INTEGER": int, "JSONLIST": list, "JSON": dict, "BOOL": bool}
        for fname, value in node.items():
            field = getattr(self, fname, None)
            if field is None:
                raise ValueError()
            elif isinstance(value, type_map[field.dtype]) is False:
                raise ValueError()

        parent = node.get("parent", False)
        children = node.get("children", None)
        if parent is False: # none  is for root
            raise ValueError()
        if children is None:
            node.update({"children": []})
        return node

    def validate_node_update(self, update: dict[str, Any]) -> dict[str, Any]:
        type_map = {"TEXT": str, "INTEGER": int, "JSON": list, "BOOL": bool}
        for fname, value in update.items():
            field = getattr(self, fname, None)
            if field is None:
                raise ValueError()
            elif isinstance(value, type_map[field.dtype]) is False:
                raise ValueError()
        return update

    def fields(self) -> Generator:
        for k,attr in self.__dict__.items():
            if isinstance(attr, Field):
                yield (k, attr)

    def create(self) -> str:
        fields, pk, fk = [], [], []
        for v in self.__dict__.values():
            if isinstance(v, Field):
                fields.append(v.to_sql())
                if v.pk:
                    pk.append(v.name)
                if v.fk:
                    fk.append(v.to_fk())
        data = ", ".join(list(filter(None, fields + fk + [self.pk_to_sql(pk)])))
        base = f"""CREATE TABLE  IF NOT EXISTS {self.table_name} ({data});"""
        return base

    def add_index(self, field_name: str) -> str:
        base = f"CREATE INDEX IF NOT EXISTS idx_{self.table_name}_{field_name} ON {self.table_name}({field_name});"
        return base

    def add_indexing_column(self, target_field: str, path: str) -> str:
        elm_path = f"$.{path}"
        if path.startswith("["):
            elm_path = f"${path}"
        base = f"ALTER TABLE {self.table_name} ADD COLUMN {target_field}_{path} TEXT AS (json_extract({target_field}, '{elm_path}'))"
        return base

    def add_insert_trigger(self, target_field: str, target_table: Optional[str] | None = None, path: Optional[str] | None = None) -> str:
        if path is not None:
            trigger = f"INSERT INTO {self.table_name}({target_field}_{path}) SELECT j.value FROM json_each(NEW.json, {path}) as j;"
        elif path is None and target_table:
            trigger = f"""
            INSERT INTO {self.table_name}({target_field}, nid, elm_idx)
            SELECT j.value, {target_table}.id, j.key FROM {target_table}, json_each(NEW.{target_field}) as j WHERE {target_table}.id = NEW.id;
            """
        else:
            raise ValueError()

        base = f"""
        CREATE TRIGGER {self.table_name}__insert_trigger AFTER INSERT ON {target_table} BEGIN
        {trigger}
        END;
        """
        return base


    def add_delete_trigger(self, target_table: str) -> str:
        trigger = f"DELETE FROM {self.table_name} where nid = OLD.id;"
        base = f"""
        CREATE TRIGGER {self.table_name}__delete_trigger AFTER DELETE ON {target_table} BEGIN
        {trigger}
        END;
        """
        return base

    def add_update_trigger(
        self,
        target_field: str,
        target_table: Optional[str] | None = None,
        path: Optional[str] | None = None
        ) -> str:
        if path is not None and target_table is None:
            target_table = self.table_name
            trigger = f"SET {target_field}_{path} = json_extract(NEW.json, {path});"
        elif path is None and target_table:
            trigger = f"""
            DELETE FROM {self.table_name} WHERE nid = OLD.id;
            INSERT INTO {self.table_name}({target_field}, nid, elm_idx)
            SELECT j.value, {target_table}.id, j.key FROM {target_table}, json_each(NEW.{target_field}) as j WHERE {target_table}.id = NEW.id;
            """
        base = f"""
        CREATE TRIGGER {self.table_name}__update_on_{target_field}_trigger AFTER UPDATE OF {target_field} ON {target_table} BEGIN
        {trigger}
        END;
        """
        return base

    def pk_to_sql(self, pk: list[str]):
        s = ""
        if pk:
            s = f"PRIMARY KEY ({', '.join(pk)})"
        return s
