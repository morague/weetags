from __future__ import annotations

import random
from abc import ABC
from hashlib import sha256
from pathlib import Path
from string import ascii_letters, digits
from attrs import define, field, validators


from typing import Any, Iterator

import weetags.engine.sql as sql

StrOrPath = str | Path
Users = Restricions = list[dict[str, Any]] | None

CHARS = ascii_letters + digits

@define(slots=True, kw_only=True)
class User:
    username: str = field(validator=[validators.instance_of(str)])
    password: str = field(validator=[validators.instance_of(str)])
    password_sha256: str = field(default=None)
    auth_level: list[str] = field()
    salt: str = field(default=None)
    max_age: int = field(validator=[validators.instance_of(int)])

    def __attrs_post_init__(self):
        if self.password_sha256 is None and self.salt is None:
            self.generate_salt()
            self.generate_sha256()

    def generate_salt(self, size: int = 16):
        self.salt = "".join([random.choice(ascii_letters) for _ in range(size)])

    def generate_sha256(self):
        salted_password = self.salt + self.password
        self.password_sha256 = sha256(salted_password.encode()).hexdigest()

    @property
    def fields(self) -> list[str]:
        return ["username", "password_sha256", "auth_level", "salt", "max_age"]

    @property
    def values(self) -> list[Any]:
        return [self.username, self.password_sha256, self.auth_level, self.salt, self.max_age]

@define(slots=True, kw_only=True)
class Restriction:
    tree: str = field(validator=[validators.instance_of(str)])
    blueprint: str = field(validator=[validators.instance_of(str)])
    auth_level: list[str] = field(validator=[validators.instance_of(list)])

    def create_restriction(self) -> tuple[str, list[Any]]:
        ...

    @property
    def fields(self) -> list[str]:
        return ["tree", "blueprint", "auth_level"]

    @property
    def values(self) -> list[Any]:
        return [self.tree, self.blueprint, self.auth_level]


@define(kw_only=True, slots=False)
class Namespace:
    table: str = field(validator=[validators.instance_of(str)])
    index: str = field(validator=[validators.instance_of(str)])
    fname: str = field(validator=[validators.instance_of(str)])
    ftype: str = field(validator=[validators.instance_of(str)])

    def is_joinable(self) -> bool:
        return self.index.split("__")[1] != "nodes"

    def is_metadata(self) -> bool:
        return self.table.split("__")[1] == "metadata"

    def select(self) -> str:
        return f"{self.table}.{self.fname}"

    def join(self, to_table: str) -> str:
        if self.index.endswith("nodes"):
            raise KeyError("__nodes is not joinable")
        return f"JOIN {self.index} ON {to_table}.id = {self.index}.nid"

    def where(self, op: str, values: Any) -> tuple[str, Any]:
        fname, op, values = self._prepare(op, values)
        return ((f"{fname} {op} {self._set_anchor(op, values)}", values))

    def _prepare(self, op: str, values: Any) -> tuple[str, Any]:
        if op.lower() == "ilike" and isinstance(values, list):
            v = []
            for val in values:
                if not isinstance(val, str):
                    raise ValueError(f"ILIKE operator must compare Strings. `{values}` is not a string")
                v.append(val.upper())
            return (f"UPPER({self.index}.{self.fname})", "LIKE", v)
        elif op.lower() == "ilike" and isinstance(values, str):
            return (f"UPPER({self.index}.{self.fname})", "LIKE", values.upper())
        elif op.lower() == "ilike":
            raise ValueError(f"ILIKE operator must compare Strings. `{values}` is not a string")
        else:
            return (f"{self.index}.{self.fname}", op, values)

    def _set_anchor(self, op: str, values: Any) -> str:
        if op.lower() == "in" and isinstance(values, list):
            return f"({' ,'.join(['?' for _ in range(len(values))])})"
        elif op.lower() == "in" and not isinstance(values, list):
            return "(?)"
        else:
            return "?"

@define(slots=False)
class SimpleSqlField:
    name: str = field()
    dtype: str = field()
    pk: bool = field(default=False)
    fk: str | None = field(default=None)
    nullable: bool = field(default=True)
    unique: bool = field(default=False)
    serial: bool = field(default=False)

    @property
    def foreign_key(self) -> str:
        if self.fk is None:
            raise ValueError("This field has no FK bound")
        table_name, field_name = self.fk.split('.')
        return f"FOREIGN KEY ({self.name}) REFERENCES {table_name}({field_name}) ON DELETE CASCADE"

    def to_sql(self) -> str:
        return f"{self.name} {self.dtype} {self._options()}"

    def _options(self) -> str:
        sql = {
            ("nullable", False): "NOT NULL",
            ("unique", True): "UNIQUE",
            ("serial", True): "AUTOINCREMENT"
        }
        options = []
        for k,v in sql.items():
            field_value = getattr(self, k[0], None)
            sql_value = sql.get((k[0], field_value), None) # type: ignore
            if sql_value is not None:
                options.append(v)
        return " ".join(options)




@define(slots=False)
class SimpleSqlTable(ABC):
    _name: str = field()

    @classmethod
    def from_pragma(cls, _name: str, table_info: list[tuple], fk_info: list[tuple]) -> SimpleSqlTable:
        table = cls(_name)
        fks = {f[3]:f"{f[2]}.{f[4]}" for f in fk_info}
        for f in table_info: # iterate over sql fields
            simple_field = SimpleSqlField(f[1], f[2],pk=bool(f[5]),fk=fks.get(f[1], None))
            setattr(table, f[1], simple_field)
        return table

    @property
    def fields(self) -> dict[str, SimpleSqlField]:
        return {k:attr for k,attr in self.__dict__.items() if isinstance(attr, SimpleSqlField)}

    @property
    def iter_fields(self) -> Iterator[tuple[str, SimpleSqlField]]:
        for k,attr in self.__dict__.items():
            if isinstance(attr, SimpleSqlField):
                yield (k, attr)

    def create_table(self) -> str:
        fields, pk, fk = [], [], []
        for _, f in self.iter_fields:
            fields.append(f.to_sql())
            if f.fk is not None:
                fk.append(f.foreign_key)
            if f.pk is not False:
                pk.append(f.name)
        data = ", ".join(fields + [sql.pk_to_sql(pk)] + fk)
        return sql.CREATE_TABLE.format(table_name=self._name, fields=data)

    def create_index(self, field_name: str) -> str:
        return sql.CREATE_INDEX.format(table_name=self._name, field_name=field_name)

    def create_json_extract_column(self, target_field: str, path: str) -> str:
        return sql.CREATE_EXTRACT_COLUMN.format(table_name=self._name, target_field=target_field, path=path)

    def create_insert_trigger(self, target_field: str, target_table: str | None = None, path: str | None = None) -> str:
        if path is not None:
            base = path.split(".")[0]
            inner_path = ".".join(path.split(".")[1:])
            fname = path.replace(".", "_")
            trigger = sql.ADD_JSON_TRIGGER.format(table_name=self._name, target_table=target_table, target_field=fname, base=base, path=inner_path)
        elif path is None and target_table:
            trigger = sql.ADD_JSONLIST_TRIGGER.format(table_name=self._name, target_table=target_table, target_field=target_field)
        else:
            raise ValueError("Cannot create insert trigger.")
        return sql.CREATE_TRIGGER.format(table_name=self._name, target_table=target_table, trigger=trigger)

    def create_delete_trigger(self, target_table: str) -> str:
        return sql.DELETE_TRIGGER.format(table_name=self._name, target_table=target_table)

    def create_update_trigger(self, target_field: str, target_table: str | None = None, path: str | None = None) -> str:
        if path is not None:
            base = path.split(".")[0]
            inner_path = ".".join(path.split(".")[1:])
            fname = path.replace(".", "_")
            trigger = sql.UPDATE_JSON_TRIGGER.format(table_name=self._name, target_table=target_table, target_field=fname, base=base, path=inner_path)
        elif path is None and target_table:
            trigger = sql.UPDATE_JSONLIST_TRIGGER.format(table_name=self._name, target_table=target_table, target_field=target_field)
        else:
            raise ValueError("Cannot Create update trigger")
        return sql.UPDATE_TRIGGER.format(table_name=self._name, target_table=target_table, target_field=target_field, trigger=trigger)


@define(slots=False)
class NodesTable(SimpleSqlTable):
    _name: str = field()
    id: SimpleSqlField = field(default=SimpleSqlField("id", "TEXT", pk=True, nullable=False, unique=True))
    parent: SimpleSqlField = field(default=SimpleSqlField("parent", "TEXT"))
    children: SimpleSqlField = field(default=SimpleSqlField("children", "JSONLIST", nullable=False))

    @classmethod
    def initialize(cls, _name: str, **fields: SimpleSqlField) -> NodesTable:
        table_name = f"{_name}__nodes"
        table = cls(table_name)
        for k,v in fields.items():
            if k in table.__dict__.keys():
                raise ValueError("Table Field already exist")
            setattr(table, k, cls.validate_field(v))
        return table

    @classmethod
    def validate_field(cls, v: SimpleSqlField) -> SimpleSqlField:
        if isinstance(v, SimpleSqlField) is False:
            raise ValueError("Field must be of type: SimpleSqlField")
        return v

@define(slots=False)
class MetadataTable(SimpleSqlTable):
    _name: str = field()
    nid: SimpleSqlField = field(default=SimpleSqlField("nid", "TEXT", pk=True, unique=True))
    depth: SimpleSqlField = field(default=SimpleSqlField("depth", "INTEGER", nullable=False))
    is_root: SimpleSqlField = field(default=SimpleSqlField("is_root", "BOOL", nullable=False))
    is_leaf: SimpleSqlField = field(default=SimpleSqlField("is_leaf", "BOOL", nullable=False))

    @classmethod
    def initialize(cls, _name: str) -> MetadataTable:
        nodes_table = f"{_name}__nodes"
        return cls(
            f"{_name}__metadata",
            nid=SimpleSqlField("nid", "TEXT", pk=True, fk=f"{nodes_table}.id")
        )

@define(slots=False)
class IndexTable(SimpleSqlTable):
    _name: str = field()
    nid: SimpleSqlField = field()
    value: SimpleSqlField = field()
    elm_idx: SimpleSqlField = field(default=SimpleSqlField("elm_idx", "INTEGER", pk=True, nullable=False))

    @classmethod
    def initialize(cls, _name: str, field_name: str, field_dtype: str) -> IndexTable:
        table_name=f"{_name}__{field_name}"
        nodes_table = f"{_name}__nodes"
        table = cls(
            table_name,
            nid= SimpleSqlField("nid", "TEXT", fk=f"{nodes_table}.id", pk=True, nullable=False),
            value= SimpleSqlField(field_name, field_dtype, pk=True)
        )
        return table

@define(slots=False)
class UsersTable(SimpleSqlTable):
    _name: str = field(default="weetags__users")
    username: SimpleSqlField = field(default=SimpleSqlField("username", "TEXT", pk=True, nullable=False))
    password_sha256: SimpleSqlField = field(default=SimpleSqlField("password_sha256", "TEXT", nullable=False))
    auth_level: SimpleSqlField = field(default=SimpleSqlField("auth_level", "JSONLIST", nullable=False))
    salt: SimpleSqlField = field(default=SimpleSqlField("salt", "TEXT", nullable=False))
    max_age: SimpleSqlField = field(default=SimpleSqlField("max_age", "INTEGER", nullable=False))

@define(slots=False)
class RestrictionsTable(SimpleSqlTable):
    _name: str = field(default="weetags__restrictions")
    tree: SimpleSqlField = field(default=SimpleSqlField("tree", "TEXT", pk=True, nullable=False))
    blueprint: SimpleSqlField = field(default=SimpleSqlField("blueprint", "TEXT", pk=True, nullable=False))
    auth_level: SimpleSqlField = field(default=SimpleSqlField("auth_level", "JSONLIST", nullable=False))
