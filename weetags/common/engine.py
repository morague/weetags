from __future__ import annotations

import re
from sqlalchemy import MetaData, Table, Column, Index, UniqueConstraint, Result, Executable
from sqlalchemy import create_engine, select, insert, update, delete, text, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import CreateSchema
from sqlalchemy.engine import Engine as BaseEngine

from typing import Any, Generator

from weetags.common.types import AcceptedFieldType
from weetags.common.configs import FieldType
from weetags.common.uri import EngineURI


class Engine:
    uri: EngineURI

    def __init__(self, base_engine: BaseEngine, uri: EngineURI) -> None:
        self.uri = uri
        self.engine = base_engine
        self.sessionmaker = sessionmaker(bind=self.engine)

        self.metadata = MetaData()
        self.reflect()
        if self.uri.dialect == "sqlite":
            self.execute_statement("PRAGMA foreign_keys = ON;")

    @property
    def is_bound(self) -> bool:
        name = getattr(self, "name", None)
        return name is not None

    @property
    def tree_bound(self) -> str | None:
        return getattr(self, "name", None)

    @classmethod
    def from_uri(cls, uri: EngineURI) -> Engine:
        engine = create_engine(uri.uri, echo=False)
        return cls(engine, uri)

    @classmethod
    def from_configs(cls, configs: dict[str, Any]) -> Engine:
        uri = configs.get("uri", {})
        url = EngineURI(**uri)
        return cls.from_uri(url)

    def bind(self, name: str) -> BoundEngine:
        return BoundEngine(name, self.engine, self.uri)
        
    def reflect(self, schema: str | None = None) -> None:
        self.metadata.reflect(self.engine, schema=schema, views=True)

    def execute_statement(self, stmt: str) -> None:
        with self.sessionmaker() as session:
            session.execute(text(stmt))
            session.commit()

    def execute(self, stmt: Executable, args: list[Any] | None = None, /, commit: bool = False) -> Result:
        with self.sessionmaker() as session:
            res = session.execute(stmt, args)
            if commit:
                session.commit()
        return res

    def exist(self, name: str) -> bool:
        self.reflect()
        pattern = re.compile(r"^(main\.)?({name}|_{name}_(topology|metadata))$".format(name=name))
        matches = [bool(re.search(pattern, k)) for k in self.metadata.tables.keys()]
        return len(self.metadata.tables.keys()) > 0 and len(list(filter(lambda x: x is True, matches))) >= 3

    def roots(self, name: str) -> list[dict[str, Any]]:
        table = self._table_or_raise(name)
        stmt = select(table).where(table.c.parent == None)
        data = self.execute(stmt).fetchall()
        return [d._asdict() for d in data]

    def create_schema(self, exist_ok: bool = True) -> None:
        with self.engine.connect() as conn:
            if conn.dialect.has_schema(conn, "main") and exist_ok is False:
                raise ValueError("Schema main already exist.")
            elif conn.dialect.has_schema(conn, "main"):
                return
            conn.execute(CreateSchema("main", if_not_exists=True))
            conn.commit()

    def create_table(
        self, 
        name: str, 
        columns: list[Column], 
        indexes: list[Index] | None = None, 
        unique_constraints: list[UniqueConstraint] | None = None, 
        exist_ok: bool = True
    ) -> Table:
        exist = self.metadata.tables.get(name, None)
        if exist is not None and exist_ok is False:
            raise ValueError(f"Table {name} already exist.")
        
        elif exist is not None:
            return exist

        if indexes is None:
            indexes = []

        if unique_constraints is None:
            unique_constraints = []

        table = Table(name, self.metadata, schema="main", *columns, *indexes, *unique_constraints)
        table.create(self.engine)
        return table

    def _add_field(
        self, 
        tree: str, 
        name: str, 
        dtype: AcceptedFieldType, 
        nullable: bool = True, 
        unique: bool = False, 
        index: bool = False
    ) -> None:
        table = self._table_or_raise(f"_{tree}_metadata")
        field_type = FieldType.from_value(dtype)
        table.append_column(Column(name, field_type.into_sqlalchemy(), nullable=nullable, unique=unique, index=index))

    def _remove_field(self, tree: str, name: str) -> None:
        table = self._table_or_raise(f"_{tree}_metadata")
        field = self._field_or_raise(table, name)
        table._columns.remove(field)

    def _write(self, table: Table, data: dict[str, Any]) -> int:
        stmt = insert(table).values(**data).returning(table.c.id)
        return self.execute(stmt, commit=True).scalar_one()

    def _write_many(self, table: Table, data: list[dict[str, Any]]) -> list[int]:
        stmt = table.insert().returning(table.c.id)
        return list(self.execute(stmt, data, commit=True).scalars().all())

    def _delete(self, table: Table, *nids: int) -> None:
        stmt = delete(table).where(table.c.id.in_(nids))
        self.execute(stmt, commit=True)
 
    def _update(self,  table: Table, nids: list[int], values: dict[str, Any]) -> None:
        self._test_fields(table, list(values.keys()))
        stmt = update(table).values(values).where(table.c.id.in_(nids))
        self.execute(stmt, commit=True)

    def _drop(self, name: str) -> None:
        self.metadata.reflect(self.engine)
        for table in self._tables(name):
            table.drop(self.engine)

    def _table_or_raise(self, name: str) -> Table:
        tables = self.metadata.tables 
        table = tables.get(name, None)
        if table is None:
            table = tables.get("main.{name}", None)
        if table is None:
            raise KeyError(f"Unknown Table: {name}.")
        return table

    def _field_or_raise(self, table: Table, field_name: str) -> Column:
        field = table._columns.get(field_name, None)
        if field is None:
            raise KeyError(f"Unknown field name: {field_name}")
        return field

    def _tables(self, name: str) -> list[Table]:
        def order(x: Table) -> int:
            if x.name.endswith("topology"):
                return 2        
            elif x.name.endswith("metadata"):
                return 1
            else:
                return 0

        pattern = re.compile(r"^(main\.)?({name}|_{name}_(topology|metadata))$".format(name=name))
        return sorted([t for k,t in self.metadata.tables.items() if bool(re.search(pattern, k))], key=order)

    def _test_fields(self, table: Table, fields: list[str]) -> bool:
        columns = [c.name for c in table.columns.values()]
        for f in fields:
            if f not in columns:
                raise KeyError(f"Unknown field: {f}")
        return True

class BoundEngine(Engine):
    uri: EngineURI
    name: str
    tree: Table
    _topology: Table
    _metadata: Table

    def __init__(self, tree_name: str, base_engine: BaseEngine, uri: EngineURI) -> None:
        super().__init__(base_engine, uri)
        self.name = tree_name
        self._topology = self._table_or_raise(f"_{tree_name}_topology")
        self._metadata = self._table_or_raise(f"_{tree_name}_metadata")
        self.tree = self._table_or_raise(tree_name)

    @classmethod
    def binded(cls, tree_name: str, uri: EngineURI) -> BoundEngine:
        return cls.from_uri(uri).bind(tree_name)

    @classmethod
    def from_configs(cls, configs: dict[str, Any]) -> BoundEngine:
        name = configs.get("name", None)
        if name is None:
            raise KeyError(f"Missing key: name.")

        uri = configs.get("uri", {})
        url = EngineURI(**uri)
        return cls.from_uri(url).bind(name)

    def non_ordered_walk(self) -> Generator[dict[str, Any]]:
        for row in self.execute(select(self.tree)).yield_per(1):
            yield row._asdict()

    def subtree(self, path: str) -> list[dict[str, Any]]:
        stmt = select(self.tree).where(self.tree.c.path.like(f"{path}%")).order_by(self.tree.c.path)
        return [r._asdict() for r in self.execute(stmt).fetchall()]

    def subtree_topology(self, name: str) -> list[str]:
        node = self._node_or_raise(name)
        return self.subtree_topology_from_path(node["path"])

    def subtree_topology_from_path(self, path: str) -> list[str]:
        stmt = (
            select(self.tree.c.path)
            .where(
                func.json_array_length(self.tree.c.children) == 0, 
                self.tree.c.path.like(f"{path}%")
            )
            .order_by(self.tree.c.path)
        )
        return list(self.execute(stmt).scalars().all())

    def write_topology(self, data: dict[str, Any]) -> int:
        return self._write(self._topology, data)

    def write_multi_topology(self, data: list[dict[str, Any]]) -> list[int]:
        return self._write_many(self._topology, data)

    def write_metadata(self, data: dict[str, Any]) -> int:
        return self._write(self._metadata, data)

    def write_multi_metadata(self, data: list[dict[str, Any]]) -> list[int]:
        return self._write_many(self._metadata, data)

    def update_topology(self, nids: list[int], values: dict[str, Any]) -> None:
        self._update(self._topology, nids, values)

    def update_metadata(self, nids: list[int], values: dict[str, Any]) -> None:
        self._update(self._metadata, nids, values)

    def delete_topology(self, *nids: int) -> None:
        self._delete(self._topology, *nids)

    def prune(self, name: str) -> None:
        node = self._node_or_raise(name)
        subtree_path_prefix = node["path"]
        subtree = self.subtree(subtree_path_prefix)
        nids = [node["id"]] + [n["id"] for n in subtree]

        parent = node.get("parent", None)
        if parent is not None:
            self._remove_child(parent, name)
        self.delete_topology(*nids)

    def _node(self, name: str) -> dict[str, Any] | None:
        stmt = select(self.tree).where(self.tree.c.name == name)
        node = self.execute(stmt).fetchone()
        if node is not None:
            node = node._asdict()
        return node

    def _node_or_raise(self, name: str) -> dict[str, Any]:
        node = self._node(name)
        if node is None:
            raise ValueError(f"Unknown node name: {name}")
        return node

    def _nodes(self, *names: str) -> list[dict[str, Any]]:
        stmt = select(self.tree).where(self.tree.c.name.in_(names))
        return [n._asdict() for n in self.execute(stmt).fetchall()]

    def _node_from_nid(self, nid: int) -> dict[str, Any] | None:
        stmt = select(self.tree).where(self.tree.c.id == nid)
        node = self.execute(stmt).fetchone()
        if node is not None:
            node = node._asdict()
        return node

    def _node_from_nid_or_raise(self, nid: int) -> dict[str, Any]:
        node = self._node_from_nid(nid)
        if node is None:
            raise ValueError(f"Unknown node nid: {nid}")
        return node

    def _nodes_from_nid(self, *nids: int) -> list[dict[str, Any]]:
        stmt = select(self.tree).where(self.tree.c.id.in_(nids))
        return [n._asdict() for n in self.execute(stmt).fetchall()]

    def _add_child(self, name: str, child: str) -> None:
        node = self._node_or_raise(name)
        children: list[str] = node.get("children", [])
        if child not in children:
            children.append(child)
        self.update_topology([node["id"]], {"children": children})

    def _remove_child(self, name: str, child: str) -> None:
        node = self._node_or_raise(name)
        children: list[str] = node.get("children", [])
        try:
            index = children.index(child)
        except ValueError:
            index = None
        if index is not None:
            children.pop(index)
            self.update_topology([node["id"]], values={"children": children})

    def _replace_child(self, name: str, child: str, child_new_name: str) -> None:
        node = self._node_or_raise(name)
        children: list[str] = node.get("children", [])
        try:
            index = children.index(child)
        except ValueError:
            index = None

        if index is not None:
            children.pop(index)

        children.append(child_new_name)
        self.update_topology([node["id"]], values={"children": children})

    def _replace_parent(self, name: str, new_parent_name: str) -> None:
        node = self._node_or_raise(name)
        self.update_topology([node["id"]], {"parent": new_parent_name})

















