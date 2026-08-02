from __future__ import annotations

import re
from functools import wraps
from sqlalchemy import MetaData, Table, Column
from sqlalchemy import create_engine, select, insert, update, delete, text, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import CreateSchema
from sqlalchemy.engine import Engine as BaseEngine

from typing import Any, Generator

from weetags.common.uri import EngineURI
from weetags.common.query import QueryBuilder



def structured(f):
    @wraps(f)
    def wrapper(instance: Engine, *args: Any, **kwargs: Any):
        if instance.is_structured() is False:
            raise AttributeError("Unable to used engine structured methods. Tree is not laoded in the instance.", name="tree", obj=instance)
        return f(instance, *args, **kwargs)
    return wrapper



class Engine:
    uri: EngineURI

    def __init__(self, base_engine: BaseEngine, uri: EngineURI):
        self.uri = uri
        self.engine = base_engine
        self.sessionmaker = sessionmaker(bind=self.engine)

        self.metadata = MetaData()
        self.reflect()
        if self.uri.dialect == "sqlite":
            self.execute_statement("PRAGMA foreign_keys = ON;")


    @classmethod
    def from_uri(cls, uri: EngineURI) -> Engine:
        engine = create_engine(uri.uri, echo=False)
        return cls(engine, uri)

    @classmethod
    def from_configs(cls, uri: dict[str, Any]) -> Engine:
        url = EngineURI(**uri)
        return cls.from_uri(url)

    def execute_statement(self, stmt: str) -> None:
        with self.sessionmaker() as session:
            session.execute(text(stmt))
            session.commit()

    def is_structured(self) -> bool:
        has_topology = getattr(self, "_topology", None) is not None
        has_meta = getattr(self, "_metadata", None) is not None
        has_tree = getattr(self, "tree", None) is not None
        return all([has_topology, has_meta, has_tree])


    def reflect(self, schema: str | None = None) -> None:
        self.metadata.reflect(self.engine, schema=schema, views=True)

    def get_tree(self, name: str) -> tuple[Table, Table, Table]:
        self._topology = self._get_table(f"_{name}_topology")
        self._metadata = self._get_table(f"_{name}_metadata")
        self.tree = self._get_table(name)
        return (self._topology, self._metadata, self.tree)

    def exist(self, name: str) -> bool:
        self.reflect()
        pattern = re.compile(r"^(main\.)?({name}|_{name}_(topology|metadata))$".format(name=name))

        matches = [bool(re.search(pattern, k)) for k in self.metadata.tables.keys()]
        return len(self.metadata.tables.keys()) > 0 and len(list(filter(lambda x: x is True, matches))) >=3

    def _drop_tree(self, name: str) -> None:
        ...

    def _roots(self, tree: str) -> list[dict[str, Any]]:
        table = self.metadata.tables.get(tree)
        if table is None:
            raise KeyError(f"Unknown tree: {tree}.")
        
        stmt = select(table).where(table.c.parent == None)
        with self.sessionmaker() as session:
            data = session.execute(stmt).fetchall()
        return [d._asdict() for d in data]



    def _delete(self, table: Table, *nids: int) -> None:
        stmt = delete(table).where(table.c.id.in_(nids))
        with self.sessionmaker() as session:
            session.execute(stmt)
            session.commit()

    @structured
    def _delete_topology(self, *nids: int) -> None:
        self._delete(self._topology, *nids)

    @structured
    def _prune(self, name: str) -> None:
        node = self._node_from_name(name)
        if node is None:
            raise KeyError(f"Unknown node name {name}")
        subtree_path_prefix = node["path"]
        subtree = self.subtree(subtree_path_prefix)
        nids = [node["id"]] + [n["id"] for n in subtree]

        parent = node.get("parent", None)
        if parent is not None:
            self._remove_child(parent, name)
        self._delete_topology(*nids)

    @structured
    def _delete_where(self, conditions: list) -> None:
        stmt = (
            QueryBuilder(self.metadata)
            .tree(self.tree)
            .fields(self.tree.c.id)
            .where_from_str(conditions).read
        )
        with self.sessionmaker() as session:
            nodes = session.execute(stmt).fetchall()
        nids = [n._asdict()["id"] for n in nodes]
        self._delete_topology(*nids)

    def _update(self,  table: Table, nids: list[int], values: dict[str, Any]) -> None:
        columns = [c.name for c in table.columns.values()]
        for k in values.keys():
            if k not in columns:
                raise KeyError(f"Unknown field: {k}")
        
        stmt = update(table).values(values).where(table.c.id.in_(nids))
        with self.sessionmaker() as session:
            session.execute(stmt)
            session.commit()

    @structured
    def _update_topology(self, nids: list[int], values: dict[str, Any]) -> None:
        self._update(self._topology, nids, values)

    @structured
    def _update_metadata(self, nids: list[int], values: dict[str, Any]) -> None:
        self._update(self._metadata, nids, values)

    @structured    
    def _add_child(self, name: str, child: str) -> None:
        node = self._node_from_name(name)
        if node is None:
            raise KeyError(f"Unknown node name: {name}")
        children: list[str] = node.get("children", [])
        if child not in children:
            children.append(child)
        self._update_topology([node["id"]], {"children": children})

    @structured
    def _remove_child(self, name: str, child: str) -> None:
        node = self._node_from_name(name)
        if node is None:
            raise KeyError(f"Unknown node name: {name}")
        children: list[str] = node.get("children", [])
        try:
            index = children.index(child)
        except ValueError:
            index = None
        if index is not None:
            children.pop(index)
            self._update_topology([node["id"]], values={"children": children})

    @structured
    def _replace_child(self, name: str, child: str, child_new_name: str) -> None:
        node = self._node_from_name(name)
        if node is None:
            raise KeyError(f"Unknown node name: {name}")
        children: list[str] = node.get("children", [])
        try:
            index = children.index(child)
        except ValueError:
            index = None

        if index is not None:
            children.pop(index)

        children.append(child_new_name)
        self._update_topology([node["id"]], values={"children": children})

    @structured
    def _replace_parent(self, name: str, new_parent_name: str) -> None:
        node = self._node_from_name(name)
        if node is None:
            raise KeyError(f"Unknown node name: {name}")
        self._update_topology([node["id"]], {"parent": new_parent_name})


    # MUST HAVE TREE TABLE METADATA

    @structured
    def sub_tree_topology_from_name(self, name: str) -> list[str]:
        node = self._node_from_name(name)
        if node is None:
            raise ValueError(f"Unknown node name: {name}")
        return self.sub_tree_topilogy_from_path(node["path"])

    @structured
    def sub_tree_topilogy_from_path(self, path: str) -> list[str]:
        stmt = select(self.tree.c.path).where(func.json_array_length(self.tree.c.children) == 0, self.tree.c.path.like(f"{path}%")).order_by(self.tree.c.path)
        with self.sessionmaker() as session:
            topological_order = list(session.execute(stmt).scalars().all())
        return topological_order

    @structured
    def non_ordered_walk(self) -> Generator[dict[str, Any]]:
        stmt = select(self.tree)
        with self.sessionmaker() as session:
            for row in session.execute(stmt).yield_per(1):
                yield row._asdict()

    @structured
    def _node_from_name(self, name: str) -> dict[str, Any] | None:
        stmt = select(self.tree).where(self.tree.c.name == name)
        with self.sessionmaker() as session:
            data = session.execute(stmt).fetchone()
        if data is not None:
            data = data._asdict()
        return data

    @structured
    def _node_from_id(self, nid: int) -> dict[str, Any] | None:
        stmt = select(self.tree).where(self.tree.c.id == nid)
        with self.sessionmaker() as session:
            data = session.execute(stmt).fetchone()
        if data is not None:
            data = data._asdict()
        return data

    @structured
    def _nodes_from_name(self, *name: str) -> list[dict[str, Any]]:
        stmt = select(self.tree).where(self.tree.c.name.in_(name))

        with self.sessionmaker() as session:
            data = session.execute(stmt).fetchall()
        return [d._asdict() for d in data]

    @structured
    def _nodes_from_id(self, *nid: int) -> list[dict[str, Any]]:
        stmt = select(self.tree).where(self.tree.c.id.in_(nid))

        with self.sessionmaker() as session:
            data = session.execute(stmt).fetchall()
        return [d._asdict() for d in data]

    @structured
    def _write_topology(self, data: dict[str, Any]) -> int:
        return self._write(self._topology, data)

    @structured
    def _write_multi_topology(self, data: list[dict[str, Any]]) -> list[int]:
        return self._write_many(self._topology, data)

    @structured
    def _write_metadata(self, data: dict[str, Any]) -> int:
        return self._write(self._metadata, data)

    @structured
    def _write_multi_metadata(self, data: list[dict[str, Any]]) -> list[int]:
        return self._write_many(self._metadata, data)

    @structured
    def subtree(self, path: str) -> list[dict[str, Any]]:
        stmt = select(self.tree).where(self.tree.c.path.like(f"{path}%")).order_by(self.tree.c.path)
        with self.sessionmaker() as session:
            res = session.execute(stmt).fetchall()
        return [r._asdict() for r in res]




    def _update_where(self, table: Table, values: dict[str, Any], conditions: list) -> None:
        stmt = QueryBuilder(self.metadata).tree(table).values(values).where_from_str(conditions)
        with self.sessionmaker() as session:
            session.execute(stmt.update)
            session.commit()

    def _write(self, table: Table, data: dict[str, Any]) -> int:
        with self.sessionmaker() as session:
            stmt = insert(table).values(**data).returning(table.c.id)
            res = session.execute(stmt).scalar_one()
            session.commit()
        return res
    
    def _write_many(self, table: Table, data: list[dict[str, Any]]) -> list[int]:
        with self.sessionmaker() as session:
            stmt = table.insert().returning(table.c.id)
            res = session.execute(stmt, data).scalars().all()
            session.commit()
        return list(res)

    def _create_table(self, name: str, columns: list[Column], exist_ok: bool = True) -> Table:
        exist = self.metadata.tables.get(name, None)
        if exist is not None and exist_ok is False:
            raise ValueError(f"Table {name} already exist.")
        elif exist is not None:
            return exist

        table = Table(name, self.metadata, schema="main", *columns)
        table.create(self.engine)
        return table

    def _create_schema(self, exist_ok: bool = True) -> None:
        with self.engine.connect() as conn:
            if conn.dialect.has_schema(conn, "main") and exist_ok is False:
                raise ValueError("Schema main already exist.")
            elif conn.dialect.has_schema(conn, "main"):
                return
            conn.execute(CreateSchema("main", if_not_exists=True))
            conn.commit()

    def _get_table(self, table_name: str) -> Table:
        tables = self.metadata.tables 

        table = tables.get(table_name, None)
        if table is None:
            table = tables.get(f"main.{table_name}", None)
        if table is None:
            raise KeyError(f"Tree Structure not found: {table_name}")
        return table