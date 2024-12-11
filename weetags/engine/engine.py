from __future__ import annotations

import json
from sqlite3 import Cursor, Row
from sqlite3 import connect, register_adapter, register_converter
from sqlite3 import PARSE_DECLTYPES

from typing import Any


import weetags.engine.sql as sql
from weetags.engine.sql import _SimpleSqlConverter, SqlConverter, OnConflict
from weetags.engine.schema import SimpleSqlTable, Namespace


Node = dict[str, Any]
Nodes = list[Node]
Conditions = list[list[tuple[str, str, Any] | str] | str]

class TreeEngine:
    tree_name: str
    database: str
    params: dict[str, Any]

    tables: dict[str, Any]
    namespaces: dict[str, Any]

    def __init__(self, tree_name: str, database: str = ":memory:", **params) -> None:
        self.tree_name = tree_name
        self.database = database
        self.params = params
        if database == ":memory":
            self.params.update({"cache":"shared"})

        self.con = connect(self.uri, detect_types=PARSE_DECLTYPES, uri=True)
        self.cursor = self.con.cursor()

        self.con.row_factory = self._record_factory
        register_adapter(list, self._serialize)
        register_adapter(dict, self._serialize)
        register_converter("JSON", self._deserialize) # type: ignore
        register_converter("JSONLIST", self._deserialize) # type: ignore

        self.tables = {}
        self.namespaces = {}

    @classmethod
    def from_pragma(cls, tree_name: str, database: str = ":memory:", **params) -> TreeEngine:
        engine = cls(tree_name, database, **params)
        engine._build_tree_context(tree_name)
        return engine

    @property
    def uri(self) -> str:
        options = ""
        if bool(self.params):
            options = "?" + "&".join([f"{k}={v}" for k,v in self.params.items()])
        return f"file:{self.database}{options}"

    def execute(self, query: str) -> None:
        self.cursor.execute(query)
        self.con.commit()

    def execute_many(self, *queries: str) -> None:
        for query in queries:
            self.cursor.execute(query)
        self.con.commit()

    def write_one(
        self,
        table_name: str,
        target_columns: list[str],
        values: list[Any],
        on_conflict: str,
        commit: bool = True
    ) -> None:
        converter = SqlConverter(
            namespaces=self.namespaces,
            tables=self.tables,
            table_name=table_name,
            target_columns=target_columns,
            values=values,
            on_conflict=OnConflict(on_conflict),
        )
        stmt, values = converter.write_one()
        self.con.execute(stmt, values)
        if commit:
            self.con.commit()

    def write_many(
        self,
        table_name: str,
        target_columns: list[str],
        values: list[list[Any]],
        on_conflict: OnConflict,
        commit: bool = True
    ) -> None:
        converter = SqlConverter(
            namespaces=self.namespaces,
            tables=self.tables,
            table_name=table_name,
            target_columns=target_columns,
            values=values,
            on_conflict=OnConflict(on_conflict),
        )
        stmt, values = converter.write_many()
        self.con.execute(stmt, values)
        if commit:
            self.con.commit()

    def read_one(
        self,
        fields: list[str] | None = None,
        conditions: Conditions | None = None,
        order_by: list[str] | None = None,
        axis: int = 1
    ) -> Node:
        converter = SqlConverter(
            namespaces=self.namespaces,
            tables=self.tables,
            fields=fields,
            conds=conditions,
            order_by=order_by,
            axis=axis
        )
        stmt, values = converter.read_one()
        return self.con.execute(stmt, values).fetchone()

    def read_many(
        self,
        fields: list[str] | None = None,
        conditions: Conditions | None = None,
        order_by: list[str] | None = None,
        limit: int | None = None,
        axis: int = 1,
    ) -> Nodes:
        converter = SqlConverter(
            namespaces=self.namespaces,
            tables=self.tables,
            fields=fields,
            conds=conditions,
            order_by=order_by,
            axis=axis,
            limit=limit
        )
        stmt, values = converter.read_many()
        return self.con.execute(stmt, values).fetchall()

    def update(
        self,
        table_name: str,
        setter: list[tuple[str, Any]],
        conditions: Conditions | None = None,
        commit: bool = True
    ) -> None:
        converter = SqlConverter(
            namespaces=self.namespaces,
            tables=self.tables,
            table_name=table_name,
            conds=conditions,
            setter=setter
        )
        stmt, values = converter.update()
        self.con.execute(stmt, values)
        if commit:
            self.con.commit()

    def _write_many(
        self,
        table_name: str,
        target_columns: list[str],
        values: list[list[Any]],
        commit: bool = True
        ) -> None:
        converter = _SimpleSqlConverter(
            table_name=table_name,
            target_columns=target_columns,
            values=values,
        )
        stmt, values = converter._write_many()
        self.con.execute(stmt, values)
        if commit:
            self.con.commit()

    def _update(
        self,
        table_name: str,
        setter: list[tuple[str, Any]],
        conditions: list[tuple[str, str, Any]] | None = None,
        commit: bool = True    
        ) -> None:
        """primitive update builder. don't use SqlConverter."""
        converter = _SimpleSqlConverter(
            table_name=table_name,
            conds=conditions,
            setter=setter
        )
        stmt, values = converter._update()
        self.con.execute(stmt, values)
        if commit:
            self.con.commit()


    def delete(self, conditions: Conditions | None = None, commit: bool = True) -> None:
        converter = SqlConverter(
            namespaces=self.namespaces,
            tables=self.tables,
            conds=conditions
        )
        stmt, values = converter.delete()
        self.con.execute(stmt, values)
        if commit:
            self.con.commit()

    def drop(self, table_name: str) -> None:
        query = sql.DROP.format(table_name=table_name)
        self.cursor.execute(query)
        self.con.commit()

    def table_info(self, table_name: str) -> list[tuple]:
        query = sql.INFO.format(table_name=table_name)
        return self.cursor.execute(query).fetchall()

    def table_fk_info(self, table_name: str) -> list[tuple]:
        query = sql.FK_INFO.format(table_name=table_name)
        return self.cursor.execute(query).fetchall()

    def table_size(self, table_name: str) -> int:
        query = sql.TABLE_SIZE.format(table_name=table_name)
        return self.cursor.execute(query).fetchone()[0]

    def max_depth(self, table_name: str) -> int:
        query = sql.TREE_DEPTH.format(table_name=table_name)
        return self.cursor.execute(query).fetchone()[0]

    def get_tables(self, tree_name: str) -> list[str]:
        query = sql.TABLE_NAMES.format(tree_name=tree_name)
        return self.cursor.execute(query).fetchall()

    @staticmethod
    def _serialize(data: dict[str, Any] | list[Any]) -> str:
        return json.dumps(data)

    @staticmethod
    def _deserialize(data: str) -> dict[str, Any] | list[Any]:
        return json.loads(data)

    @staticmethod
    def _record_factory(cursor: Cursor, row: Row) -> dict[str, Any]:
        fields = [column[0] for column in cursor.description]
        return {k:v for k,v in zip(fields, row)}
    
    @staticmethod
    def condition_anchor(op: str, values: Any) -> str:
        """define the right anchor for the given condition operator."""
        anchor = "?"
        if op.lower() == "in" and isinstance(values, list):
            anchors = ' ,'.join(["?" for _ in range(len(values))])
            anchor = f"({anchors})"
        return anchor

    def _build_tree_context(self, tree_name: str) -> None:
        """Build tables and namespaces collections from db pragma"""
        self.tables = {}
        self.namespaces = {}

        tables = self.get_tables(tree_name)
        if len(tables) == 0:
            raise ValueError(
                f"tree: {tree_name} is currently not builded.",
                "Consider building the tree first with the TreeBuilder"
            )

        for table in tables:
            table_name = table[0]
            info = self.table_info(table_name)
            fk_info = self.table_fk_info(table_name)
            table_type = table_name.split("__")[1]

            table_repr = SimpleSqlTable.from_pragma(table_name, info, fk_info)
            for fname, f in table_repr.iter_fields:
                current_namespace = self.namespaces.get(fname, None)
                if table_name not in ["metadata", "nodes"] and fname in ("nid", "elm_idx"):
                    continue
                elif current_namespace is None:
                    self.namespaces[fname] = Namespace(
                        table = table_repr.name,
                        index = table_repr.name,
                        fname = fname,
                        ftype = f.dtype
                    )
                else:
                    current_namespace.index_table = table_repr.name
            self.tables[table_type] = table_repr


