from __future__ import annotations

import json
import sqlite3
from sqlite3 import Cursor, Row
from sqlite3 import register_adapter, register_converter
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

    def __init__(self, tree_name: str, database: str = ":memory:", timeout: float = 5, **params) -> None:
        self.tree_name = tree_name
        self.database = database
        self.params = params
        self.timeout = timeout
        if database == ":memory":
            self.params.update({"cache":"shared"})

        self.con = sqlite3.connect(self.uri, detect_types=PARSE_DECLTYPES, uri=True, timeout=timeout)
        self.cursor = self.con.cursor()

        self.con.execute("PRAGMA foreign_keys=ON;")
        self.con.execute("PRAGMA case_sensitive_like=ON;")
        self.con.row_factory = self._record_factory
        register_adapter(list, self._serialize)
        register_adapter(dict, self._serialize)
        register_converter("JSON", self._deserialize) # type: ignore
        register_converter("JSONLIST", self._deserialize) # type: ignore

        self.tables = {}
        self.namespaces = {}

    @classmethod
    def from_pragma(cls, tree_name: str, database: str = ":memory:", timeout: float = 5, **params) -> TreeEngine:
        engine = cls(tree_name, database, **params)
        engine._build_tree_context(tree_name)
        return engine

    @property
    def uri(self) -> str:
        options = ""
        if bool(self.params):
            options = "?" + "&".join([f"{k}={v}" for k,v in self.params.items()])
        return f"file:{self.database}{options}"

    def _execute(self, query: str) -> None:
        self.cursor.execute(query)
        self.con.commit()

    def _execute_many(self, *queries: str) -> None:
        for query in queries:
            self.cursor.execute(query)
        self.con.commit()

    def _create_tables(self, *tables: SimpleSqlTable) -> None:
        queries = [table.create_table() for table in tables]
        self._execute_many(*queries)

    def _create_index(self, table: SimpleSqlTable, field_name: str) -> None:
        self._execute(table.create_index(field_name))

    def _create_json_extract_column(self, table: SimpleSqlTable, target_field: str, path: str) -> None:
        self._execute(table.create_json_extract_column(target_field, path))

    def _create_triggers(self, table: SimpleSqlTable, target_field: str,  path: str | None = None) -> None:
        nodes_table = self.tables["nodes"]
        self._execute(table.create_insert_trigger(target_field, nodes_table._name, path))
        self._execute(table.create_update_trigger(target_field, nodes_table._name, path))
        self._execute(table.create_delete_trigger(nodes_table._name))

    def _write_one(
        self,
        table_name: str,
        target_columns: list[str],
        values: list[Any],
        on_conflict: str = "none",
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

    def _write_many(
        self,
        table_name: str,
        target_columns: list[str],
        values: list[list[Any]],
        on_conflict: str = "none",
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

    def _builder_write_many(
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
        self.con.executemany(stmt, values)
        if commit:
            self.con.commit()

    def _read_one(
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

    def _read_many(
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

    def _update(
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


    def _builder_update(
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

    def _delete(self, conditions: Conditions | None = None, commit: bool = True) -> None:
        converter = SqlConverter(
            namespaces=self.namespaces,
            tables=self.tables,
            conds=conditions
        )
        stmt, values = converter.delete()
        self.con.execute(stmt, values)
        if commit:
            self.con.commit()

    def _drop(self, table_name: str) -> None:
        query = sql.DROP.format(table_name=table_name)
        self.cursor.execute(query)
        self.con.commit()

    def _table_info(self, table_name: str) -> list[tuple]:
        query = sql.INFO.format(table_name=table_name)
        return self.cursor.execute(query).fetchall()

    def _table_fk_info(self, table_name: str) -> list[tuple]:
        query = sql.FK_INFO.format(table_name=table_name)
        return self.cursor.execute(query).fetchall()

    def _table_size(self, table_name: str) -> int:
        query = sql.TABLE_SIZE.format(table_name=table_name)
        return self.cursor.execute(query).fetchone()[0]

    def _max_depth(self, table_name: str) -> int:
        query = sql.TREE_DEPTH.format(table_name=table_name)
        return self.cursor.execute(query).fetchone()[0]

    def _get_tables(self, tree_name: str) -> list[str]:
        query = sql.TABLE_NAMES.format(tree_name=tree_name)
        return self.cursor.execute(query).fetchall()

    def _get_children_from_ids(self, nodes_table: str, ids: list[str]) -> list[tuple[str, list[str]]]:
        converter = _SimpleSqlConverter(table_name=nodes_table, values=ids)
        query, values = converter._children_from_ids()
        return self.con.execute(query, values).fetchall()

    def _get_children_from_id(self, nodes_table: str, id: str):
        converter = _SimpleSqlConverter(table_name=nodes_table, values=[id])
        query, values = converter._children_from_id()
        return self.con.execute(query, values).fetchone()

    def _get_user(self, username: str) -> dict[str, Any] | None:
        return self.con.execute(sql.GET_USER, [username]).fetchone()

    def _get_restriction(self, tree_name: str, blueprint: str) -> dict[str, Any] | None:
        return self.con.execute(sql.GET_RESTRICTION, [tree_name, blueprint]).fetchone()

    def _add_users(self, *users) -> None:
        for settings in users:
            converter = _SimpleSqlConverter(
                table_name="weetags__users", 
                target_columns=settings.fields, 
                values=[settings.values]
            )
            stmt, values = converter._write_many()
            self.con.executemany(stmt, values)
        self.con.commit()

    def _add_restrictions(self, *restrictions) -> None:
        for settings in restrictions:
            converter = _SimpleSqlConverter(
                table_name="weetags__restrictions", 
                target_columns=settings.fields, 
                values=[settings.values]
            )
            stmt, values = converter._write_many()
            self.con.executemany(stmt, values)
        self.con.commit()

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

        tables = self._get_tables(tree_name)
        if len(tables) == 0:
            raise ValueError(
                f"tree: {tree_name} is currently not builded.",
                "Consider building the tree first with the TreeBuilder"
            )

        for table in tables:
            table_name = table[0]
            info = self._table_info(table_name)
            fk_info = self._table_fk_info(table_name)
            table_type = table_name.split("__")[1]

            table_repr = SimpleSqlTable.from_pragma(table_name, info, fk_info)
            for fname, f in table_repr.iter_fields:
                current_namespace = self.namespaces.get(fname, None)
                if table_type not in ["metadata", "nodes"] and fname in ("nid", "elm_idx"):
                    continue
                elif current_namespace is None:
                    self.namespaces[fname] = Namespace(
                        table = table_repr._name,
                        index = table_repr._name,
                        fname = fname,
                        ftype = f.dtype
                    )
                else:
                    current_namespace.index_table = table_repr._name
            self.tables[table_type] = table_repr
