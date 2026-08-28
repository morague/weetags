from __future__ import annotations


import json
from abc import ABC
from typing import Callable
from sqlalchemy import func, update, select

from typing import Any

from weetags.common.engine import BoundEngine

class JsonObjectHandler(ABC):
    _engine: BoundEngine

    def __init__(self, engine: BoundEngine) -> None:
        self._engine = engine

    def get_func(self, name: str) -> Callable:
        handler = getattr(func, name, None)
        if handler is None:
            raise KeyError(f"Unknown sql function {name}")
        return handler
        
    def _handle_path(self, path: str) -> tuple[str, list[str]]:
        field, *inner = path.split(".")
        if len(inner) == 0:
            raise ValueError("Path must be composed like: <field>.<layer1>.<layer2>... ")
        return (field, inner)

    def _get_current_value(self, node: dict[str, Any], field: str, inner_path: list[str]) -> Any:
        obj = node.get(field, None)
        if obj is None:
            raise ValueError("Json Object is None.")
        for path in inner_path:
            obj = obj[path]
        return obj

    def _serde(self, value: Any) -> list[Any]:
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except Exception:
                raise TypeError("Should be json serializable list")

        if isinstance(value, list) is False:
            raise TypeError(f"Only list can be appended. field {value} is of  type {type(value)}")
        return value


class JsonObjectUpdateKey(JsonObjectHandler):
    _engine: BoundEngine

    def __init__(self, engine: BoundEngine) -> None:
        super().__init__(engine)

    def apply(self, node: str, path: str, value: Any) -> None:
        self._engine._node_or_raise(node)
        handler = self.func()
        fname, inner = self._handle_path(path)
        field = self._engine._field_or_raise(self._engine._metadata, fname)
        inner_path = ".".join(inner)
        
        nids = select(self._engine.tree.c.id).where(self._engine.tree.c.name == node).scalar_subquery()
        stmt = (
            update(self._engine._metadata)
            .values({field: handler(self._engine._metadata.c.data, f"$.{inner_path}", json.dumps(value))})
            .where(self._engine._metadata.c.id == nids)
        )
        self._engine.execute(stmt, commit=True)

    def func(self) -> Callable:
        match self._engine.uri.dialect:
            case "sqlite":
                handler = self.get_func("json_set")
            case "psql":
                handler = self.get_func("json_set")
            case _:
                raise ValueError("Unknown sql dialect")
        return handler

class JsonObjectRemoveKey(JsonObjectHandler):
    _engine: BoundEngine

    def __init__(self, engine: BoundEngine) -> None:
        super().__init__(engine)

    def apply(self, node: str, path: str) -> None:
        self._engine._node_or_raise(node)
        handler = self.func()
        fname, inner = self._handle_path(path)
        field = self._engine._field_or_raise(self._engine._metadata, fname)
        inner_path = ".".join(inner)

        nids = select(self._engine.tree.c.id).where(self._engine.tree.c.name == node).scalar_subquery()
        stmt = (
            update(self._engine._metadata)
            .values({field: handler(self._engine._metadata.c.data, f"$.{inner_path}")})
            .where(self._engine._metadata.c.id == nids)
        )
        self._engine.execute(stmt, commit=True)

    def func(self) -> Callable:
        match self._engine.uri.dialect:
            case "sqlite":
                handler = self.get_func("json_remove")
            case "psql":
                handler = self.get_func("json_remove")
            case _:
                raise ValueError("Unknown sql dialect")
        return handler



 
class JsonObjectAppendList(JsonObjectHandler):
    _engine: BoundEngine

    def __init__(self, engine: BoundEngine) -> None:
        super().__init__(engine)

    def apply(self, node: str, path: str, value: Any) -> None:
        n = self._engine._node_or_raise(node)
        handler = self.func()
        fname, inner = self._handle_path(path)
        field = self._engine._field_or_raise(self._engine._metadata, fname)
        inner_path = ".".join(inner)
        current: list = self._serde(self._get_current_value(n, fname, inner))
        current.append(value)
        nids = select(self._engine.tree.c.id).where(self._engine.tree.c.name == node).scalar_subquery()
        stmt = (
            update(self._engine._metadata)
            .values({field: handler(self._engine._metadata.c.data, f"$.{inner_path}", json.dumps(current))})
            .where(self._engine._metadata.c.id == nids)
        )
        self._engine.execute(stmt, commit=True)

    def func(self) -> Callable:
        match self._engine.uri.dialect:
            case "sqlite":
                handler = self.get_func("json_set")
            case "psql":
                handler = self.get_func("json_set")
            case _:
                raise ValueError("Unknown sql dialect")
        return handler

class JsonObjectExtendList(JsonObjectHandler):
    _engine: BoundEngine

    def __init__(self, engine: BoundEngine) -> None:
        super().__init__(engine)

    def apply(self, node: str, path: str, value: list[Any]) -> None:
        n = self._engine._node_or_raise(node)
        handler = self.func()
        fname, inner = self._handle_path(path)
        field = self._engine._field_or_raise(self._engine._metadata, fname)
        inner_path = ".".join(inner)
        current: list = self._serde(self._get_current_value(n, fname, inner))
        current.extend(value)
        nids = select(self._engine.tree.c.id).where(self._engine.tree.c.name == node).scalar_subquery()
        stmt = (
            update(self._engine._metadata)
            .values({field: handler(self._engine._metadata.c.data, f"$.{inner_path}", json.dumps(current))})
            .where(self._engine._metadata.c.id == nids)
        )
        self._engine.execute(stmt, commit=True)

    def func(self) -> Callable:
        match self._engine.uri.dialect:
            case "sqlite":
                handler = self.get_func("json_set")
            case "psql":
                handler = self.get_func("json_set")
            case _:
                raise ValueError("Unknown sql dialect")
        return handler

class JsonObjectPopList(JsonObjectHandler):
    _engine: BoundEngine

    def __init__(self, engine: BoundEngine) -> None:
        super().__init__(engine)

    def apply(self, node: str, path: str, index: int) -> None:
        n = self._engine._node_or_raise(node)
        handler = self.func()
        fname, inner = self._handle_path(path)
        field = self._engine._field_or_raise(self._engine._metadata, fname)
        inner_path = ".".join(inner)
        current: list = self._serde(self._get_current_value(n, fname, inner))
        current.pop(index)
        nids = select(self._engine.tree.c.id).where(self._engine.tree.c.name == node).scalar_subquery()
        stmt = (
            update(self._engine._metadata)
            .values({field: handler(self._engine._metadata.c.data, f"$.{inner_path}", json.dumps(current))})
            .where(self._engine._metadata.c.id == nids)
        )
        self._engine.execute(stmt, commit=True)

    def func(self) -> Callable:
        match self._engine.uri.dialect:
            case "sqlite":
                handler = self.get_func("json_set")
            case "psql":
                handler = self.get_func("json_set")
            case _:
                raise ValueError("Unknown sql dialect")
        return handler
