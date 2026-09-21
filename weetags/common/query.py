from __future__ import annotations

from sqlalchemy import Executable
from sqlalchemy.sql.elements import UnaryExpression, ColumnElement
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    update,
    delete,
    select,
    func
)

from typing import Any, Callable, Literal

from weetags.common.utils import OP

class SQLConditionParser:
    table: Table

    def __init__(self, table: Table) -> None:
        self.table = table
        self._anon_table = []

    def parse(self, conditions: list[tuple[str, str | tuple, str] | str]) -> tuple[list[Table], ColumnElement | None]:
        condition, operator = None, "__and__"
        for block in conditions:
            if isinstance(block, tuple) or isinstance(block, list):
                cond = self._parse_condition_block(block)
                if condition is None:
                    condition = cond
                else:
                    handler = getattr(condition, operator)
                    condition = handler(cond)
                operator = "__and__"
            elif self._is_operator(block) and condition is None:
                raise ValueError(f"operator {block} cannot be first in a conditions")
            elif self._is_operator(block) and self._is_and(block):
                operator = "__and__"
            elif self._is_operator(block) and self._is_or(block):
                operator = "__or__"
        return (self._anon_table, condition)

    def _is_operator(self, block: tuple[str, str | list, str] | str) -> bool:
        return isinstance(block, str) and block in ["and", "or", "AND", "OR", "&", "|"]

    def _is_and(self, block: str) -> bool:
        return block in ["and", "AND", "&"]

    def _is_or(self, block: str) -> bool:
        return block in ["or", "OR", "|"]

    def _parse_condition_block(self, block: tuple[str, str | tuple[str, tuple[Any]], Any]) -> ColumnElement:
        key, op_block, value = block
        column = self._get_column(key)
        if isinstance(op_block, str):
            op = self._get_op(op_block)
            callback = getattr(column, op, None)
            if callback is None:
                raise ValueError(f"Unknown operator: {op}") 
            cond = callback(value)
        else:
            cond = self._apply_special_op(column, op_block, value)
        return cond

    def _get_column(self, field: str) -> Column:
        column = self.table.c.get(field)
        if column is None:
            raise ValueError(f"Unknown column: {field}")
        return column

    def _get_op(self, op: str) -> str:
        callable_name = OP.get(op, None)
        if callable_name is None:
            raise KeyError(f"Unknown operator: {op}")
        return callable_name

    def _apply_special_op(self, column: ColumnElement, op_block: tuple[str, tuple[Any]], value: Any) -> ColumnElement:
        callback_name, args = op_block

        f = getattr(func, callback_name, None)
        if f is None:
            raise ValueError(f"Unknown function: {callback_name}")

        match callback_name:
            case "json_each":
                op = self._apply_json_each(f, column, args, value)  
            case _:
                raise ValueError(f"Non handled function: {callback_name}")
        return op
    
    def _apply_json_each(self, f: Callable, column: ColumnElement, args: tuple[Any], value: Any) -> ColumnElement:
        if len(args) <= 1:
            t = f(column, *args).table_valued('value', joins_implicitly=True)
        else:
            raise ValueError(f"too many arguments: {args}")
        op = t.c.value == value
        self._anon_table.append(t)
        return op



class QueryBuilder:
    metadata: MetaData

    operation: Literal["select", "update", "delete"]

    _limit: int | None = None
    _offset: int | None = None
    _from: list[Table]
    _fields: list[Column]
    _where: list[ColumnElement]
    _order_by: list[ColumnElement | UnaryExpression]
    _values: dict[str, Any]


    def __init__(self, tree: Table, metadata: MetaData) -> None:
        self.metadata = metadata

        self._limit = None
        self._offset = None
        self._from = [tree]
        self._fields = []
        self._values = {}
        self._where = []
        self._order_by = []

    def __call__(self) -> Executable:
        match self.operation:
            case "select":
                exec = self._select()
            case "delete":
                exec = self._delete()
            case "update":
                exec = self._update()
            case _:
                raise ValueError("Unknown Sql operation.")
        return exec

    def _select(self) -> Executable:
        fields = self._fields
        if len(fields) == 0:
            fields = [self._from[0]]

        exec = (
            select(*fields)
            .select_from(*self._from)
            .where(*self._where)
            .order_by(*self._order_by)
            .limit(self._limit)
            .offset(self._offset)
        )
        return exec
    
    def _update(self) -> Executable:
        exec = update(self._from[0]).values(self._values).where(*self._where)
        return exec

    def _delete(self) -> Executable:
        exec = delete(self._from[0]).where(*self._where)
        return exec
    
    def select(self) -> QueryBuilder:
        self.operation = "select"
        return self

    def update(self) -> QueryBuilder:
        self.operation = "update"
        return self

    def delete(self) -> QueryBuilder:
        self.operation = "delete"
        return self


    def values(self, values: dict[str, Any]) -> QueryBuilder:
        self._values.update(values) 
        return self

    def limit(self, limit: int) -> QueryBuilder:
        self._limit = limit
        return self
    
    def offset(self, offset: int) -> QueryBuilder:
        self._offset = offset
        return self


    def where(self, *conditions: ColumnElement) -> QueryBuilder:
        self._where.extend(list(conditions))
        return self

    def fields(self, *field: Column) -> QueryBuilder:
        self._fields.extend(field)
        return self

    def order_by(self, *fields: ColumnElement | UnaryExpression) -> QueryBuilder:
        self._order_by.extend(fields)
        return self

    def order_by_fro_str(self, *fields: str) -> QueryBuilder:
        orders = [self._get_unary_expr(field, self._from[0]) for field in fields]
        return self.order_by(*orders)

    def fields_from_str(self, *fields: str) -> QueryBuilder:
        columns = self._str_to_column(*fields)
        return self.fields(*columns)

    def where_from_str(self, conditions: list[tuple | str]) -> QueryBuilder:
        table = self._get_table()
        tables, where = SQLConditionParser(table).parse(conditions)
        self._from.extend(tables)

        if where is not None:
            return self.where(where)
        return self

    def _get_column(self, field: str, table: Table) -> Column:
        column = table.c.get(field, None)
        if column is None:
            raise KeyError(f"Unknonw Column: {field}")
        return column

    def _get_table(self) -> Table:
        return self._from[0]

    def _get_unary_expr(self, field: str, table: Table) -> Column | UnaryExpression:
        expr = field.split(".")
        match len(expr):
            case 1:
                order = self._get_column(expr[0], table)
            case 2:
                column = self._get_column(expr[0], table)
                order = self._get_order(column, expr[1])
            case _:
                raise ValueError("Unknown order by expression.")
        return order

    def _get_order(self, column: Column, unary: Literal["asc", "desc"] | str) -> UnaryExpression:
        match unary:
            case "asc":
                order = column.asc()
            case "desc":
                order = column.desc()
            case _:
                raise ValueError("Unknown order by expression.")
        return order

    def _str_to_order(self, *fields: str) -> list[UnaryExpression | Column]:
        table = self._get_table()
        order = [self._get_unary_expr(field, table) for field in fields]
        return order

    def _str_to_column(self, *fields) -> list[Column]:
        table = self._get_table()
        columns = [self._get_column(f, table) for f in fields]
        return columns
