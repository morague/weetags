from __future__ import annotations
from ast import Raise, Tuple

import sqlalchemy
from sqlalchemy.sql.elements import UnaryExpression, ColumnElement
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Select,
    Delete,
    Update,
    update,
    delete,
    select,
    func
)

from typing import Any, Callable

from weetags.common.utils import OP

"""

CONDITION | AND | OR

condition:
    tuple: key, op, value
    where
        op is either a string or a tuple: func, *args

return anon table, sqlach conditions


json_each(c, path) 

"""


class SQLConditionParser:
    table: Table

    def __init__(self, table: Table) -> None:
        self.table = table
        self._anon_table = []

    def parse(self, conditions: list[tuple[str, str | tuple, str] | str]) -> tuple[list[Table], ColumnElement | None]:
        condition, operator = None, "__and__"
        for block in conditions:
            if isinstance(block, tuple): # or isinstance(block, list)
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







# class SQLConditionParser:
#     def __init__(self, table: Table) -> None:
#         self.table = table

#         self.anon_table = []
#         self.condition = None
#         self.next_op = None
    
#     def parse(self, conditions: list) -> tuple[list[Table], list[ColumnElement]]:
#         self._parse_block(conditions)

#         if self.condition is None:
#             self.condition = []
#         return (self.anon_table, self.condition)
    
#     def _parse_block(self, block: list | tuple | str) -> None:
#         for sub in block:
#             if self._is_expr(sub):
#                 table, expr = self._parse_expr(*sub)
#                 if table is not None:
#                     self.anon_table.append(table)
#                 self._handle_expr(expr)
#             elif self._is_op(sub):
#                 self.next_op = sub
#             else:
#                 expr = self._parse_block(sub)
#                 self._handle_expr(expr)

#     def _handle_expr(self, expr: ColumnElement) -> None:
#         if self.condition is None:
#             self.condition = expr
#         elif self.next_op in ["or", "OR", "|"]:
#             self.condition = self.condition.__or__(expr)
#             self.next_op = None
#         elif self.next_op in ["and", "AND", "&"] or self.next_op is None:
#             self.condition = self.condition.__and__(expr)
#             next_op = None
#         else:
#             raise ValueError(f"Unknown operator: {next_op}")

#     def _parse_expr(self, key: str, op: str, value: Any) -> tuple[Table | None, ColumnElement]:
#         column = self._get_column(key)
#         callable_name = self._get_op(op)
#         return self._apply_op(callable_name, column, value)

#     def _is_expr(self, block: list | tuple | str) -> bool:
#         return (
#             (
#                 type(block) is list
#                 or type(block) is tuple
#             )
#             and len(block) == 3
#             and isinstance(block[0], str)
#             and  isinstance(block[1], str)
#         )

#     def _is_op(self, block: list | tuple | str) -> bool:
#         return isinstance(block, str) and block in ["and", "or", "AND", "OR", "&", "|"]

#     def _json_each(self, column: Column, value: Any) -> tuple[Table, ColumnElement]:
#         t = func.json_each(column).table_valued('value', joins_implicitly=True)
#         c = t.c.value == value
#         return (t, c)
    
#     def _get_column(self, field: str) -> Column:
#         column = self.table.c.get(field)
#         if column is None:
#             raise ValueError(f"Unknown column: {field}")
#         return column
    
#     def _get_op(self, op: str) -> str:
#         callable_name = OP.get(op, None)
#         if callable_name is None:
#             raise KeyError(f"Unknown operator: {op}")
#         return callable_name
        
#     def _apply_op(self, name: str, column: Column, value: Any) -> tuple[Table | None, ColumnElement]:
#         table, operator = None, None
#         match name:
#             case "json_each":
#                 table, operator = self._json_each(column, value)
#             case _:
#                 o = getattr(column, name, None)
#                 operator = o(value)
#         return (table, operator)
                


class QueryBuilder:
    metadata: MetaData

    _from: list[Table]
    _values: dict[str, Any] | None = None
    _offset: int | None = None
    _limit: int | None = None
    _fields: list[Column]
    _where: list[ColumnElement]
    _order: list[UnaryExpression]

    def __init__(self, metadata: MetaData) -> None:
        self.metadata = metadata



        self._fields = []
        self._from = []
        self._values = None
        self._where = []
        self._order_by = []
        self._offset = None
        self._limit = None

    @property
    def read(self) -> Select:
        if len(self._from) == 0:
            raise ValueError("Query missing _from argument.")
        
        stmt = select(*self._fields)
        for table in self._from:
            stmt = stmt.select_from(table)
        stmt = stmt.where(*self._where).order_by(*self._order_by)
        if self._offset is not None:
            stmt = stmt.offset(self._offset)
        if self._offset is not None:
            stmt = stmt.limit(self._limit) 
        return stmt

    @property
    def delete(self) -> Delete:
        if len(self._from) == 0:
            raise ValueError("Query missing _from argument.")
        return (
            delete(self._from)
            .where(*self._where)
        )

    @property
    def update(self) -> Update:
        if self._from is None:
            raise ValueError("Query missing _from argument.")
        if len(self._from) > 1:
            raise ValueError("Update stmt only imply on table.")
        return (
            update(*self._from)
            .where(*self._where)
            .values(self._values)
        )

    @property
    def create(self) -> ...:
        ...

    def limit(self, limit: int) -> QueryBuilder:
        self._limit = limit
        return self
    
    def offset(self, offset: int) -> QueryBuilder:
        self._offset = offset
        return self

    def values(self, values: dict[str, Any]) -> QueryBuilder:
        self._values = values
        return self

    def fields(self, *field: Column) -> QueryBuilder:
        if (len(self._fields) == 1 
            and len(self._from) > 0 and 
            self._fields[0] in self._from
        ):
            self._fields = []
        self._fields.extend(field)
        return self

    def fields_from_str(self, *fields: str) -> QueryBuilder:
        columns = self._str_to_column(*fields)
        return self.fields(*columns)

    def tree(self, tree: Table) -> QueryBuilder:
        self._from.append(tree)
        if len(self._fields) == 0:
            self._fields.append(tree)
        return self

    def tree_from_str(self, tree: str) -> QueryBuilder:
        table = self.metadata.tables.get(tree, None)
        if table is None:
            raise KeyError("Unknown tree.")
        return self.tree(table)

    def order_by(self, *fields: UnaryExpression) -> QueryBuilder:
        self._order_by.extend(fields)
        return self

    def order_by_from_str(self, *fields: str) -> QueryBuilder:
        exprs = self._str_to_order(*fields)
        return self.order_by(*exprs)

    def where(self, conditions: list[ColumnElement]) -> QueryBuilder:
        """
        func.json_contains(f, k, "$")
        """
        self._where.extend(conditions)
        return self
        
    def where_from_str(self, conditions: list) -> QueryBuilder:
        table = self._get_table()
        tables, where = SQLConditionParser(table).parse(conditions)
        if tables is not None:
            self._from.extend(tables)
        return self.where([where])

    
    def _get_table(self) -> Table:
        if len(self._from) == 0:
            raise ValueError("A tree must be selected first before infering str to Column.")
        return self._from[0]

    def _get_column(self, field: str, table: Table) -> Column:
        column = table.c.get(field, None)
        if column is None:
            raise KeyError(f"Unknonw Column: {field}")
        return column

    def _get_unary_expr(self, field: str, table: Table) -> UnaryExpression:
        expr = field.split(".")
        if len(expr) == 1:
            o = self._get_column(expr[0], table)
        elif len(expr) == 2:
            f,u = expr
            column = self._get_column(f, table)
            if u == "asc":
                o = column.asc()
            elif u == "desc":
                o = column.desc()
            else:
                raise ValueError("Unknown order by expression.")
        else:
            raise ValueError("Unknown order by expression.")
        return o
    
    def _str_to_order(self, *fields: str) -> list[UnaryExpression]:
        table = self._get_table()
        order = [self._get_unary_expr(field, table) for field in fields]
        return order

    def _str_to_column(self, *fields) -> list[Column]:
        table = self._get_table()
        columns = [self._get_column(f, table) for f in fields]
        return columns



