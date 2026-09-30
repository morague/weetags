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
        self.tree = tree

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

    def select_from(self, *table: Table) -> QueryBuilder:
        for t in table:
            if t not in self._from:
                self._from.append(t)
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

    # def where_from_str(self, *conditions: ConditionExpr) -> QueryBuilder:
    #     anons, exprs = sqlalch_conditions(self.tree, conditions)
    #     for table in anons:
    #         if table not in self._from:
    #             self._from.append(table) 

    #     if exprs is not None:
    #         return self.where(*exprs)
    #     return self

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
