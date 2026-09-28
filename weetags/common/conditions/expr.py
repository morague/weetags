from __future__ import annotations

from attrs import define, field
from sqlalchemy import column, func, Table, Column, ColumnElement

from typing import Any, Sequence

from weetags.common.utils import OP
from weetags.common.conditions.tokenizer import CToken, Token, SqlFunction




class ArgsSequence:
    sequence: list[Token] = [Token.ARG, Token.COMMA]
    min_args: int = 0
    max_args: int = 10

    def __init__(self, min_args: int = 1, max_args: int = 10) -> None:
        self.min_args = min_args
        self.max_args = max_args

    def is_valid(self, sequence: list[CToken]) -> bool:
        valid = False

        args = [t.token for t in sequence if t.token == Token.ARG]
        if len(args) < self.min_args or len(args) > self.max_args:
            return valid

        for i in range(len(sequence)):
            token = sequence[i]
            if i % 2 == 0 and token.token != Token.ARG:
                return False
            if i % 2 != 0 and token.token != Token.COMMA:
                return False
        return True


ACCEPTABLE_LEFT = [
    [Token.CALLABLE, Token.COLON, ArgsSequence(min_args=1, max_args=2)],
    [Token.ARG]
]

ACCEPTABLE_RIGHT = [
    [Token.OPERATOR, Token.COLON, Token.ARG]
]


def test_condition_side(sequence: list[CToken], acceptables: list[list[Token]]) -> bool:
    """could be improved for ArgsSequence handling, but sufficient as long as the argsequence is at the end of a side."""
    accept = []
    for acceptable in acceptables:
        valid = []
        for i in range(len(acceptable)):
            accept_token = acceptable[i]
            if len(sequence) - 1 < i:
                valid.append(False)
                break
            
            if isinstance(accept_token, ArgsSequence):
                valid.append(accept_token.is_valid(sequence[i:]))
            else:
                sequence_token = sequence[i]
                valid.append(sequence_token.token == accept_token)
        accept.append(all(valid))
    return any(accept)

def get_column(table: Table, field: str) -> Column:
    column = table.c.get(field)
    if column is None:
        raise ValueError(f"Unknown column: {field}")
    return column


@define
class ConditionExpr:
    raw: str = field()
    tokens: list[CToken] = field()

    def __repr__(self) -> str:
        return self.raw

    def __attrs_post_init__(self) -> None:
        """control expr"""
        l = test_condition_side(self.left, ACCEPTABLE_LEFT)
        r = test_condition_side(self.right, ACCEPTABLE_RIGHT)
        if not all([l, r]):
            raise ValueError(
                "Non valid condition format.",
                "accepted formats: `field_name`->`operator`:`value` or `function_name`:`argument`,...->`operator`:`value`"
            )

    @property
    def left_type(self) -> str:
        size = len(self.left)
        if size == 1:
            return "field_name"
        else:
            return "anon_table"

    @property
    def _t(self) -> list[Token]:
        return [t.token for t in self.tokens]

    @property
    def left(self) -> list[CToken]:
        index = self._t.index(Token.ARROW)
        return self.tokens[:index]

    @property
    def right(self) -> list[CToken]:
        index = self._t.index(Token.ARROW) + 1
        return self.tokens[index:]

    def build(self, table: Table) -> tuple[Table | None, ColumnElement]:
        match self.left_type:
            case "field_name":
                t,e = self._column_operation(table)
            case "anon_table":
                t,e = self._f_column_operation(table)
            case _:
                raise ValueError("???")
        return (t, e)
    
    def _column_operation(self, table: Table) -> tuple[None, ColumnElement]:
        fname, *_ = [t for t in self.left if t.token == Token.ARG]
        column = get_column(table, fname.value)
        e = self._apply_operator(column)
        return (None, e)

    def _f_column_operation(self, table: Table) -> tuple[Table, ColumnElement]:   
        anon_table = self._set_anon_table(table)     
        e = self._apply_operator(anon_table.c.value)
        return (anon_table, e)


    def _apply_operator(self, left: Column) -> ColumnElement:
        operator_name = self.right[0]
        value = self.right[-1]

        operator = getattr(left, operator_name.value)
        return operator(value.value)

    def _set_anon_table(self, table: Table) -> Table:
        func_name = self.left[0] 
        fname, *args = [t for t in self.left if t.token == Token.ARG]

        column = get_column(table, fname.value)
        valued_args = [a.value for a in args]
        f = func_name.value.to_callable()
        return f(column, *valued_args).table_valued('value', joins_implicitly=True)


@define
class ConditionBlock:
    block: Sequence[Any] = field()

    @property
    def left(self) -> tuple[Any, ...]:
        return tuple(self.block[:-2])

    @property
    def right(self) -> tuple[Any, ...]:
        return tuple(self.block[-2:])

    @property
    def size(self) -> int:
        return len(self.block)

    @property 
    def left_type(self) -> str:
        if self.size == 3:
            return "field_name"
        if self.size >= 4:
            return "anon_table"
        else:
            raise ValueError("???")

    def build(self, table: Table) -> tuple[Table | None, ColumnElement]:
        match self.left_type:
            case "field_name":
                t,e = self._column_operation(table)
            case "anon_table":
                t,e = self._f_column_operation(table)
            case _:
                raise ValueError("???")
        return (t, e)

    def _column_operation(self, table: Table) -> tuple[None, ColumnElement]:
        fname = self.left[0]
        column = get_column(table, fname)
        e = self._apply_operator(column)
        return (None, e)

    def _f_column_operation(self, table: Table) -> tuple[Table, ColumnElement]:   
        func = SqlFunction.from_value(self.left[0])
        if func is None:
            raise KeyError(f"Unknown sql function: {self.left[0]}")

        fname, *args = self.left[1:]
        args = [arg for arg in args if arg != None]
        column = get_column(table, fname)
        f = func.to_callable()

        anon_table = f(column, *args).table_valued('value', joins_implicitly=True)
        e = self._apply_operator(anon_table.c.value)
        return (anon_table, e)


    def _apply_operator(self, left: Column) -> ColumnElement:
        operator_name = self.right[0]
        value = self.right[-1]

        callable_name = OP.get(operator_name, None)
        if callable_name is None:
            raise KeyError(f"Unknown operator: {operator_name}") 
        operator = getattr(left, callable_name)
        return operator(value)