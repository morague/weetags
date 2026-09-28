from __future__ import annotations

import re
from abc import ABC
import itertools as it
from enum import Enum
from collections import deque
from sqlalchemy import func, Table, Column, ColumnElement, TableValuedAlias
from attrs import define, field
from typing import Any, Callable, Iterable, Generator

from weetags.common.utils import OP

BOOL1_PATTERN = re.compile("[Tt](rue|RUE)")
BOOL0_PATTERN = re.compile("[Ff](alse|ALSE)")
NONE_PATTERN = re.compile("[Nn](one|ONE|ull|ULL)")
DATETIME_PATTERN = re.compile("")




def parse(value: Any) -> Any:
    if value.isnumeric():
        return int(value)
    elif bool(re.search(BOOL0_PATTERN, value)):
        return False
    elif bool(re.search(BOOL1_PATTERN, value)):
        return True
    elif bool(re.search(NONE_PATTERN, value)):
        return None
    else:
        return value






class SqlFunction(str, Enum):
    JSON_EACH = "json_each"

    @classmethod
    def from_value(cls, value: str) -> SqlFunction | None:
        for e in cls:
            if e.value == value:
                return e
        return None

    def to_callable(self) -> Callable:
        f = getattr(func, self.value, None)
        if f is None:
            raise KeyError(f"Unknown sql function: {self.value}")
        return f

class Token(str, Enum):
    OPERATOR = "operator"
    CALLABLE = "callable"
    ARG = "arg"
    ARROW = "->"
    COLON = ":"
    COMMA = ","

    @classmethod
    def longest_expr(cls) -> int:
        special = [Token.OPERATOR, Token.CALLABLE, Token.ARG]
        return max([len(v.value) for v in Token.__members__.values() if v not in special])

    @classmethod
    def from_value(cls, value: str) -> Token | None:
        for e in cls:
            if e.value == value:
                return e
        return None


class CToken(ABC):
    token: Token
    value: Any

@define
class SeperatorToken(CToken):
    token: Token = field()
    value: Any = field()

@define
class ValuedToken(CToken):
    token: Token = field()
    value: Any = field()

    def into_sql_function(self) -> Callable:
        if self.token != Token.CALLABLE:
            raise ValueError("not callable")
        f: SqlFunction = getattr(self, "value")
        return f.to_callable()

    def into_operator(self, c: ColumnElement) -> Callable:
        if self != Token.OPERATOR:
            raise ValueError("not operator")
        operator: SqlFunction = getattr(self, "valued")
        return getattr(c, operator)






class Tokenizer:
    def __init__(self) -> None:
        pass

    def tokenize(self, value: str) -> list[CToken]:
        size = Token.longest_expr()
        tokens, buffer, SKIP = [], "", 0
        for w in self.sliding_window(value, size=size):
            if SKIP > 0:
                SKIP -= 1
                continue

            token = self.eval(w)
            match token:
                case Token.ARROW | Token.COLON | Token.COMMA:
                    if len(buffer) > 0:
                        tokens.append(self.valued_token(buffer))
                        buffer = ""
                    tokens.append(SeperatorToken(token, None))
                    SKIP += len(token.value) - 1
                case None:
                    buffer += w[0]
                case _:
                    raise ValueError("???")
        if len(buffer) > 0:
            v = self.valued_token(buffer)
            tokens.append(v)
        return tokens

    @staticmethod
    def valued_token(value: str) -> ValuedToken:
        operator = OP.get(value, None)
        callable = SqlFunction.from_value(value)
        if callable is not None:
            token = Token.CALLABLE
            valued = ValuedToken(token, callable)
        elif operator is not None:
            token = Token.OPERATOR
            valued = ValuedToken(token, operator)
        else:
            token = Token.ARG
            valued = ValuedToken(token, parse(value))
        return valued

    @staticmethod
    def sliding_window(value: str, size: int) -> Generator[tuple]:
        iterable = iter(list(value) + [""])

        iterator = iter(iterable)
        window = deque(it.islice(iterator, size - 1), maxlen=size)
        for x in iterator:
            window.append(x)
            yield tuple(window)

    @staticmethod
    def eval(window: tuple) -> Token | None:
        e = None
        for i in range(1, len(window) + 1):
            value = "".join(window[:i])
            if len(value) != i:
                break
            e = e or Token.from_value(value)
        return e
