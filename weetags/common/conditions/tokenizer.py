from __future__ import annotations

import re
from abc import ABC
import itertools as it
from enum import Enum
from collections import deque
from sqlalchemy import func, Table, Column, ColumnElement, TableValuedAlias
from attrs import define, field
from typing import Any, Callable, Iterable, Generator, Sequence

from weetags.common.utils import OP

BOOL1_PATTERN = re.compile("[Tt](rue|RUE)")
BOOL0_PATTERN = re.compile("[Ff](alse|ALSE)")
NONE_PATTERN = re.compile("[Nn](one|ONE|ull|ULL)")
DATETIME_PATTERN = re.compile("")




def parse(value: Any) -> Any:
    if value is None:
        return None
    elif isinstance(value, (list, tuple)):
        return [parse(v) for v in value]
    elif isinstance(value, (bool, int)):
        return value
    elif isinstance(value, str):
        if len(value.split(",")) > 1:
            return [parse(v) for v in value.split(",")]
        elif value.isnumeric():
            return int(value)
        elif bool(re.search(BOOL0_PATTERN, value)):
            return False
        elif bool(re.search(BOOL1_PATTERN, value)):
            return True
        elif bool(re.search(NONE_PATTERN, value)):
            return None
        else:
            return value
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

    @classmethod
    def from_value_or_raise(cls, value: str) -> SqlFunction:
        e = cls.from_value(value)
        if e is None:
            raise ValueError(f"Unknown sql function: {value}")
        return e

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



class ArgsSequence:
    sequence: Sequence[Token] = [Token.ARG, Token.COMMA]
    min_args: int = 0
    max_args: int = 10

    def __init__(self, min_args: int = 1, max_args: int = 10) -> None:
        self.min_args = min_args
        self.max_args = max_args

    def is_valid(self, sequence: Sequence[ValuedToken]) -> bool:
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


class Tokenizer:
    def __init__(self) -> None:
        pass

    def tokenize(self, value: str) -> list[ValuedToken]:
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
                    tokens.append(ValuedToken(token, None))
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
