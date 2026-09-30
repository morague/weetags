from __future__ import annotations

import json
from functools import partial
from abc import ABC, abstractmethod
from attrs import define, field, Attribute, validators, asdict
from enum import Enum, auto
from typing import Sequence, Any, Type, get_args



from weetags.common.utils import OP
from weetags.common.types import BaseRelations
from weetags.common.conditions.tokenizer import Tokenizer, Token, ValuedToken, ArgsSequence, SqlFunction, parse

ACCEPTABLE_LEFT = [
    [Token.CALLABLE, Token.COLON, ArgsSequence(min_args=1, max_args=2)],
    [Token.ARG]
]

ACCEPTABLE_RIGHT = [
    [Token.OPERATOR, Token.COLON, ArgsSequence(min_args=1, max_args=100)]
]

ACCEPTABLE_REL = [
    [Token.ARG, Token.COLON, Token.ARG]
]

def convert_conditions(value: Sequence[Any] | None, expr_handler: Type[ConditionExpr | RelationExpr]) -> list[Condition] | None:
    if value is None:
        return

    values = []
    tokenizer = Tokenizer()
    for condition in value:
        if isinstance(condition, str):
            tokens = tokenizer.tokenize(condition)
            expr = expr_handler(condition, tokens)
            c = Condition.from_expr(expr)
            values.append(c)
        elif isinstance(condition, dict):
            c = Condition.from_block(condition)
            values.append(c)
    return values

def test_condition_side(sequence: Sequence[ValuedToken], acceptables: list[list[Token]]) -> bool:
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

def validate_relation(instance: Type, attribute: Attribute, value: Any) -> None:
    relations = get_args(BaseRelations.__value__)
    if value not in relations:
        raise ValueError(f"Unknown relation: {value}")

def convert_operator_symbol(value: str) -> str:
    if value in OP.values():
        return value
    op = OP.get(value, None)
    if op is None:
        raise KeyError(f"Unknown operator name: {value}")
    return op

def convert_func_args(value: Sequence[Any]) -> Sequence[Any]:
    return [parse(v) for v in value]

class ConditionType(Enum):
    RELATION = auto()
    OPERATION = auto()
    FSQL_OPERATION = auto()


class CPayload(ABC):
    ...

@define
class SqlFPayload(CPayload):
    func: SqlFunction = field(converter=SqlFunction.from_value_or_raise)
    func_args: Sequence[Any] = field(converter=convert_func_args)
    operator: str = field(converter=convert_operator_symbol)
    value: Any = field(converter=parse)

@define
class CMPPayload(CPayload):
    fname: str = field(validator=[validators.instance_of(str)])
    operator: str = field(converter=convert_operator_symbol)
    value: Any = field(converter=parse)

@define
class RelPayload(CPayload):
    relation: str = field(validator=[validate_relation])
    value: Any = field(converter=parse)

@define(repr=False)
class Condition:
    payload: CPayload = field()
    type: ConditionType = field()
    expr: Expr | None = field(default=None)

    def __repr__(self) -> str:
        return json.dumps(self.as_dict())
    
    @classmethod
    def from_expr(cls, expr: Expr) -> Condition:
        return cls(expr.as_payload(), expr.type, expr)

    @classmethod
    def from_block(cls, payload: dict[str, Any]) -> Condition:
        ctype = cls._define_type(payload)
        match ctype:
            case ConditionType.FSQL_OPERATION:
                p = SqlFPayload(**payload)
            case ConditionType.OPERATION:
                p = CMPPayload(**payload)
            case ConditionType.RELATION:
                p = RelPayload(**payload)
        return cls(p, ctype)

    @staticmethod
    def _define_type(payload: dict[str, Any]) -> ConditionType:
        if payload.get("relation") is not None:
            return ConditionType.RELATION
        elif payload.get("func") is not None:
            return ConditionType.FSQL_OPERATION
        elif payload.get("fname") is not None:
            return ConditionType.OPERATION
        else:
            raise ValueError("???")

    def as_dict(self) -> dict[str, Any]:
        return asdict(self.payload)


class Expr(ABC):
    raw: str
    tokens: Sequence[ValuedToken]
    type: ConditionType

    @property
    def _t(self) -> Sequence[Token]:
        return [t.token for t in self.tokens]

    @abstractmethod
    def as_payload(self) -> CPayload:
        raise NotImplementedError()

@define
class ConditionExpr(Expr):
    raw: str = field()
    tokens: Sequence[ValuedToken] = field()
    type: ConditionType = field(init=False)

    def __attrs_post_init__(self) -> None:
        l = test_condition_side(self.left, ACCEPTABLE_LEFT)
        r = test_condition_side(self.right, ACCEPTABLE_RIGHT)
        if not all([l, r]):
            raise ValueError(
                "Non valid condition format.",
                "accepted formats: `field_name`->`operator`:`value` or `function_name`:`argument`,...->`operator`:`value`"
            )
                
        match len(self.left):
            case 1:
                self.type = ConditionType.OPERATION
            case _:
                self.type = ConditionType.FSQL_OPERATION

    @property
    def left(self) -> Sequence[ValuedToken]:
        index = self._t.index(Token.ARROW)
        return self.tokens[:index]

    @property
    def right(self) -> Sequence[ValuedToken]:
        index = self._t.index(Token.ARROW) + 1
        return self.tokens[index:]

    @property
    def func(self) -> SqlFunction:
        if self.type != ConditionType.FSQL_OPERATION:
            raise TypeError("FSQL")
        return self.left[0].value

    @property
    def func_args(self) -> Sequence[Any]:
        if self.type != ConditionType.FSQL_OPERATION:
            raise TypeError("FSQL")
        return [t.value for t in self.left[1:] if t.token == Token.ARG]

    @property
    def cmp_field(self) -> str:
        match self.type:
            case ConditionType.OPERATION:
                fname = self.left[0].value
            case ConditionType.FSQL_OPERATION:
                fname = self.left[2].value
            case _:
                raise ValueError("???")
        assert isinstance(fname, str)
        return fname

    @property
    def operator_symbol(self) -> str:
        return self.right[0].value

    @property
    def cmp_value(self) -> Any:
        values = [t.value for t in self.right[1:] if t.token == Token.ARG]
        if len(values) == 1:
            values = values[0]
        return values

    def as_payload(self) -> CPayload:
        match self.type:
            case ConditionType.OPERATION:
                payload = CMPPayload(self.cmp_field, self.operator_symbol, self.cmp_value)
            case ConditionType.FSQL_OPERATION:
                payload = SqlFPayload(self.func, self.func_args, self.operator_symbol, self.cmp_value)
            case _:
                raise ValueError()            
        return payload

@define
class RelationExpr(Expr):
    raw: str = field()
    tokens: Sequence[ValuedToken] = field()
    type: ConditionType = ConditionType.RELATION

    def __attrs_post_init__(self) -> None:
        if self.relation not in get_args(BaseRelations.__value__):
            raise ValueError(f"Unknown relation: {self.relation}")
        valid = test_condition_side(self.tokens, ACCEPTABLE_REL)
        if valid is False:
            raise ValueError("invalid token sequence. accepable token sequence: `relation`:`node_name` ")
        
    @property
    def relation(self) -> str:
        return self.tokens[0].value

    @property
    def cmp_value(self) -> str:
        return self.tokens[-1].value        

    def as_payload(self) -> RelPayload:
        return RelPayload(self.relation, self.cmp_value)

convert_cmp_conditions = partial(convert_conditions, expr_handler=ConditionExpr)
convert_rel_conditions = partial(convert_conditions, expr_handler=RelationExpr)