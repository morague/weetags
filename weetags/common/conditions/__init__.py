from __future__ import annotations

from sqlalchemy import Table, ColumnElement

from typing import Sequence, Any

from weetags.common.conditions.tokenizer import Tokenizer
from weetags.common.conditions.expr import ConditionExpr, ConditionBlock

"""
conditions can be a 
    list of json array      | > same handler
    list of Sequence        |
    list of strings expressions
"""



def handle_conditions(conditions: list[Any]) -> list[ConditionExpr] | list[ConditionBlock]:
    assert isinstance(conditions, Sequence)
    print(conditions)
    if all([isinstance(c, str) for c in conditions]):
        parsed = parse_condition_exprs(conditions)
    elif all([isinstance(c, Sequence) for c in conditions]):
        parsed = parse_condition_block(conditions)
    else:
        raise ValueError("???")
    return parsed



def parse_condition_block(block: list[Sequence[Any]]) -> list[ConditionBlock]:
    return [ConditionBlock(b) for b in block]

def parse_condition_exprs(block: list[str]) -> list[ConditionExpr]:
    tokenizer = Tokenizer()
    conditions = []
    for expr in block:
        tokens = tokenizer.tokenize(expr)
        conditions.append(ConditionExpr(expr, tokens))
    return conditions

def sqlalch_conditions(
    tree: Table, 
    conditions: Sequence[ConditionExpr] | Sequence[ConditionBlock]
) -> tuple[list[Table], list[ColumnElement]]:
    t, c = [], []
    for condition in conditions:
        anon, ce = condition.build(tree)
        c.append(ce)
        if anon is not None:
            t.append(anon)
    return (t, c)
