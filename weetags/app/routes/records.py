from __future__ import annotations

from collections import ChainMap
from sanic import Blueprint
from sanic.request import Request
from sanic.response import json, text, empty, JSONResponse
from sanic_ext import openapi

from typing import Any, Literal, get_args

from weetags.app.routes.middlewares import extract_params
from weetags.app.authentication.authentication import protected

from weetags.trees.tree import Tree
from weetags.exceptions import TreeDoesNotExist, UnknownRelation, OutputError

Node = dict[str, Any]
Relations = Literal["parent", "children", "siblings", "ancestors", "descendants"]


records = Blueprint("records", "/records")
records.on_request(extract_params, priority=100)

@records.route("node/<tree_name:str>/<nid:str>", methods=["GET", "POST"])
@protected
async def node(request: Request, tree_name: str, nid: str) -> JSONResponse:
    tree: Tree = request.app.ctx.trees.get(tree_name, None)
    if tree is None:
        raise TreeDoesNotExist(tree_name, list(request.app.ctx.trees.keys()))
    params = request.ctx.params.get_kwargs(tree.node)
    return json({"status": "200", "reasons": "OK", "data": tree.node(**params)}, status=200)

@records.route("node/<tree_name:str>/<relation:str>/<nid:str>", methods=["GET", "POST"])
@protected
async def node_relations(request: Request, tree_name: str, relation: Relations, nid: str) -> JSONResponse:
    tree: Tree = request.app.ctx.trees.get(tree_name, None)
    if tree is None:
        raise TreeDoesNotExist(tree_name, list(request.app.ctx.trees.keys()))

    if relation not in get_args(Relations):
        raise UnknownRelation(relation, list(get_args(Relations)))

    if relation != "parent":
        raise OutputError(relation, "Node")

    callback = {
        "parent": tree.parent_node,
        "children": tree.children_nodes,
        "siblings": tree.siblings_nodes,
        "ancestors": tree.ancestors_nodes,
        "descendants": tree.descendants_nodes
    }[relation]

    params = request.ctx.params.get_kwargs(callback)
    return json({"status": "200", "reasons": "OK", "data": callback(**params)}, status=200)


@records.route("nodes/<tree_name:str>/<relation:str>/<nid:str>", methods=["GET", "POST"])
@protected
async def nodes_relations(request: Request, tree_name: str, relation: Relations, nid: str) -> JSONResponse:
    tree: Tree = request.app.ctx.trees.get(tree_name, None)
    if tree is None:
        raise TreeDoesNotExist(tree_name, list(request.app.ctx.trees.keys()))

    if relation not in get_args(Relations):
        raise UnknownRelation(relation, list(get_args(Relations)))

    if relation == "parent":
        raise OutputError(relation, "list[Node]")

    callback = {
        "parent": tree.parent_node,
        "children": tree.children_nodes,
        "siblings": tree.siblings_nodes,
        "ancestors": tree.ancestors_nodes,
        "descendants": tree.descendants_nodes
    }[relation]

    params = request.ctx.params.get_kwargs(callback)
    return json({"status": "200", "reasons": "OK", "data": callback(**params)}, status=200)

@records.route("nodes/<tree_name:str>/where", methods=["GET", "POST"])
@protected
async def nodes_where(request: Request, tree_name: str) -> JSONResponse:
    tree: Tree = request.app.ctx.trees.get(tree_name, None)
    if tree is None:
        raise TreeDoesNotExist(tree_name, list(request.app.ctx.trees.keys()))

    params = request.ctx.params.get_kwargs(tree.nodes_where)
    return json({"status": "200", "reasons": "OK", "data": tree.nodes_where(**params)}, status=200)


@records.route("nodes/<tree_name:str>/<relation:str>/where", methods=["GET", "POST"])
@protected
async def nodes_relation_where(request: Request, tree_name: str, relation: str) -> JSONResponse:
    tree: Tree = request.app.ctx.trees.get(tree_name, None)
    if tree is None:
        raise TreeDoesNotExist(tree_name, list(request.app.ctx.trees.keys()))

    if relation not in get_args(Relations):
        raise UnknownRelation(relation, list(get_args(Relations)))

    callback = {
        "parent": tree.parent_node,
        "children": tree.children_nodes,
        "siblings": tree.siblings_nodes,
        "ancestors": tree.ancestors_nodes,
        "descendants": tree.descendants_nodes
    }[relation]

    params = request.ctx.params.get_kwargs(tree.nodes_relation_where)
    return json(
        {
            "status": "200",
            "reasons": "OK",
            "data": tree.nodes_relation_where(**params, relation=callback, include_base=True)
        },
        status=200
    )
