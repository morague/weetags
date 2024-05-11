from __future__ import annotations

from collections import ChainMap
from sanic import Blueprint
from sanic.request import Request
from sanic.response import json, text, empty
from sanic_ext import openapi


from typing import Any, Literal, get_args

from weetags.tools.authentication import protected

from weetags.params_handler import ParamsHandler
from weetags.trees.permanent_tree import PermanentTree
from weetags.exceptions import TreeDoesNotExist, UnknownRelation, OutputError



Node = dict[str, Any]
Relations = Literal["parent", "children", "siblings", "ancestors", "descendants"]

reader = Blueprint("reader")

@reader.on_request(priority=100)
async def extract_params(request: Request):
    nid = {k:v for k,v in request.match_info.items() if k == "nid"} or {}
    query_args = {k:(v[0] if len(v) == 1 else v) for k,v in request.args.items()}
    payload = request.load_json() or {}
    params = dict(ChainMap(nid, payload, query_args))
    request.ctx.params = ParamsHandler(**params).to_payload()

@reader.get("favicon.ico")
async def favicon(request: Request):
    return empty()

@reader.route("show/<tree_name:str>", methods=["GET", "POST"])
@protected
async def show(request: Request, tree_name: str):
    tree: PermanentTree = request.app.ctx.trees.get(tree_name, None)
    if tree is None:
        raise TreeDoesNotExist(tree_name, list(request.app.ctx.trees.keys()))
    return text(tree.draw_tree(**request.ctx.params))

@reader.route("node/<tree_name:str>/<nid:str>", methods=["GET", "POST"])
@protected
async def node(request: Request, tree_name: str, nid: str) -> Node:
    tree: PermanentTree = request.app.ctx.trees.get(tree_name, None)
    if tree is None:
        raise TreeDoesNotExist(tree_name, list(request.app.ctx.trees.keys()))
    return json(tree.node(**request.ctx.params))

@reader.route("node/<tree_name:str>/<relation:str>/<nid:str>", methods=["GET", "POST"])
@protected
async def node_relations(request: Request, tree_name: str, relation: Relations, nid: str) -> Node:
    tree: PermanentTree = request.app.ctx.trees.get(tree_name, None)
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
    return json(callback(**request.ctx.params))
    

@reader.route("nodes/<tree_name:str>/<relation:str>/<nid:str>", methods=["GET", "POST"])
@protected
async def nodes_relations(request: Request, tree_name: str, relation: Relations, nid: str) -> list[Node]:
    tree: PermanentTree = request.app.ctx.trees.get(tree_name, None)
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
    return json(callback(**request.ctx.params))

@reader.route("nodes/<tree_name:str>/where", methods=["GET", "POST"])
@protected
async def nodes_where(request: Request, tree_name: str) -> list[Node]:
    tree: PermanentTree = request.app.ctx.trees.get(tree_name, None)
    if tree is None:
        raise TreeDoesNotExist(tree_name, list(request.app.ctx.trees.keys()))
    return json(tree.nodes_where(**request.ctx.params))


@reader.route("nodes/<tree_name:str>/<relation:str>/where", methods=["GET", "POST"])
@protected
async def nodes_relation_where(request: Request, tree_name: str, relation: str) -> list[list[Node]]:
    tree: PermanentTree = request.app.ctx.trees.get(tree_name, None)
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
    return json(tree.nodes_relation_where(**request.ctx.params, relation=callback, include_base=True))


"""
curl "http://localhost:8000/topics/node/children" \
-H "Accept: application/json" \
-H "Content-Type:application/json" \
-X POST --data '{"nid": "Healthcare", "fields": ["id", "alias"]}'
"""
