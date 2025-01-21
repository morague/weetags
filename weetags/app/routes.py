from __future__ import annotations

from sanic import Blueprint
from sanic.request import Request
from sanic.response import json, text, empty, HTTPResponse, html, JSONResponse
from sanic_ext import openapi

from typing import Any, Literal, get_args

from weetags.tree import Tree
from weetags.app.middlewares import extract_params
from weetags.app.authentication import Authenticator, protected
from weetags.exceptions import (
    MissingLogin,
    TreeDoesNotExist,
    UnknownRelation,
    OutputError
)



Node = dict[str, Any]
Nodes = list[dict[str, Any]]
Relations = Literal["parent", "children", "siblings", "ancestors", "descendants"]

base = Blueprint("base")
login = Blueprint("login", url_prefix="/")
records = Blueprint("records", "/records")
shower = Blueprint("shower", "/show")
utils = Blueprint("utils", "/utils")
writer = Blueprint("writer", "/records")

records.on_request(extract_params, priority=100)
shower.on_request(extract_params, priority=100)
utils.on_request(extract_params, priority=100)
writer.on_request(extract_params, priority=100)

from typing import Optional, Callable
from functools import wraps

class Auth:
    username: str
    password: str

class BaseNodeResponse:
    status: int
    reasons: str
    data: dict[str, Any]

class BaseNodesResponse:
    status: int
    reasons: str
    data: list[dict[str, Any]]

class BaseResponseError:
    status: int
    reasons: str

class NodeParams:
    nid: str
    fields: Optional[list[str]] = None

class NodesParams:
    conditions: Optional[list[list[list[str, str, Any] | str] | str]] = None
    fields: Optional[list[str]] = None
    order_by: Optional[list[str]] = None,
    axis: Optional[int] = 1,
    limit: Optional[int] = None

class RelationNodesParams:
    relation: Relations
    conditions: Optional[list[list[list[str, str, Any] | str] | str]] = None
    fields: Optional[list[str]] = None
    order_by: Optional[list[str]] = None,
    axis: Optional[int] = 1,
    limit: Optional[int] = None
    include_base: Optional[bool] = False

class AddNode:
    id: str
    parent: str
    kwargs: str|int|bool|list|dict

class DeleteNodes:
    conditions: Optional[list[list[list[str, str, Any] | str] | str]] = None
   
class UpdateNode:
    set_values: list[list[str, Any]]

class UpdateNodes:
    conditions: Optional[list[list[list[str, str, Any] | str] | str]] = None
    set_values: list[list[str, Any]]

class AppendNode:
    field_name: str
    value: str|int|bool|list|dict

class ExtendNode:
    field_name: str
    values: str|int|bool|list|dict

# @openapi.response(status=200, content=BaseResponse)
# @openapi.response(status=400, description="Server Error.", content=BaseResponseError)
# @openapi.response(status=401, description="Auth Issue.", content=BaseResponseError)
# @openapi.response(status=503, description="Service Unavailable.", content=BaseResponseError)
# @openapi.response(status=501, description="Not implemented yet.", content=BaseResponseError)


A = """\
single set of conditions: {'conditions': [[['nid', '=', 'xx'],['parent', '=', yyy]]], ...}\n
multiple set of conditions: {'conditions': [[['nid', '=', 'xx'], ['depth', '>', 1]], 'OR', [['nid', '=', 'xx'], 'OR', ['is_leaf', 'IS', 'True']]], ...}\n
"""




@base.get("favicon.ico")
@openapi.exclude()
async def favicon(request: Request):
    return empty()

@base.route("/weetags/infos", methods=["GET"])
async def infos(request: Request):
    trees = request.app.ctx.trees
    return json({"status": 200, "reasons": "OK", "data": {name:tree.info for name,tree in trees.items()}})

@base.route("/weetags/infos/<tree_name:str>", methods=["GET"])
async def tree_infos(request: Request, tree_name: str):
    tree: Tree = request.app.ctx.trees.get(tree_name, None)
    if tree is None:
        raise TreeDoesNotExist(tree_name, list(request.app.ctx.trees.keys()))
    return json({"status": 200, "reasons": "OK", "data": tree.info})

@login.get("login")
@openapi.description("Login Template. Following auth set the JwtToken as a cookie.")
async def login_page(request: Request) -> HTTPResponse:
    return html(
    """
        <html>
        <body>
            <form action="/setJwtToken" method="post">
                <label for="username">username:</label>
                <input type="text" name="username">
                <label for="password">password:</label>
                <input type="password" name="password">
                <input type="submit" value="Login">
            </form>
        </body>
        </html>
    """
    )

@login.post("auth")
@openapi.body({"application/json": Auth})
@openapi.description("Authentication endpoint. return a JwtToken, not set as cookie.")
async def authenticate(request: Request):
    authenticator: Authenticator = request.app.ctx.authenticator
    payload = request.load_json()
    username = payload.get("username", None)
    password = payload.get("password", None)
    if not all([username, password]):
        raise MissingLogin()
    if authenticator is None:
        raise ValueError("desabled Authenticator")
    token = authenticator.authenticate(request, username, password)
    return json({"status": 200, "reasons": "OK", "data": {"token": token, "cookie": False}})

@login.post("setJwtToken")
@openapi.exclude()
async def setJwtToken(request: Request):
    authenticator: Authenticator = request.app.ctx.authenticator
    form = request.get_form()
    if authenticator is None:
        raise ValueError("desabled Authenticator")
    if form is None:
        raise ValueError("missing form data")

    username = form.get("username", None)
    password = form.get("password", None)

    if username is None or password is None:
        raise MissingLogin()

    token = authenticator.authenticate(request, username, password)
    max_age = authenticator._get_user(username).get("max_age", 600) # type: ignore

    response = json({"status": 200, "reasons": "OK", "data": {"token": token, "cookie": True}})
    response.add_cookie("Authorization", f"Bearer {token}", max_age=max_age)
    return response

@records.route("node/<tree_name:str>/<nid:str>", methods=["GET"])
@openapi.description("Retrieve a Node from a tree.")
@openapi.parameter("nid", str, location="path", description="Node id")
@openapi.parameter("fields", Optional[list[str]], location="query", description="list of fields to be returned")
@protected
async def node(request: Request, tree_name: str, nid: str) -> JSONResponse:
    tree: Tree = request.app.ctx.trees.get(tree_name, None)
    if tree is None:
        raise TreeDoesNotExist(tree_name, list(request.app.ctx.trees.keys()))
    params = request.ctx.params.get_kwargs(tree.node)
    return json({"status": "200", "reasons": "OK", "data": tree.node(**params)}, status=200)

@records.route("nodes/<tree_name:str>/where", methods=["POST"])
@openapi.description("Retrieve nodes complying with a set of conditions from a tree.")
@openapi.body({"application/json": NodesParams})
@protected
async def nodes_where(request: Request, tree_name: str) -> JSONResponse:
    tree: Tree = request.app.ctx.trees.get(tree_name, None)
    if tree is None:
        raise TreeDoesNotExist(tree_name, list(request.app.ctx.trees.keys()))

    params = request.ctx.params.get_kwargs(tree.nodes_where)
    return json({"status": "200", "reasons": "OK", "data": tree.nodes_where(**params)}, status=200)


@records.route("node/<tree_name:str>/<relation:str>/<nid:str>", methods=["GET"])
@openapi.description("Retrieve nodes related to the requested base node.")
@openapi.parameter("nid", str, location="path", description="Node id")
@openapi.parameter("relation", schema= {"type":"str", "enum":["parent"]} , location="path", description="requested Relation")
@openapi.parameter("fields", Optional[list[str]], location="query", description="list of fields to be returned")
@openapi.parameter("order_by", Optional[list[str]], location="query", description="Ordering priorities")
@openapi.parameter("axis", schema= {"type": int, "enum": [0, 1]}, location="query", description="ordering axis. default: 1 (ASC).")
@openapi.parameter("limit", Optional[int], location="query", description="Number of returned records")
@protected
async def node_relations(request: Request, tree_name: str, relation: Relations, nid: str) -> JSONResponse:
    tree: Tree = request.app.ctx.trees.get(tree_name, None)
    if tree is None:
        raise TreeDoesNotExist(tree_name, list(request.app.ctx.trees.keys()))

    if relation not in get_args(Relations):
        raise UnknownRelation(relation, list(get_args(Relations)))

    if relation != "parent":
        raise OutputError(relation, "Node")

    callback = tree.parent_node
    params = request.ctx.params.get_kwargs(callback)
    return json({"status": "200", "reasons": "OK", "data": callback(**params)}, status=200)


@records.route("nodes/<tree_name:str>/<relation:str>/<nid:str>", methods=["GET"])
@openapi.description("Retrieve nodes related to the requested base node.")
@openapi.parameter("nid", str, location="path", description="Node id")
@openapi.parameter("relation", schema= {"type":"str", "enum":["siblings", "children", "ancestors", "descendants"]}, location="path", description="requested Relation")
@openapi.parameter("fields", Optional[list[str]], location="query", description="list of fields to be returned")
@openapi.parameter("order_by", Optional[list[str]], location="query", description="Ordering priorities")
@openapi.parameter("axis", schema= {"type": int, "enum": [0, 1]}, location="query", description="ordering axis. default: 1 (ASC).")
@openapi.parameter("limit", Optional[int], location="query", description="Number of returned records")
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


@records.route("nodes/<tree_name:str>/<relation:str>/where", methods=["POST"])
@openapi.description("Retrieve related nodes from base nodes complying with a set of conditions.")
@openapi.parameter("relation", schema= {"type":"str", "enum":["parent","siblings", "children", "ancestors", "descendants"]}, location="path", description="requested Relation")
@openapi.body({"application/json": NodesParams})
@protected
async def nodes_relation_where(request: Request, tree_name: str, relation: str) -> JSONResponse:
    tree: Tree = request.app.ctx.trees.get(tree_name, None)
    if tree is None:
        raise TreeDoesNotExist(tree_name, list(request.app.ctx.trees.keys()))

    if relation not in get_args(Relations):
        raise UnknownRelation(relation, list(get_args(Relations)))

    params = request.ctx.params.get_kwargs(tree.nodes_relation_where)
    return json(
        {
            "status": "200",
            "reasons": "OK",
            "data": tree.nodes_relation_where(**params)
        },
        status=200
    )




@utils.route("<tree_name:str>/related/<nid0:str>/<nid1:str>", methods=["GET"])
@openapi.parameter("nid0", str, location="path")
@openapi.parameter("nid0", str, location="path")
@protected
async def is_related(request: Request, tree_name: str, nid0: str, nid1: str) -> JSONResponse:
    tree: Tree = request.app.ctx.trees.get(tree_name, None)
    if tree is None:
        raise TreeDoesNotExist(tree_name, list(request.app.ctx.trees.keys()))
    params = request.ctx.params.get_kwargs(tree.is_related)
    return json({"status": "200", "reasons": "OK", "data": tree.is_related(**params)},status=200)

@utils.route("export/<tree_name:str>", methods=["GET"])
@openapi.exclude()
@protected
async def export(request: Request, tree_name: str) -> JSONResponse:
    raise NotImplementedError()


@shower.route("/<tree_name:str>", methods=["GET"])
@openapi.parameter("nid", str, location="query")
@openapi.parameter("style", schema= {"type":"str", "enum":["ascii", "ascii-ex", "ascii-exr", "ascii-emh", "ascii-emv", "ascii-em"]}, location="query")
@openapi.parameter("extra_space", bool, location="query", description="Increased space between branches and leaves")
@protected
async def show(request: Request, tree_name: str) -> HTTPResponse:
    tree: Tree = request.app.ctx.trees.get(tree_name, None)
    if tree is None:
        raise TreeDoesNotExist(tree_name, list(request.app.ctx.trees.keys()))

    params = request.ctx.params.get_kwargs(tree.draw_tree)
    return text(tree.draw_tree(**params))


@writer.route("add/node/<tree_name:str>/<nid:str>", methods=["POST"])
@openapi.parameter("nid", str, location="path", description="Node id")
@openapi.body({"application/json": AddNode})
@protected
async def add_node(request: Request, tree_name:str, nid: str) -> JSONResponse:
    tree: Tree = request.app.ctx.trees.get(tree_name, None)
    if tree is None:
        raise TreeDoesNotExist(tree_name, list(request.app.ctx.trees.keys()))

    params = request.ctx.params.get_kwargs(tree.add_node)
    if params.get("node",None) is None:
        raise ValueError("missing node payload")

    params.get("node").update({"id": nid})
    params.get("node").pop("nid", None)
    tree.add_node(**params)
    return json({"status": 200, "reasons": "OK", "data": {"added": nid}},status=200)

@writer.route("delete/node/<tree_name:str>/<nid:str>", methods=["GET"])
@openapi.parameter("nid", str, location="path", description="Node id")
@protected
async def delete_node(request: Request, tree_name:str, nid: str):
    tree: Tree = request.app.ctx.trees.get(tree_name, None)
    if tree is None:
        raise TreeDoesNotExist(tree_name, list(request.app.ctx.trees.keys()))

    params = request.ctx.params.get_kwargs(tree.delete_node)
    tree.delete_node(**params)
    return json({"status": 200, "reasons": "OK", "data": {"deleted": nid}},status=200)

@writer.route("delete/nodes/<tree_name:str>", methods=["POST"])
@openapi.body({"application/json": DeleteNodes})
@protected
async def deletes_nodes_where(request: Request, tree_name: str):
    tree: Tree = request.app.ctx.trees.get(tree_name, None)
    if tree is None:
        raise TreeDoesNotExist(tree_name, list(request.app.ctx.trees.keys()))
    
    params = request.ctx.params.get_kwargs(tree.delete_nodes_where)
    tree.delete_nodes_where(**params)
    return json({"status": 200, "reasons": "OK", "data": {}},status=200)

@writer.route("update/node/<tree_name:str>/<nid:str>", methods=["POST"])
@openapi.parameter("nid", str, location="path", description="Node id")
@openapi.body({"application/json": UpdateNode})
@protected
async def update_node(request: Request, tree_name:str, nid: str):
    tree: Tree = request.app.ctx.trees.get(tree_name, None)
    if tree is None:
        raise TreeDoesNotExist(tree_name, list(request.app.ctx.trees.keys()))

    params = request.ctx.params.get_kwargs(tree.update_node)

    if params.get("set_values",None) is None:
        raise ValueError("missing set_values payload")

    tree.update_node(nid, params.get("set_values"))
    return json({"status": 200, "reasons": "OK", "data": {"updated": nid}},status=200)

@writer.route("update/nodes/<tree_name:str>", methods=["POST"])
@openapi.body({"application/json": UpdateNodes})
@protected
async def update_nodes(request: Request, tree_name: str):
    tree: Tree = request.app.ctx.trees.get(tree_name, None)
    if tree is None:
        raise TreeDoesNotExist(tree_name, list(request.app.ctx.trees.keys()))
    
    params = request.ctx.params.get_kwargs(tree.update_nodes_where)
    tree.update_nodes_where(**params)
    return json({"status": 200, "reasons": "OK", "data": {}},status=200)

@writer.route("append/node/<tree_name:str>/<nid:str>", methods=["POST"])
@openapi.parameter("nid", str, location="path", description="Node id")
@openapi.body({"application/json": AppendNode})
@protected
async def append_nodes(request: Request, tree_name: str, nid: str):
    tree: Tree = request.app.ctx.trees.get(tree_name, None)
    if tree is None:
        raise TreeDoesNotExist(tree_name, list(request.app.ctx.trees.keys()))
    
    params = request.ctx.params.get_kwargs(tree.append_node)
    tree.append_node(**params)
    return json({"status": 200, "reasons": "OK", "data": {}},status=200)

@writer.route("extend/node/<tree_name:str>/<nid:str>", methods=["GET", "POST"])
@openapi.parameter("nid", str, location="path", description="Node id")
@openapi.body({"application/json": ExtendNode})
@protected
async def extend_node(request: Request, tree_name: str, nid: str):
    tree: Tree = request.app.ctx.trees.get(tree_name, None)
    if tree is None:
        raise TreeDoesNotExist(tree_name, list(request.app.ctx.trees.keys()))
    
    params = request.ctx.params.get_kwargs(tree.extend_node)
    tree.extend_node(**params)
    return json({"status": 200, "reasons": "OK", "data": {}},status=200)




