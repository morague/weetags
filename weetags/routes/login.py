from __future__ import annotations

from collections import ChainMap
from sanic import Blueprint
from sanic.request import Request
from sanic.response import json, text
from sanic_ext import openapi


from typing import Any, Literal, get_args

from weetags.params_handler import ParamsHandler
from weetags.trees.permanent_tree import PermanentTree
from weetags.exceptions import TreeDoesNotExist, UnknownRelation, OutputError

login = Blueprint("login", url_prefix="/login")


@login.on_request(priority=100)
async def extract_params(request: Request):
    query_args = {k:(v[0] if len(v) == 1 else v) for k,v in request.args.items()}
    payload = request.load_json() or {}
    request.ctx.params = dict(ChainMap(payload, query_args))
    
@login.route("/", methods=["GET", "POST"])
async def authenticate(request: Request):
    return request.app.ctx.authenticator.login(request)
