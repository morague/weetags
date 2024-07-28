import logging
from time import perf_counter
from collections import ChainMap
from urllib.parse import unquote
from sanic.request import Request
from sanic.response import HTTPResponse

from weetags.app.params_handler import ParamsParser

logger = logging.getLogger("endpointAccess")

async def log_entry(request: Request) -> None:
    request.ctx.t = perf_counter()

async def log_exit(request: Request, response: HTTPResponse) -> None:
    perf = round(perf_counter() - request.ctx.t, 5)
    if response.status == 200:
        logger.info(f"[{request.host}] > {request.method} {request.url} [{str(response.status)}][{str(len(response.body))}b][{perf}s]")

async def extract_params(request: Request) -> None:
    nid = {k:unquote(v) for k,v in request.match_info.items() if k == "nid"} or {}
    query_args = {k:(v[0] if len(v) == 1 else v) for k,v in request.args.items()}
    payload = request.load_json() or {}
    params = dict(ChainMap(nid, payload, query_args))
    request.ctx.params = ParamsParser(**params)

async def cookie_token(request: Request) -> None:
    cookie = request.cookies.get("Authorization", None)
    if cookie is not None:
        request.headers.add("Authorization", cookie)
