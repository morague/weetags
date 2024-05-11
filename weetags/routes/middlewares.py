import logging
from time import perf_counter
from sanic.request import Request
from sanic.response import HTTPResponse

logger = logging.getLogger("endpointAccess")

async def log_entry(request: Request) -> HTTPResponse:
    request.ctx.t = perf_counter()

async def log_exit(request: Request, response: HTTPResponse) -> HTTPResponse:
    perf = round(perf_counter() - request.ctx.t, 5)
    if response.status == 200:
        logger.info(f"[{request.host}] > {request.method} {request.url} [{str(response.status)}][{str(len(response.body))}b][{perf}s]")