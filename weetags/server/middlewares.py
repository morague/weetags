from __future__ import annotations

import logging
import traceback
from time import perf_counter
from sanic import Request, HTTPResponse, json

logger = logging.getLogger("access")




async def go_fast(request: Request) -> None:
    request.ctx.t = perf_counter()

async def authorize(request: Request) -> None:
    print(request.path)

async def log_exit(request: Request, response: HTTPResponse) -> None:
    perf, size = None, None

    if getattr(request.ctx, "t", None) is not None:
        perf = round(perf_counter() - request.ctx.t, 5)

    if response.body is not None:
        size = len(response.body)

    if response.status == 200:
        logger.info(
            f"{request.host} > {request.method} {request.url} [{str(response.status)}][{str(size)}b][{perf}s]"
        )


async def error_handler(request: Request, exception: Exception):
    perf = None  # for some unknown reasons perf middleware get skipped for some requests. thus need to check if t is stored.
    if getattr(request.ctx, "t", None) is not None:
        perf = round(perf_counter() - request.ctx.t, 5)
    status = getattr(exception, "status", 500)
    logger.error(
        f"{request.host} > {request.method} {request.url} : {str(exception)} [{str(status)}][{str(len(str(exception)))}b][{perf}s]"
    )

    logger.error(traceback.format_exc())
    return json({"status": status, "reasons": str(exception)}, status=status)