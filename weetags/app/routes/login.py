from __future__ import annotations

from sanic import Blueprint
from sanic.request import Request
from sanic.response import json, text, html, HTTPResponse
from sanic_ext import openapi

from typing import Any, Literal, get_args

from weetags.trees.tree import Tree
from weetags.exceptions import MissingLogin
from weetags.app.authentication.authentication import Authenticator

login = Blueprint("login", url_prefix="/")

@login.get("login")
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
    max_age = authenticator.get_user(username).get("max_age", 600) # type: ignore

    response = json({"status": 200, "reasons": "OK", "data": {"token": token, "cookie": True}})
    response.add_cookie("Authorization", f"Bearer {token}", max_age=max_age)
    return response
