from __future__ import annotations

import jwt
import time
import random
from string import ascii_letters
from hashlib import sha256
from functools import wraps
from attrs import field, define, validators

from pathlib import Path

from sanic import text
from sanic.request import Request
from sanic_routing import Route

from typing import Optional, Any

from weetags.app.authentication.schema import UsersTable, RestrictionsTable
from weetags.app.utils import generate_uuid
from weetags.database.db import _Db
from weetags.exceptions import (
    OutatedAuthorizationToken,
    AuthorizationTokenRequired,
    InvalidToken,
    MissingLogin,
    InvalidLogin,
    AccessDenied
)

def protected(f):
    @wraps(f)
    async def wrapped(request: Request, *args, **kwargs):
        authenticator: Authenticator = request.app.ctx.authenticator
        if authenticator is None:
            response = await f(request, *args, **kwargs)
            return response

        elif authenticator.authorize(request):
            response = await f(request, *args, **kwargs)
            return response
        else:
            raise AccessDenied()
    return wrapped

class Authenticator(_Db):
    def __init__(self, db: str | Path = "db.db") -> None:
        super().__init__(db)

    @classmethod
    def initialize(
        cls,
        users: list[dict[str, Any]] | None = None,
        restrictions: list[dict[str, Any]] | None = None,
        db: str | Path = "db.db",
        replace: bool = False
    ) -> Authenticator:
        authenticator = cls(db)

        tables = authenticator.get_tables("weetags")
        if ((len(tables) == 0 or replace) and users is None):
            raise ValueError("you must define users and restrictions in your configurations")

        if len(tables) > 0 and replace:
            authenticator.drop("weetags__restrictions")
            authenticator.drop("weetags__users")

        if len(tables) == 0 or replace:
            print("create tables")
            authenticator.create_table(
                UsersTable.initialize(),
                RestrictionsTable.initialize()
            )

        if users is None and replace:
            raise ValueError("Set some users or remove the authenticator from the configs")

        if users is not None:
            authenticator.add_users(*users)
        if restrictions is not None:
            authenticator.add_restrictions(*restrictions)

        return authenticator


    def authenticate(self, request: Request, username: str, password: str) -> str:
        user = self.get_user(username)
        if user is None:
            raise InvalidLogin()

        max_age = user.get("max_age")
        auth_level = user.get("auth_level")
        salted_password= user["salt"] + password
        password_sha256 = sha256(salted_password.encode()).hexdigest()

        if password_sha256 != user["password_sha256"]:
            raise InvalidLogin()

        return jwt.encode({"auth_level": auth_level, "max_age": self._max_time_age(max_age)}, request.app.config.SECRET)

    def authorize(self, request: Request) -> bool:
        token = request.token
        route = request.route
        tree = request.match_info.get("tree_name")

        if token is None:
            raise AuthorizationTokenRequired()

        if tree is None:
            raise ValueError("tree name not available")

        if route is None:
            raise ValueError("Route name not available")

        try:
            payload = jwt.decode(
                token, request.app.config.SECRET, algorithms=["HS256"]
            )
        except jwt.exceptions.InvalidTokenError:
            raise InvalidToken()

        _, blueprint, _ = route.name.split('.')
        restriction = self.get_restriction(tree, blueprint)

        if restriction is not None and not any([level in payload["auth_level"] for level in restriction["auth_level"]]):
            raise AccessDenied()

        if int(time.time()) > payload["max_age"]:
            raise OutatedAuthorizationToken()
        return True

    def get_user(self, username: str) -> dict[str, Any] | None:
        return self.con.execute("SELECT username, password_sha256, auth_level, salt, max_age FROM weetags__users WHERE username=?", [username]).fetchone()

    def get_restriction(self, tree: str, blueprint: str) -> dict[str, Any] | None:
        return self.con.execute("SELECT auth_level FROM weetags__restrictions WHERE tree=? AND blueprint=?", [tree, blueprint]).fetchone()

    def add_users(self, *users_settings) -> None:
        for settings in users_settings:
            parsed_user = User(**settings)
            self._insert_user(parsed_user)

    def add_restrictions(self, *restrictions_settings) -> None:
        for settings in restrictions_settings:
            parsed_restriction = Restriction(**settings)
            self._insert_restriction(parsed_restriction)


    def _insert_restriction(self, restriction: Restriction) -> None:
        self.write(
            "weetags__restrictions",
            ["tree", "blueprint", "auth_level"],
            restriction.to_sql()
        )

    def _insert_user(self, user: User) -> None:
        self.write(
            "weetags__users",
            ["username", "password_sha256", "auth_level", "salt", "max_age"],
            user.to_sql()
        )

    def _max_time_age(self, max_age: int) -> int:
        return int(time.time()) + max_age


@define(slots=True, kw_only=True)
class User:
    username: str = field(validator=[validators.instance_of(str)])
    password: str = field(validator=[validators.instance_of(str)])
    password_sha256: str = field(default=None)
    auth_level: list[str] = field()
    salt: str = field(default=None)
    max_age: int = field(validator=[validators.instance_of(int)])

    def __attrs_post_init__(self):
        if self.password_sha256 is None and self.salt is None:
            self.generate_salt()
            self.generate_sha256()

    def generate_salt(self):
        self.salt =  "".join([random.choice(ascii_letters) for _ in range(16)])

    def generate_sha256(self):
        salted_password = self.salt + self.password
        self.password_sha256 = sha256(salted_password.encode()).hexdigest()

    def to_sql(self):
        return [self.username, self.password_sha256, self.auth_level, self.salt, self.max_age]

@define(slots=True, kw_only=True)
class Restriction:
    tree: str = field(validator=[validators.instance_of(str)])
    blueprint: str = field(validator=[validators.instance_of(str)])
    auth_level: list[str] = field(validator=[validators.instance_of(list)])

    def to_sql(self):
        return [self.tree, self.blueprint, self.auth_level]
