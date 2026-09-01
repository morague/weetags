from __future__ import annotations
from ast import Str
import select
from typing import Literal

import jwt
import argon2
from sanic import Request, Sanic
from sqlalchemy import Row, select, create_engine
from sqlalchemy.engine import Engine as BaseEngine

import weetags.common.base as base
from weetags.common.engine import Engine
from weetags.common.serializer import BaseSerializer
from weetags.common.uri import EngineURI

"""
users
    user_id, username, password_hash, roles

permissions
    permission_id, rule, roles, priority

    rule is a regex operation applied on the path
"""

class Authenticator(Engine):
    def __init__(
        self, 
        base_engine: BaseEngine, 
        uri: EngineURI, 
        serializer: BaseSerializer | None = None,
    ) -> None:
        super().__init__(base_engine, uri, serializer)

    @classmethod
    def from_uri(cls, uri: EngineURI) -> Authenticator:
        engine = create_engine(uri.uri, echo=False)
        return cls(engine, uri)

    def authenticate(self, username: str, password: str) -> str:
        user = self.get_user(username)
        if user is None:
            raise ValueError(f"Unknown Username: {username}")

        _,_,password_hash,roles=user

        hasher = argon2.PasswordHasher()
        hasher.verify(password_hash, password)
        secret_key: str = "aaaa"
        return jwt.encode({"roles": roles, "max_age": 0}, secret_key)

    def authorize(self, method: str, blueprint: str, path: str, token: str | None = None) -> bool:

        if token is None:
            raise KeyError("Authorization token required")
        return True

    def get_user(self,username: str) -> Row | None:
        table = self._table_or_raise(f"users")
        stmt = select(table).where(table.c.username == username)
        user = self.execute(stmt).fetchone()
        return user

    def resgister_user(self, username: str, password: str, roles: list[str]) -> int:
        table = self._table_or_raise("users")

        hasher = argon2.PasswordHasher()
        password_hash = hasher.hash(password)
        user_id = self._write(table, {"username": username, "password_hash": password_hash, "roles": roles})
        return user_id

    def register_rule(
        self, 
        rtype:Literal["path", "blueprint"], 
        method: str, 
        rule: str, 
        role: str, 
        priority: int
    ) -> None:
        table = self._table_or_raise("permissions")

        if rtype not in ["path", "blueprint"]:
            raise ValueError(f"Permission type is either `path` or `blueprint`")
        
        self._write(table, {"type": rtype, "method": method, "rule": rule, "role": role, "priority": priority})

    def initialize(self) -> None:
        self._initialize_users()
        self._initialize_permissions()

    def _initialize_users(self) -> None:
        self.create_table(f"users", base.TreeUsers.columns)

    def _initialize_permissions(self) -> None:
        self.create_table(f"permissions", base.TreePermissions.columns)