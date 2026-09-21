from __future__ import annotations

import jwt
import time
import argon2
from sqlalchemy import Row, select, delete, create_engine
from sqlalchemy.engine import Engine as BaseEngine

from typing import Literal, Any

import weetags.common.base as base
from weetags.common.engine import Engine
from weetags.common.serializer import BaseSerializer
from weetags.common.uri import EngineURI


class Users:
    _engine: Engine

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def initialize_users(self) -> None:
        self._engine.create_table(f"users", base.TreeUsers.columns)

    def get_user(self, username: str) -> Row | None:
        table = self._engine._table_or_raise(f"users")
        stmt = select(table).where(table.c.username == username)
        user = self._engine.execute(stmt).fetchone()
        return user

    def get_user_or_raise(self, username: str) -> Row:
        user = self.get_user(username)
        if user is None:
            raise ValueError(f"Unknown username: {username}")
        return user

    def set_user(self, username: str, password: str, roles: list[str]) -> int:
        hasher = argon2.PasswordHasher()
        password_hash = hasher.hash(password)

        table = self._engine._table_or_raise("users")
        user_id = self._engine._write(table, {"username": username, "password_hash": password_hash, "roles": roles})
        return user_id

    def remove_user(self, username: str) -> None:
        table = self._engine._table_or_raise("users")
        stmt = delete(table).where(table.c.username == username)
        with self._engine.sessionmaker() as session:
            session.execute(stmt)
            session.commit()

    def update_user_password(self, username: str, password: str, new_password: str, confirmation: str) -> None:
        if new_password != confirmation:
            raise ValueError("Password and confirmation are different")
        table = self._engine._table_or_raise("users")

        hasher = argon2.PasswordHasher()
        user = self.get_user_or_raise(username)
        uid, _, pwhash,_ = user
        hasher.verify(pwhash, password)        
        password_hash = hasher.hash(new_password)
        self._engine._update(table, [uid], {"password_hash": password_hash})

    def update_user_roles(self, username: str, roles: list[str]) -> None:
        uid, *_ = self.get_user_or_raise(username)
        table = self._engine._table_or_raise("users")
        self._engine._update(table, [uid], {"roles": roles})



class Rules:
    _engine: Engine
    MAXIMUM_PRIORITY: int = 1000

    def __init__(self, engine: Engine, maximum_priority: int = 1000) -> None:
        self._engine = engine
        self.MAXIMUM_PRIORITY = maximum_priority

    def initialize_rules(self, recreate_rules: bool = False) -> None:
        if recreate_rules:
            self.drop_rules()
            self._engine.reflect()
        self._engine.create_table("rules", base.TreeRules.columns)

    def build(self, *rules: dict[str, Any], default: Literal["deny", "allow"]) -> None:
        existing = self.get_rules()
        if len(existing) == 0:
            self._set_default(default)
            self.compile_rules(*rules)

    def drop_rules(self) -> None:
        # self._engine._drop("rules")
        table = self._engine._table("rules")
        if table is not None:
            self._engine._clear(table)
        

    def get_rule(self, blueprint: str, method: str, path: str) -> Row | None:
        _ = self._engine._table_or_raise("rules")

        stmt = f"""
            SELECT * FROM rules
            WHERE 
                (type = 'path' AND '{path}' GLOB rule AND '{method}' REGEXP method)
                OR (type = 'blueprint' AND rule = '{blueprint}')
            ORDER BY PRIORITY
            LIMIT 1;
        """
        res = self._engine.execute_str_statement(stmt)
        return res.fetchone()

    def get_rules(self) -> list[dict[str, Any]]:
        table = self._engine._table_or_raise("rules")
        stmt = select(table)
        return [r._asdict() for r in self._engine.execute(stmt).fetchall()]

    def set_rule(
        self, 
        type:Literal["path", "blueprint"], 
        methods: list[str],
        rule: str,  
        priority: int,
        role: str | None = None,
    ) -> None:
        if type not in ["path", "blueprint"]:
            raise ValueError(f"Permission type is either `path` or `blueprint`")
        if priority >= self.MAXIMUM_PRIORITY:
            raise ValueError(f"priority must be lower than: {self.MAXIMUM_PRIORITY}")

        table = self._engine._table_or_raise("rules")
        if "*" in methods:
            method = ".*"
        else:
            method = "|".join(methods)
        self._engine._write(table, {"priority": priority, "type": type, "method": method, "rule": rule, "role": role})

    def remove_rule(self, rid: int) -> None: 
        table = self._engine._table_or_raise("rules")
        self._engine._delete(table, rid)

    def compile_rules(self, *rules: dict[str, Any]) -> None:
        for rule in rules:
            self.set_rule(**rule)

    def _set_default(self, default: Literal["deny", "allow"]) -> None:
        if default == "deny":
            table = self._engine._table_or_raise("rules")
            self._engine._write(table, {"priority": 999, "type": "path", "method": ".*", "rule": "/static*", "role": "*"})
            self._engine._write(table, {"priority": 999, "type": "blueprint", "method": ".*", "rule": "auth", "role": "*"})
            self._engine._write(table, {"priority": 1000, "type": "path", "method": ".*", "rule": "/*", "role": None})
        


class Authenticator(Engine):
    MAX_AGE: int = 10800
    SECRET_KEY: str
    MAXIMUM_PRIORITY: int = 1000

    def __init__(
        self, 
        base_engine: BaseEngine, 
        uri: EngineURI, 
        secret_key: str,
        max_age: int = 10800,
        maximum_priority: int = 1000,
        serializer: BaseSerializer | None = None,
    ) -> None:
        super().__init__(base_engine, uri, serializer)
        self.SECRET_KEY = secret_key
        self.MAX_AGE = max_age
        self.MAXIMUM_PRIORITY = maximum_priority

    
    @classmethod
    def from_configs(cls, configs: dict[str, Any]) -> Authenticator:
        build = configs.pop("builder", {})
        rules= configs.pop("rules", {})
        uri = configs.pop("uri", {})
        engine_uri = EngineURI(**uri)

        engine = create_engine(engine_uri.uri, echo=False)
        auth = cls(engine, uri=engine_uri, **configs)
        auth.build(*rules, **build)
        return auth
        

    @property
    def users(self) -> Users:
        return Users(self)

    @property
    def rules(self) -> Rules:
        return Rules(self, maximum_priority=self.MAXIMUM_PRIORITY)

    def build(self, *rules: dict[str, Any], default: Literal["deny", "allow"] = "deny",  recreate_rules: bool = False) -> None:
        self.initialize(recreate_rules)
        self.rules.build(*rules, default=default)
        self.reflect()

    def initialize(self, recreate_rules: bool = False) -> None:
        self.users.initialize_users()
        self.rules.initialize_rules(recreate_rules)
        self.reflect()

    def authenticate(self, username: str, password: str) -> str:
        user = self.users.get_user(username)
        if user is None:
            raise ValueError(f"Unknown Username: {username}")

        _,_,password_hash,roles=user

        hasher = argon2.PasswordHasher()
        hasher.verify(password_hash, password)
        return jwt.encode({"roles": roles, "max_age": time.time() + self.MAX_AGE}, self.SECRET_KEY)

    def authorize(self, blueprint: str, method: str, path: str, token: str | None = None) -> bool:
        rule = self.rules.get_rule(blueprint, method, path)
        if rule is None:
            return True

        _, priority, rtype, _, _, role = rule
        if role is None:
            raise ValueError("Forbidden") #403 
        if role == "*":
            return True

        if token is None:
            raise KeyError("Unauthorized") # 401
        payload = self._authorize(token)
        if payload is None:
            raise KeyError("Unauthorized") # 401

        max_age = payload.get("max_age", 0)
        if time.time() > max_age:
            raise ValueError("Unauthorized") # 401

        user_roles = payload.get("roles", [])
        if role not in user_roles:
            raise KeyError("Unauthorized") # 401
        return True

    def _authorize(self, token: str) -> dict[str, Any] | None:
        try:
            payload = jwt.decode(token, self.SECRET_KEY, algorithms=["HS256"])
        except Exception as e:
            print(e)
            payload = None
        return payload