from __future__ import annotations
from attrs import field, define

from weetags.database.table import Table, Field

@define(slots=False, repr=False)
class UsersTable(Table):
    table_name: str = field(default="weetags__users")
    username: Field = field(default=Field("username", "TEXT", pk=True, nullable=False))
    password_sha256: Field = field(default=Field("password_sha256", "TEXT", nullable=False))
    auth_level: Field = field(default=Field("auth_level", "JSONLIST", nullable=False))
    salt: Field = field(default=Field("salt","TEXT", nullable=False))
    max_age: Field = field(default=Field("max_age", "REAL", nullable=False))

    def __repr__(self) -> str:
        return super().__repr__()

    @classmethod
    def initialize(cls):
        return cls()

@define(slots=False, repr=False)
class RestrictionsTable(Table):
    table_name: str = field(default="weetags__restrictions")
    tree: Field = field(default=Field("tree", "TEXT", pk=True, nullable=False))
    blueprint: Field = field(default=Field("blueprint", "TEXT", pk=True, nullable=False))
    auth_level: Field = field(default=Field("auth_level", "JSONLIST", nullable=False))

    def __repr__(self) -> str:
        return super().__repr__()

    @classmethod
    def initialize(cls):
        return cls()


if __name__ == "__main__":
    print(UsersTable.initialize().create())
    print("\n\n")
    print(RestrictionsTable.initialize().create())

    from weetags.database.db import _Db

    db = _Db()
    db.create_table(UsersTable.initialize(),RestrictionsTable.initialize())
