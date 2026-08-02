from __future__ import annotations

from pathlib import Path
from attrs import define, field, Attribute

from typing import Type, Any


def str_or_none(instance: Type, attribute: Attribute, value: Any):
    if not isinstance(value, str) and value is not None:
        raise ValueError(f"Argument {attribute} is not of type[str|None]")


def int_or_none(instance: Type, attribute: Attribute, value: Any):
    if not isinstance(value, int) and value is not None:
        raise ValueError(f"Argument {attribute} is not of type[int|None]")



@define(frozen=True, kw_only=True, repr=False)
class EngineURI:
    """
    Handle Database URI
    allow for sqlite in-memory when database is None.
    """

    dialect: str = field(default="sqlite")
    database: str | None = field(default=":memory:", validator=[str_or_none])
    driver: str | None = field(default=None, validator=[str_or_none])
    username: str | None = field(default=None, validator=[str_or_none])
    password: str | None = field(default=None, validator=[str_or_none])
    host: str | None = field(default="localhost", validator=[str_or_none])
    port: int | None = field(default=5432, validator=[int_or_none])

    def __attrs_post_init__(self) -> None:
        if self.database is None:
            assert self.dialect == "sqlite"
        if self.dialect != "sqlite":
            assert self.username is not None
            assert self.password is not None
            assert self.host is not None
            assert self.port is not None
            assert self.database is not None

    def __repr__(self) -> str:
        return self.uri

    @property
    def uri(self) -> str:
        if self.dialect == "sqlite":
            return self._sqlite_uri()
        else:
            return self._uri()

    @property
    def in_memory(self) -> bool:
        return self.dialect == "sqlite" and (self.database == ":memory:" or self.database == "")



    def _sqlite_uri(self) -> str:
        uri = "sqlite://" # default: equivalent to :memory:
        if self.database is not None:
            db_path = Path(self.database)
            path = "/".join(db_path.parts)
            if db_path.is_absolute():
                uri += f"{path}"
            else:
                uri += f"/{path}"
        return uri

    def _uri(self) -> str:
        dnd = [self.dialect]
        if self.driver is not None:
            dnd.append(self.driver)
        dnd_fmt = "+".join(dnd)
        return f"{dnd_fmt}://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"


