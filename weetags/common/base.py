from __future__ import annotations

from sqlalchemy import (
    TEXT,
    Table,
    Column, 
    ForeignKey,
    Integer,
    Text,
    Boolean,
    JSON, 
    DateTime,
    false
)

from weetags.common.configs import FieldDefinition


def field_is_reserved(field: str) -> bool:
    if field in TreeTopologyDefinition().namespace:
        raise KeyError(f"Field name: {field} is a reserved namespace.")
    return True


class TreeUsers(object):
    columns = [
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("username", Text, unique=True, nullable=False, index=True),
        Column("password_hash", Text, nullable=False),
        Column("roles", JSON, nullable=False),
    ]

    def __init__(self) -> None:
        self.columns = TreeUsers.columns

class TreeRules(object):
    columns = [
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("priority", Integer, nullable=False),
        Column("type", Text, nullable=False),
        Column("method", Text, nullable=False),
        Column("rule", Text, nullable=True),
        Column("role", Text, nullable=True)
    ]

    def __init__(self) -> None:
        self.columns = TreeRules.columns



class TreeTopologyDefinition(object):
    
    columns = [
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("name", Text, unique=True, nullable=False, index=True),
        Column("parent", Integer, nullable=True),
        Column("children", JSON, nullable=False),
        Column("path", Text, unique=True, nullable=False, index=True),
        Column("level", Integer, nullable=False)
    ]

    @property
    def namespace(self) -> list[str]:
        return [c.name for c in self.columns]

    @property
    def generated_keys(self) -> list[str]:
        return ["id", "children", "path", "level"]
    
    @property
    def base_keys(self) -> list[str]:
        return [c.name for c in self.columns if c.name not in self.generated_keys]

    def __init__(self) -> None:
        self.columns = TreeTopologyDefinition.columns


class TreeMetadataDefinition:
    fk: list[str] = ["id"]

    def generate_columns(self, fields: list[FieldDefinition], topology: Table) -> list[Column]:
        columns = [
            Column("id", Integer, ForeignKey(topology.c.id, ondelete="CASCADE"), primary_key=True)
        ]
        [columns.append(c.into_column()) for c in fields]
        return columns

class TreeViewDefinition:
    STMT = "{create} main.{name} AS SELECT t.name, t.path, t.parent, t.children, t.level, m.* FROM {topology_name} AS t JOIN {meta_name} AS m on m.id = t.id;"

    def __init__(self, dialect: str) -> None:
        self.dialect = dialect
        
    def render_view_query(self, name: str, topology: Table, metadata: Table) -> str:
        create = self._create()
        return TreeViewDefinition.STMT.format(
            create=create, 
            name=name,  
            topology_name=topology.name, 
            meta_name=metadata.name
        )
    
    def _create(self) -> str:
        match self.dialect:
            case "sqlite":
                create = "CREATE VIEW IF NOT EXISTS"
            case "psql":
                create = "CREATE OR REPLACE VIEW"
            case _:
                raise ValueError(f"Unknown database dialect: {self.dialect}")
        return create
