from __future__ import annotations

from sqlalchemy import (
    Table,
    Column, 
    ForeignKey,
    Integer,
    Text,
    Boolean,
    JSON, 
    DateTime
)

from weetags.common.configs import FieldDefinition

class TreeTopologyDefinition(object):
    
    columns = [
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("name", Text, unique=True, nullable=False, index=True),
        Column("parent", Integer, nullable=True),
        Column("children", JSON, nullable=False),
        Column("path", Text, unique=True, nullable=False, index=True),
    ]

    @property
    def namespace(self) -> list[str]:
        return [c.name for c in self.columns]

    @property
    def generated_keys(self) -> list[str]:
        return ["id", "children", "path"]
    
    @property
    def base_keys(self) -> list[str]:
        return [c.name for c in self.columns if c.name not in self.generated_keys]

    def __init__(self) -> None:
        self.columns = TreeTopologyDefinition.columns


class TreeMetadataDefinition:
    fk: list[str] = ["id"]

    def generate_columns(self, fields: list[FieldDefinition], core: Table) -> list[Column]:
        columns = [
            Column("id", Integer, ForeignKey(core.c.id, ondelete="CASCADE"), primary_key=True)
        ]
        [columns.append(c.into_column()) for c in fields]
        return columns

class TreeViewDefinition:
    STMT = "{create} main.{name} ({view_fields}) AS SELECT {base_fields} FROM {core_name} AS c JOIN {meta_name} AS m on m.id = c.id;"

    def __init__(self, dialect: str) -> None:
        self.dialect = dialect
        
    def render_view_query(self, name: str, core: Table, metadata: Table) -> str:
        create = self._create()
        base_fields, view_fields = self._fields(core, metadata)
        return TreeViewDefinition.STMT.format(
            create=create, 
            name=name, 
            view_fields=", ".join(view_fields), 
            base_fields=", ".join(base_fields), 
            core_name=core.name, 
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

    def _fields(self, core: Table, metadata: Table) -> tuple[list[str], list[str]]:
        core_fields = [f.name for f in core.columns]
        meta_fields = [f.name for f in metadata.columns if f.name not in TreeMetadataDefinition.fk]

        view_fields = core_fields + meta_fields
        base_fields = [f"c.{n}" for n in core_fields] + [f"m.{n}" for n in meta_fields]
        return (base_fields, view_fields)
