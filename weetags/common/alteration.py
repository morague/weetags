from __future__ import annotations

from sqlalchemy import Index
from sqlalchemy.schema import CreateIndex, DropIndex

from weetags.common.engine import Engine
from weetags.common.types import AcceptedFieldType
from weetags.common.configs import FieldType
from weetags.common.base import TreeTopologyDefinition

class Alteration:
    tree_name: str
    _engine: Engine

    def __init__(self, tree_name: str, engine: Engine) -> None:
        self.tree_name = tree_name
        self._engine = engine

        self._engine.get_tree(tree_name)

    def add_field(
        self,
        name: str, 
        dtype: AcceptedFieldType, 
        nullable: bool = True, 
        unique: bool = False, 
    ) -> None:
        if name in TreeTopologyDefinition().namespace:
            raise KeyError(f"Field name: {name} is a reserved namespace.")
        if name in self._engine._metadata.columns.keys():
            raise KeyError(f"Field name: {name} already exist")
        field_type = FieldType.from_value(dtype)
        opts = " ".join([o[0] for o in [("NULLABLE", nullable), ("UNIQUE",unique)] if o[1]])
        stmt = f"ALTER TABLE _{self.tree_name}_metadata ADD COLUMN {name} {field_type.name} {opts};"
        self._engine.execute_statement(stmt)

    def remove_field(self, name: str) -> None:
        if name in TreeTopologyDefinition().namespace:
            raise KeyError(f"Field name: {name} is part of the tree topoligy. Therefore, not removeable")
        if name not in self._engine._metadata.columns.keys():
            raise KeyError(f"Field name: {name} doesn't exist")
        stmt = f"ALTER TABLE _{self.tree_name}_metadata DROP COLUMN {name};"
        self._engine.execute_statement(stmt)

    def rename_field(self, name: str, new_name: str) -> None:
        if name in TreeTopologyDefinition().namespace:
            raise KeyError(f"Field name: {name} is a reserved namespace.")
        if new_name in TreeTopologyDefinition().namespace:
            raise KeyError(f"Field name: {new_name} is a reserved namespace.")
        if new_name in self._engine._metadata.columns.keys():
            raise KeyError(f"Field name: {new_name} does already exist")
        stmt = f"ALTER TABLE _{self.tree_name}_metadata RENAME {name} TO {new_name};"
        self._engine.execute_statement(stmt)

    def move_field_data(self) -> None:
        ...

    def add_field_nullable(self, name: str) -> None:
        if self._engine.uri.dialect != "postgres":
            raise ValueError("Unique constraint manipulation after tree creation is only available for psql dialect.")

        if name not in self._engine._metadata.columns.keys():
            raise KeyError(f"Field name: {name} doesn't exist")

        field = self._engine._metadata.columns[name]
        if field.nullable:
            return
        
        stmt = f"ALTER TABLE _{self.tree_name}_metadata ALTER COLUMN {name} DROP NOT NULL;"
        self._engine.execute_statement(stmt)

    def remove_field_nullable(self, name: str) -> None:
        if self._engine.uri.dialect == "sqlite":
            version = self._engine.engine.dialect.server_version_info
            if version is not None and version[0] >= 3 and version[1] >= 53:
                pass
            else:
                raise ValueError("Nullable constraint manipulation require sqlite engine version >= 3.53.0")
        elif self._engine.uri.dialect != "postgres":
            raise ValueError("Unique constraint manipulation after tree creation is only available for psql dialect.")

        if name not in self._engine._metadata.columns.keys():
            raise KeyError(f"Field name: {name} doesn't exist")

        field = self._engine._metadata.columns[name]
        if field.nullable is False:
            return
        stmt = f"ALTER TABLE _{self.tree_name}_metadata ALTER COLUMN {name} SET NOT NULL;"
        self._engine.execute_statement(stmt)




    def add_field_unique_constraint(self, *names: str) -> None:
        """sqlite have to recreate the table... or limit this to psql"""

        if self._engine.uri.dialect == "sqlite":
            version = self._engine.engine.dialect.server_version_info
            if version is not None and version[0] >= 3 and version[1] >= 53:
                pass
            else:
                raise ValueError("Nullable constraint manipulation require sqlite engine version >= 3.53.0")
        elif self._engine.uri.dialect != "postgres":
            raise ValueError("Unique constraint manipulation after tree creation is only available for psql dialect.")

        for name in names:
            if name not in self._engine._metadata.columns.keys():
                raise KeyError("Unique constraint can only be set on a list of metadata fields")

        constraint_name = "_".join(names) + f"_unique"
        fields = ", ".join(names)

        stmt = f"ALTER TABLE _{self.tree_name}_metadata ADD CONSTRAINT {constraint_name} UNIQUE ({fields});"
        self._engine.execute_statement(stmt)

    def remove_field_unique_constraint(self, *names: str) -> None:
        """sqlite have to recreate the table... or limit this to psql dialect ??
        
        TODO: test against existing constraints...
        """

        if self._engine.uri.dialect != "postgres":
            raise ValueError("Unique constraint manipulation after tree creation is only available for psql dialect.")

        for name in names:
            if name not in self._engine._metadata.columns.keys():
                raise KeyError("Only metadata related unique constraint are removable.")

        # self._engine._metadata.constraints.

        constraint_name = "_".join(names) + f"_unique"
        stmt = f"ALTER TABLE _{self.tree_name}_metadata DROP CONSTRAINT {constraint_name};"
        self._engine.execute_statement(stmt)

    def create_index(self, *names: str) -> None:
        index_name = "_".join(names) + f"_index"
        fields = [v for k,v in self._engine._metadata.columns.items() if k in names]
        create_index = CreateIndex(Index(index_name, *fields),if_not_exists= True)

        with self._engine.sessionmaker() as session:
            session.execute(create_index)
            session.commit()

    def remove_index(self, *names: str) -> None:
        index_name = "_".join(names) + f"_index"
        fields = [v for k,v in self._engine._metadata.columns.items() if k in names]
        drop_index = DropIndex(Index(index_name, *fields))

        with self._engine.sessionmaker() as session:
            session.execute(drop_index)
            session.commit()
