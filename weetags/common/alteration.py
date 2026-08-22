from __future__ import annotations


from typing import Any

from sqlalchemy import Index
from sqlalchemy.schema import CreateIndex, DropIndex

from weetags.common.base import field_is_reserved
from weetags.common.engine import BoundEngine
from weetags.common.types import AcceptedFieldType
from weetags.common.configs import FieldType
import weetags.common.utils as cutils

class Alteration:
    _engine: BoundEngine

    def __init__(self, engine: BoundEngine) -> None:
        self._engine = engine

    def add_field(
        self,
        name: str, 
        dtype: AcceptedFieldType, 
        nullable: bool = True, 
        unique: bool = False,
        default: Any = None
    ) -> None:
        field_is_reserved(name)
        cutils.field_exist(name, self._engine._metadata)
        cutils.field_non_nullable(nullable, default)

        field_type = FieldType.from_value(dtype)
        opts = " ".join([o[0] for o in [("NULLABLE", nullable), ("UNIQUE",unique), (f"DEFAULT {default}", default)] if o[1]])
        stmt = f"ALTER TABLE _{self._engine.name}_metadata ADD COLUMN {name} {field_type.name} {opts};"
        self._engine.execute_statement(stmt)

    def remove_field(self, name: str) -> None:
        field_is_reserved(name)
        cutils.field_not_exist(name, self._engine._metadata)

        stmt = f"ALTER TABLE _{self._engine.name}_metadata DROP COLUMN {name};"
        self._engine.execute_statement(stmt)

    def rename_field(self, name: str, new_name: str) -> None:
        field_is_reserved(name)
        field_is_reserved(new_name)
        cutils.field_not_exist(new_name, self._engine._metadata)
        
        stmt = f"ALTER TABLE _{self._engine.name}_metadata RENAME {name} TO {new_name};"
        self._engine.execute_statement(stmt)

    def create_index(self, *names: str) -> None:
        index_name = "_".join(names) + f"_index"
        fields = [v for k,v in self._engine._metadata.columns.items() if k in names]
        create_index = CreateIndex(Index(index_name, *fields), if_not_exists= True)
        self._engine.execute(create_index, commit=True)

    def remove_index(self, *names: str) -> None:
        index_name = "_".join(names) + f"_index"
        fields = [v for k,v in self._engine._metadata.columns.items() if k in names]
        drop_index = DropIndex(Index(index_name, *fields))
        self._engine.execute(drop_index, commit=True)

    def add_field_nullable(self, name: str) -> None:
        cutils.psql_required(self._engine.uri.dialect)
        cutils.field_not_exist(name, self._engine._metadata)

        if self._engine._metadata.columns[name].nullable is False:        
            stmt = f"ALTER TABLE _{self._engine.name}_metadata ALTER COLUMN {name} DROP NOT NULL;"
            self._engine.execute_statement(stmt)

    def remove_field_nullable(self, name: str) -> None:
        cutils.psql_required(self._engine.uri.dialect)
        cutils.field_not_exist(name, self._engine._metadata)
        if self._engine._metadata.columns[name].nullable:
            stmt = f"ALTER TABLE _{self._engine.name}_metadata ALTER COLUMN {name} SET NOT NULL;"
            self._engine.execute_statement(stmt)

    def add_field_unique_constraint(self, *names: str) -> None:
        cutils.psql_required(self._engine.uri.dialect)
        [cutils.field_not_exist(name, self._engine._metadata) for name in names]

        constraint_name = "_".join(names) + f"_unique"
        fields = ", ".join(names)
        stmt = f"ALTER TABLE _{self._engine.name}_metadata ADD CONSTRAINT {constraint_name} UNIQUE ({fields});"
        self._engine.execute_statement(stmt)

    def remove_field_unique_constraint(self, *names: str) -> None:
        cutils.psql_required(self._engine.uri.dialect)
        [cutils.field_not_exist(name, self._engine._metadata) for name in names]

        constraint_name = "_".join(names) + f"_unique"
        stmt = f"ALTER TABLE _{self._engine.name}_metadata DROP CONSTRAINT {constraint_name};"
        self._engine.execute_statement(stmt)













