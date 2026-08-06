from __future__ import annotations

import json
import warnings
from pathlib import Path
from hashlib import sha256
from abc import ABC, abstractmethod
from collections import defaultdict
from sqlalchemy import Index, Constraint, UniqueConstraint

from typing import Any

from weetags.common import Engine
from weetags.common.types import OnCollision
from weetags.common.base import TreeTopologyDefinition, TreeMetadataDefinition
from weetags.common.loaders import JLLoader
from weetags.common.utils import translate_sqlalchemy_sqltype, translate_keys, Json


class Validator(ABC):
    def __init__(self, tree_name: str, engine: Engine):
        self.tree_name = tree_name
        self._engine = engine
        self._engine.get_tree(tree_name)

    @abstractmethod
    def test(self) -> bool:
        raise NotImplementedError()




class TreeIntegrityCheck(Validator):
    """
    validate parent / children relations + number of roots.
    """

    def __init__(self, tree_name: str, engine: Engine):
        super().__init__(tree_name, engine)

    def test(self) -> bool:
        n_root = 0
        existing = []
        await_parent_validation = []
        await_child_validation = []
        for node in self._engine.non_ordered_walk():
            node_name = node["name"]
            parent = node["parent"]
            children = node.get("children", [])

            existing.append(node_name)
            if parent is None:
                n_root += 1
            elif parent not in existing:
                await_parent_validation.append(parent)

            for child in children:
                if child not in existing:
                    await_child_validation.append(child)

        missing_parent, missing_child = [], []
        for parent in await_parent_validation:
            if parent not in existing:
                missing_parent.append(parent)
        for child in await_child_validation:
            if child not in existing:
                missing_child.append(child)

        if n_root > 1:
            raise ValueError(f"Too many root nodes: {n_root}")
        if len(missing_child) > 0:
            raise ValueError(f"Nodes referencing non existing children: {missing_child}")
        if len(missing_parent) > 0:
            raise ValueError(f"Nodes referencing non existing parent: {missing_parent}")
        return True

    
class JsonKeyConsistencyCheck(Validator):
    def __init__(self, tree_name: str, engine: Engine):
        super().__init__(tree_name, engine)

    def test(self) -> bool:
        json_fields = []

        metadata = self._engine._metadata
        return True

        

class DataValidator:
    """
    TODO: Make sure constraint check works with indexes
    """
    def __init__(self, tree_name: str, engine: Engine) -> None:
        self.tree_name = tree_name
        self.engine = engine

        self.tree_topology, self.tree_metadata,_ = self.engine.get_tree(tree_name)
        if self.tree_topology is None or self.tree_metadata is None:
            raise KeyError("Cannot validate Data file against tree structure that is not constructed yet.")

        self._existing_namespace = []
        self._uniq_constraint = []
        self._uniq_contraint_namespaces = defaultdict(list)

        self._constraints()
        self._existing()

        self._namespace = self._existing_namespace
        self._await_parent_check = []
        self.n_root_keys = 0
        self.valid_json_types = defaultdict(set)
        self.valid_uniq = defaultdict(list)

    def _test_node(
        self, 
        data: dict[str, Any], 
        fields: list[tuple], 
        keymap: dict[str, Any] | None = None,
        on_collision: OnCollision | str = "raise"
    ) -> bool | Exception:
        uniq_contraints = defaultdict(list)
        
        data = translate_keys(data, keymap)
        for name, dtype, nullable in fields:
            has_field = name in data.keys()

            # check if non nullable is null.
            if has_field is False and nullable is False:
                return KeyError(f"Missing key: {name}. This key is not nullable")
            value = data.get(name)
            if value is None and nullable is False:
                return ValueError(f"Key {name} is non nullable")

            # check engine type == value type
            valid_type = (
                type(value) == dtype 
                or type(value) in [dict, list] and dtype == Json
                or name == "parent" and (type(value) == str or value is None)
                or nullable is True and value is None
            )
            if valid_type is False:
                return TypeError(f"Key {name} as an invalid type. expected type: {dtype}")


            # no `.` in name value
            if name == "name" and isinstance(value, str) and value.__contains__("."):
                raise ValueError("Name values cannot contain `.` .")

            # check name key collision > behavior depend on on_collision argument
            if name == "name" and value in self._existing_namespace and on_collision == "raise":
                raise KeyError(f"Node name already existing: {value}")
            if name == "name" and value in self._existing_namespace and on_collision != "raise":
                warnings.warn(f"Node collision: {value}. collision settings set on: {on_collision}")

            # check if name already in the name space
            if name == "name" and value in self._namespace:
                raise KeyError(f"Node name collision. Node Name is supposed to bbe unique accross a tree: {value}")
            # else register the names to catch later unicity constraint violation.
            elif name == "name" and value not in self._namespace:
                self._namespace.append(value)

            # check parent key or await until it pops out ( remaining keys at the end of process have missing parent)
            if name == "parent" and value is not None and value not in self._namespace:
                self._await_parent_check.append(value)
            elif name == "name" and value in self._await_parent_check:
                index = self._await_parent_check.index(value)
                self._await_parent_check.pop(index)

            # change global status that monitor cross node pattern.
            if name == "parent" and value is None:
                self.n_root_keys += 1
            if dtype == Json:
                self.valid_json_types[name].add(type(value))

            # check other uniq contraints & indexes
            for constraint in self._uniq_constraint:
                if name in constraint:
                    uniq_contraints[constraint].append(value)

        # once all uniq constraint values are gathered, check against existing namespaces.
        for constraint in self._uniq_constraint:
            digest = sha256(json.dumps(uniq_contraints[constraint]).encode()).hexdigest()
            if digest in self._uniq_contraint_namespaces[constraint]:
                raise KeyError(f"Unique constraint not respected for: {constraint}")
            else:
                self._uniq_contraint_namespaces[constraint].append(digest)

        return True

    def _test_dataset_consistency(self) -> bool | Exception:
        if len(self._await_parent_check) > 0:
            raise KeyError(f"Certain node have non existing parents: {self._await_parent_check}")
        if self.n_root_keys > 1:
            return ValueError("More than one Root node (nodes without parents.)")
        for k,v in self.valid_json_types.items():
            if len(v) > 1:
                return TypeError(f"Key {k} type is inconsistant accross all nodes data")
        for k,v in self.valid_uniq.items():
            if len(v) > len(set(v)):
                raise ValueError(f"Key {k} has a non respected unique constraint.")
        return True

    def _fields(self) -> list[tuple[str, Any, bool | None]]:
        topology_fields =  [
            (c.name, translate_sqlalchemy_sqltype(c.type), c.nullable) 
            for c in self.tree_topology.columns 
            if c.name in TreeTopologyDefinition().base_keys
        ]

        meta_fields =  [
            (c.name, translate_sqlalchemy_sqltype(c.type), c.nullable) 
            for c in self.tree_metadata.columns 
            if c.name not in TreeMetadataDefinition.fk
        ]
        return topology_fields + meta_fields

    def _constraints(self) -> None:
        """only check uniq and indexes"""

        for constraint in self.tree_metadata.indexes.union(self.tree_metadata.constraints):
            if isinstance(constraint, Index) or isinstance(constraint, UniqueConstraint):
                c = tuple([c.name for c in constraint._columns])
            else:
                continue
            if c not in self._uniq_constraint and "id" not in c:
                self._uniq_constraint.append(c)


    def _existing(self) -> None:
        fields = self._fields()
        for node in self.engine.non_ordered_walk():
            # set existing namespace for name collision check
            self._existing_namespace.append(node["name"])

            # set existing constraint digest for contraint collision
            for constraint in self._uniq_constraint:
                values = []
                constraint_fields = [f for f in fields if f[0] in constraint]

                
                for f in constraint_fields:
                    v = node.get(f, None) # pyright: ignore
                    values.append(v)
                digest = sha256(json.dumps(values).encode()).hexdigest()
                self._uniq_contraint_namespaces[constraint].append(digest)





class FileCheck(DataValidator):
    def __init__(self, tree_name: str, engine: Engine) -> None:
        super().__init__(tree_name, engine)

    def test(self, path: str | Path, keymap: dict[str, str] | None = None, on_collision: OnCollision | str = "raise") -> bool:
        fields = self._fields()
        n = 1
        for payload in JLLoader().lazy_loader(path):
            ok = self._test_node(payload, fields, keymap, on_collision)
            if isinstance(ok, Exception):
                raise ok.__class__(f"node {n}: ", *ok.args)
            n += 1   
        ok = self._test_dataset_consistency()
        if isinstance(ok, Exception):
            raise ok
        return True

class PayloadCheck(DataValidator):
    def __init__(self, tree_name: str, engine: Engine) -> None:
        super().__init__(tree_name, engine)

    def test(self, data: list[dict[str, Any]], keymap: dict[str, str] | None = None, on_collision: OnCollision | str = "raise") -> bool:
        fields = self._fields()
        n = 1
        for payload in data:
            ok = self._test_node(payload, fields, keymap, on_collision)
            if isinstance(ok, Exception):
                raise ok.__class__(f"node {n}: ", *ok.args)
            n += 1   
        ok = self._test_dataset_consistency()
        if isinstance(ok, Exception):
            raise ok
        return True