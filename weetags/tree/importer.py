from __future__ import annotations

from pathlib import Path
from collections import deque, defaultdict


from typing import Any, Generator, Type

from weetags.common import Engine, BoundEngine
from weetags.common.types import OnCollision, BatchT
from weetags.common.base import TreeTopologyDefinition, TreeMetadataDefinition
from weetags.common.loaders import JLLoader, Loader, DictLoader
from weetags.tree.validation import FileCheck, PayloadCheck



class TypedBatch(list):
    btype: BatchT

    def __init__(self, iterable, btype: BatchT):
        super().__init__(iterable)
        self.btype = btype


class DataLoader:
    loader: Loader | DictLoader
    keymap: dict[str, Any] | None
    args: list[Any]

    def __init__(self, loader: Type[Loader], args: list[Any], keymap: dict[str, Any] | None = None):
        self.loader = loader()
        self.keymap = keymap
        self.args = args

    def load(self) -> Generator[dict[str, Any]]:
        for data in self.loader.lazy_loader(*self.args):
            yield self.translate_keys(data)

    def translate_keys(self, data: dict[str, Any]) -> dict[str, Any]:
        if self.keymap is None:
            return data
        
        for k,v in self.keymap.items():
            if k in data.keys():
                data[v] = data.pop(k)
        return data


class Importer:
    """
    TODO: verify that the data is not already imported. abort importation if necessary
    """
    engine: BoundEngine

    batch_size: int = 100

    # global states
    _root: str | None = None
    _existing_nodes: list[str]
    _parent2children: dict[str, list[str]]
    _child2parent: dict[str, str | None]
    _name2path: dict[str, str]
    
    @property
    def _has_mapped_relation(self) -> bool:
        return (
            getattr(self, "_parent2children", None) is not None
            and getattr(self, "_child2parent", None) is not None
        )
    
    def __init__(self, tree_name: str, engine: Engine, batch_size: int = 100) -> None:
        self.tree_name = tree_name

        self.engine = engine.bind(tree_name)
        self.engine.reflect()
        # self.tree_topology, self.tree_metadata, _ = self.engine.get_tree(self.tree_name)

        # configuration
        self.batch_size = batch_size
        
    def load(
        self, path: str | Path, 
        keymap: dict[str, str] | None = None, 
        on_collision: OnCollision | str = "raise"
    ) -> None:
        self.loader = DataLoader(JLLoader, args=[path], keymap=keymap)

        validator = FileCheck(self.tree_name, self.engine)
        validator.test(path, keymap, on_collision)

        self._map_nodes_relation()
        self._generate_nodes_path()
        self._node_indexer(on_collision)


    def loads(
        self, 
        data: list[dict[str, Any]], 
        keymap: dict[str, str] | None = None, 
        on_collision: OnCollision | str = "raise"
    ) -> None:
        self.loader = DataLoader(DictLoader, args=[data], keymap=keymap)

        validator = PayloadCheck(self.tree_name, self.engine)
        validator.test(data, keymap, on_collision)
        
        self._map_nodes_relation()
        self._generate_nodes_path()
        self._node_indexer(on_collision)

    def _map_nodes_relation(self) -> None:
        self._map_existing_relations()

        for payload in self.loader.load():
            node_name = payload["name"]
            parent_name = payload.get("parent", None)

            self._child2parent[node_name] = parent_name
            
            if parent_name is None:
                self._root = node_name
            else:
                self._parent2children[parent_name].append(node_name) 

    def _generate_nodes_path(self) -> None:
        if self._root is None:
            raise ValueError("No root nodes found.")
        
        if self._has_mapped_relation is False:
            raise KeyError("Nodes relations has not been mapped yet.")

        self._name2path = {}

        self._name2path[self._root] = self._root
        queue = deque(self._parent2children[self._root])

        while len(queue) > 0:
            node = queue.popleft()
            parent = self._child2parent[node]
            if parent is None:
                self._name2path[node] = f"{node}"
            else:
                parent_path = self._name2path[parent]
                self._name2path[node] = f"{parent_path}.{node}"

            children = self._parent2children.get(node, None)
            if children is not None:
                queue.extend(children)

    def _node_indexer(self, on_collision: OnCollision | str = "raise") -> None:
        for batch in self._batch_loader(on_collision):
            if batch.btype == "insert":
                topologies = [self._build_topology_payload(payload) for payload in batch]
                nids = self.engine.write_multi_topology(topologies)

                metadatas = [self._build_metadata_payload(payload, nid) for payload, nid in zip(batch, nids)]
                self.engine.write_multi_metadata(metadatas)
            elif batch.btype == "parent_update":
                """update topology of the parent node by inserting new child name."""
                for parent, child in batch:
                    self.engine._add_child(parent, child)
            elif batch.btype == "update":
                """
                Update underlying meaning is about metadata only, never topology...
                """
                # metadatas = [self._build_metadata_payload(payload, nid) for payload, nid in zip(batch, nids)]
                raise NotImplementedError()
        
            elif batch.btype == "replace":
                """
                replace as larger impact to analyze... 
                affecting topology can break exisiting structure if not carefull on the data imported
                """
                raise NotImplementedError()

    def _batch_loader(self, on_collision: OnCollision | str = "raise") -> Generator[TypedBatch]:
        batch_insert = TypedBatch([], "insert")
        batch_update = TypedBatch([], "update")
        batch_parent_update = TypedBatch([], "parent_update")
        batch_replace = TypedBatch([], "replace")
        for node in self.loader.load():
            name = node.get("name")
            parent = node.get("parent", None)

            if name in self._existing_nodes and on_collision == "raise":
                raise ValueError(f"Node collision: {name}")
            
            elif name in self._existing_nodes and on_collision == "ignore":
                continue
            
            elif name in self._existing_nodes and on_collision == "replace":
                batch_replace.append(node)

            elif name in self._existing_nodes and on_collision == "update":
                batch_update.append(node)
            
            else:
                batch_insert.append(node)
                if parent in self._existing_nodes:
                    batch_parent_update.append((parent, name))

            for batch in [batch_insert, batch_parent_update, batch_update, batch_replace]:
                if len(batch) == self.batch_size:
                    yield batch
                    batch.clear()
        for batch in [batch_insert, batch_parent_update, batch_update, batch_replace]:
            if len(batch) > 0:
                yield batch

    def _build_topology_payload(self, data: dict[str, Any]) -> dict[str, Any]:
        node_name = data["name"]
        payload = {
            "children": self._parent2children.get(node_name, []),
            "path": self._name2path.get(node_name),
            "level": len(self._name2path[node_name].split(".")) - 1
        } 
        [payload.update({f.name: data.get(f.name, None)}) for f in self.engine._topology.columns.values() if f.name not in TreeTopologyDefinition().generated_keys]
        return payload

    def _build_metadata_payload(self, data: dict[str, Any], nid: int) -> dict[str, Any]:
        payload = {"id": nid}
        [payload.update({f.name: data.get(f.name, None)}) for f in self.engine._metadata.columns.values() if f.name not in TreeMetadataDefinition.fk]
        return payload

    def _map_existing_relations(self) -> None:
        self._existing_nodes = []
        self._root = None
        self._parent2children = defaultdict(list)
        self._child2parent = {}

        for node in self.engine.non_ordered_walk():
            node_name = node["name"]
            parent_name = node.get("parent", None)

            self._existing_nodes.append(node_name)
            self._child2parent[node_name] = parent_name
            if parent_name is None:
                self._root = node_name
            else:
                self._parent2children[parent_name].append(node_name) 


