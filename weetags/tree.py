from __future__ import annotations

import copy
import json
from pathlib import Path
from hashlib import sha1
from collections import deque
from itertools import chain
from typing import Literal, Optional, Any

from weetags.engine.engine import TreeEngine
from weetags.utils import valid_creation, valid_update, valid_append, apply_handler, ErrorHandler

Nid = str
StrOrPath = str | Path
Conditions = list[list[tuple[str, str, Any] | str] | str] | None
Setter = list[tuple[str, Any]]
Fields = list[str] | None
Node = dict[str, Any] | None
Nodes = list[dict[str, Any]]
Style = Literal["ascii", "ascii-ex", "ascii-exr", "ascii-emh", "ascii-emv", "ascii-em"]
Relations = Literal["parent", "children", "siblings", "ancestors", "descendants"]



# @apply_handler(ErrorHandler)
class Tree(TreeEngine):
    """
    A Tree Reprensation based on the Sqlite Engine. Able to realise basic graph operations on trees.
    :attributes:
        :name: (str). name of the tree
        :tables: (dict[str, Table]). Sqlites tables schema storing the tree.
        :namespace: (dict[FieldName, Namespace]) namespace representation of the tree data.
        :root_id: (str) id of the root node.
        :tree_size: (int) number of nodes contained in the tree.
        :tree_depth: (int) maximum number of depth in the tree.
        :info: (dict[str, Any]) summary of tree data.
    :warnings:
        :efficiency: As SQlite is not a native Graphdb, Large operation recquiring to walk accross the whole tree tend to be inneficients.
        Large but relatively light trees can be better off Being cached rather than stored in a database.
    """

    def __init__(
        self,
        tree_name: str,
        database: Optional[str] = ":memory:",
        timeout: float = 5,
        **params: Any) -> None:
        super().__init__(tree_name, database, timeout, **params)
        self._build_tree_context(tree_name)
        self.name = tree_name
        self.remove_orphans = True
        self.root_id = None
        if self.tree_size > 0:
            self.root_id = self.root

    def __repr__(self) -> str:
        return f"<Tree name: {self.name}, size: {self.tree_size}, depth: {self.tree_depth}>"

    @property
    def tree_size(self) -> int:
        nodes = self.tables["nodes"]._name
        return self._table_size(nodes)

    @property
    def tree_depth(self) -> int:
        metadata = self.tables["metadata"]._name
        return self._max_depth(metadata)

    @property
    def root(self) -> dict[str, Any]:
        return self._read_one(fields=["id"], conditions=[[("depth", "=", 0)]])["id"]

    @property
    def info(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "uri": self.uri,
            "size": self.tree_size,
            "depth": self.tree_depth,
            "model": {f.fname:f.ftype for f in self.namespaces.values()}
        }

    def export(self, path: StrOrPath, fields: Optional[Fields] = None) -> None:
        filtered = ["depth", "is_root","is_leaf", "nid"]
        base = self.node(self.root_id, fields)
        if base is None:
            raise KeyError("Root Node Not found")
        if fields is not None and any([bool(f) for f in fields if f in filtered]):
            raise KeyError(f'metadata Fields cannot be exported `["depth", "is_root","is_leaf", "nid"]`')

        with open(path, "w+") as f:
            node = {k:v for k,v in base.items() if k not in filtered}
            f.write(f"{json.dumps(base)}\n")
            for node in self.descendants_nodes(self.root_id, fields):
                node = {k:v for k,v in node.items() if k not in filtered}
                f.write(f"{json.dumps(node)}\n")

    def node(self, nid: Nid, fields: Fields = None) -> Node:
        return self._read_one(fields=fields, conditions=[[("id", "=", nid)]])

    def nodes_where(
        self,
        conditions: Optional[Conditions] = None,
        fields: Optional[Fields] = None,
        order_by: Optional[Fields] = None,
        axis: Optional[int] = 1,
        limit: Optional[int | None] = None
        ) -> Nodes:
        return self._read_many(fields, conditions, order_by, limit, axis)

    def nodes_relation_where(
        self,
        relation: Relations,
        conditions: Optional[Conditions] = None,
        fields: Optional[Fields] = None,
        order: Optional[Fields] = None,
        axis: Optional[int] = 1,
        limit: Optional[int | None] = None,
        include_base: bool = False,
    ) -> Nodes:
        relations = {
            "parent": self.parent_node, 
            "children": self.children_nodes, 
            "siblings": self.siblings_nodes, 
            "ancestors": self.ancestors_nodes, 
            "descendants": self.descendants_nodes
        }
        callback = relations.get(relation)
        nodes = self._nodes_where(conditions, list(set(["id"] + fields)), order, axis, limit)
        res = {}
        for node in nodes:
            related = callback(node["id"], fields)
            if include_base:
                res.update({sha1(json.dumps(node).encode()).hexdigest(): node})
            if isinstance(related, list):
                res.update({sha1(json.dumps(n).encode()).hexdigest():n for n in related})
            else:
                res.update({sha1(json.dumps(related).encode()).hexdigest(): related})
        return list(res.values())
            
    def parent_node(self, nid: Nid, fields: Optional[Fields] = None) -> Node:
        node = self.node(nid, ["id","parent"])
        if node is None:
            return None
        return self.node(node["parent"], fields)

    def children_nodes(
        self, 
        nid: Nid, 
        fields: Optional[Fields] = None,
        order_by: Optional[Fields] = None,
        axis: Optional[int] = 1,
        limit: Optional[int | None] = None
    ) -> Nodes:
        node = self.node(nid, ["id","children"])
        if node is None:
            return []
        return self.nodes_where([[("id","IN", node["children"])]], fields, order_by, axis, limit)

    def siblings_nodes(
        self, 
        nid: Nid, 
        fields: Optional[Fields] = None,
        order_by: Optional[Fields] = None,
        axis: Optional[int] = 1,
        limit: Optional[int | None] = None
        ) -> Nodes:
        node = self.node(nid, ["id","parent"])
        if node is None:
            return []
        pnode = self.node(node["parent"], ["children"])
        if pnode is None:
            return []
        return self.nodes_where([[("id", "IN", pnode["children"]), ("id", "!=", nid)]], fields, order_by, axis, limit)


    def ancestors_nodes(
        self,
        nid: Nid, 
        fields: Optional[Fields] = None,
        axis: Optional[int] = 1,
        limit: Optional[int | None] = None 
    ) -> Nodes:
        remove_parent= False
        if fields is not None and "parent" not in fields:
            # force parent, as needed to go up ancestors
            remove_parent = True
            fields =  ["parent"] + fields


        node = self.node(nid, ["id","parent"])
        if node is None:
            return []

        ancestors = []
        while node["parent"]:
            node = self.node(node["parent"], fields)
            if remove_parent:
                payload = copy.deepcopy(node)
                payload.pop("parent", None)
                ancestors.append(payload)
            else:
                ancestors.append(node)

        if axis != 1 or limit is not None:
            ancestors = self._parse_selection(ancestors, None, axis, limit)
        return ancestors

    def descendants_nodes(
        self,
        nid: Nid,
        fields: Optional[Fields] = None,
        axis: Optional[int] = 1,
        limit: Optional[int | None] = None
    ) -> Nodes:
        node = self.node(nid, ["id","children"])
        if node is None:
            return []

        descendants, queue = [], deque(node["children"])
        while len(queue) > 0:
            cid = queue.pop()
            node = self.node(cid, fields)
            children = self.node(cid, ["children"])
            queue.extendleft(children["children"]) # weird to do that, have to do 2 I/O operations
            descendants.append(node)

        if axis != 1 or limit is not None:
            descendants = self._parse_selection(descendants, None, axis, limit)
        return descendants

    def orphans_nodes(
        self,
        fields: Optional[Fields] = None,
        order: Optional[Fields] = None,
        axis: Optional[int] = 1,
        limit: Optional[int | None] = None
        ) -> Nodes:
        orphans = self.nodes_where([[("parent","is", None)]], fields, order, axis, limit)
        for i in range(len(orphans)):
            if orphans[i]["id"] == self.root_id:
                orphans.pop(i)
                break
        return orphans

    def is_related(self, nid0: Nid, nid1: Nid, check_siblings: bool=False) -> bool:
        if nid0 == nid1:
            return True

        desc = [i["id"] for i in self.descendants_nodes(nid0, fields=["id"])]
        if nid1 in desc:
            return True

        ancs = [i["id"] for i in self.ancestors_nodes(nid0, fields=["id"])]
        if nid1 in ancs:
            return True

        if check_siblings:
            sibs = [i["id"] for i in self.siblings_nodes(nid0, fields=["id"])]
            if nid1 in sibs:
                return True
        return False

    def path(self, nid: Nid, to: Nid, fields: Optional[Fields] = None) -> Nodes:
        if fields is None:
            fields = []

        from_node = [self.node(nid, list(set(["id", "parent"] + fields)))]
        to_node = [self.node(to, list(set(["id", "parent"] + fields)))]
        meetup = False
        while meetup is False:
            if ((from_node[-1]["parent"] == to_node[-1]["id"]) or
                (to_node[-1]["parent"] == from_node[-1]["id"])):
                to_node.append(self.node(to_node[-1]["parent"], list(set(["id","parent"] + fields))))
                break

            if from_node[-1]["parent"] is not None:
                from_node.append(self.node(from_node[-1]["parent"], list(set(["id","parent"] + fields))))
            if to_node[-1]["parent"] is not None:
                to_node.append(self.node(to_node[-1]["parent"], list(set(["id","parent"] + fields))))

            if from_node[-1]["id"] == to_node[-1]["id"]:
                meetup = True
        return from_node[:-1] + to_node[::-1]

    @valid_creation
    def add_node(self, *, nid: Nid, parent: Nid | None, node_values: dict[str, Any] | None = None) -> None:
        node = {"id": nid, "parent": parent}
        if node_values is not None:
            node.update(node_values)
        pid = node.get("parent", None)
        if pid is None and self.root_id is not None:
            raise ValueError("tree can only have one root")
        elif pid is None:
            self._add_node(node, 0, True, True)
        else:
            pnode = self._add_children(pid, node["id"])
            self._add_node(node, pnode["depth"] + 1)

    @valid_update
    def update_node(self, *, nid: Nid, set_values: Setter) -> None:
        self._update("nodes", set_values, [[("id", "=", nid)]])

    @valid_update
    def update_nodes_where(self, *, conditions: Conditions, set_values: Setter) -> None:
        nids = [n["id"] for n in self.nodes_where(conditions, ["id"])]
        self._update("nodes", set_values, [[("id", "IN", nids)]])

    @valid_append
    def append_node(self, *, nid: Nid, field_name: str, value: Any) -> None:
        base_value = self.node(nid, fields=[field_name]).get(field_name)
        self._update("nodes", [(field_name, base_value + [value])], [[("id","=",nid)]])

    @valid_append
    def extend_node(self, *, nid: Nid, field_name: str, values: list[Any]) -> None:
        base_value = self.node(nid, fields=[field_name]).get(field_name)
        self._update("nodes", [(field_name, base_value + values)], [[("id","=",nid)]])

    def delete_node(self, nid: Nid) -> None:
        self._delete_node(nid)
        if self.remove_orphans:
            self.delete_dead_branches()

    def delete_nodes_where(self, conditions: Optional[Conditions] = None) -> None:
        nodes = self.nodes_where(conditions, ["id"])
        for n in nodes:
            nid = n["id"]
            if nid == self.root_id:
                raise ValueError("cannot delete root node")
            self._delete_node(nid)
        if self.remove_orphans:
            self.delete_dead_branches()

    def delete_dead_branches(self) -> None:
        orphans = self.orphans_nodes(["id"])
        nodes = chain.from_iterable([[o] + self.descendants_nodes(o["id"], ["id"]) for o in orphans])
        [self._delete([[("id","=", node["id"])]]) for node in nodes]

    def delete_orphans(self):
        orphans = self.orphans_nodes(["id"])
        [self._delete([[("id","=", o["id"])]]) for o in orphans]

    def draw_tree(
        self,
        nid: Optional[Nid | None]  = None, 
        style: Optional[Style] = "ascii-ex", 
        extra_space: bool = False
    ) -> str:
        dt = {
            "ascii": ("|", "|-- ", "+-- "),
            "ascii-ex": ("\u2502", "\u251c\u2500\u2500 ", "\u2514\u2500\u2500 "),
            "ascii-exr": ("\u2502", "\u251c\u2500\u2500 ", "\u2570\u2500\u2500 "),
            "ascii-em": ("\u2551", "\u2560\u2550\u2550 ", "\u255a\u2550\u2550 "),
            "ascii-emv": ("\u2551", "\u255f\u2500\u2500 ", "\u2559\u2500\u2500 "),
            "ascii-emh": ("\u2502", "\u255e\u2550\u2550 ", "\u2558\u2550\u2550 "),
        }[style]

        if nid is None:
            nid = self.root_id

        root = self.node(nid, ["id", "parent", "children", "depth", "is_leaf"])
        if root["is_leaf"]:
            tree = f"{dt[2]}{root['id']}"
            return tree

        tree = f"{root['id']}\n"

        INITIAL_DEPTH = root["depth"]
        MAX_DEPTH = self.tree_depth
        BLOCK_SIZE = 2
        INDENTATION = 2
        LINED_SPACE = dt[0] + (" " * BLOCK_SIZE)
        EMPTY_SPACE = " " * (BLOCK_SIZE + 1)
        layer_state = [False] * (MAX_DEPTH - INITIAL_DEPTH)
        layer_state[0] =  bool(len(root["children"]))

        def _spacing(layer:int, layer_state: list[bool]):
            base_indentation = " " * INDENTATION
            layers = "".join([LINED_SPACE if v else EMPTY_SPACE for v in layer_state[:layer]])
            return base_indentation + layers

        def _draw(tree: str, queue: deque, layer_state: list[bool]):
            seen = set()
            while len(queue) > 0:
                nid = queue.popleft()
                node = self.node(nid, ["id", "parent", "children", "depth", "is_leaf"])
                layer = node["depth"] - INITIAL_DEPTH - 1

                if nid not in seen and len(node["children"]) == 0:
                    seen.add(nid)
                    queue.append(nid)
                    continue

                if len(queue) > 0:
                    space = _spacing(layer, layer_state)
                    tree += f"{space}{dt[1]}{node['id']}\n"

                else:
                    space = _spacing(layer, layer_state)
                    tree += f"{space}{dt[2]}{node['id']}\n"

                layer_state[(node["depth"] - INITIAL_DEPTH - 1)] = bool(len(queue))

                if extra_space and len(queue) == 0 and any(layer_state[:layer]) and len(node["children"])== 0:
                    space = _spacing(layer, layer_state)
                    tree += f"{space}\n"

                tree = _draw(tree, deque(node["children"]), layer_state)
            return tree
        return _draw(tree, deque(root["children"]), layer_state)

    def show_tree(
        self, 
        nid: Optional[Nid | None] = None, 
        style: Optional[Style] = "ascii-ex", 
        extra_space: bool=False
    ) -> None:
        tree = self.draw_tree(nid, style, extra_space)
        print(tree)

    def _add_node(self, node: Node, depth: int=0, is_root: bool= False, is_leaf: bool = True) -> None:
        self._write_one("nodes", list(node.keys()), list(node.values()), "none")
        self._write_one("metadata", ["nid", "depth", "is_root", "is_leaf"], [node["id"], depth, is_root, is_leaf], "none")

    def _add_children(self, nid: Nid, cnid: Nid) -> Node:
        pnode = self.node(nid, ["id", "depth", "children"])
        assert(pnode is not None)

        pnode.update({"children": list((set(pnode["children"] + [cnid])))})
        self._update("nodes", [("children", pnode["children"])], [[("id", "=", nid)]])
        self._update("metadata", [("is_leaf", False)], [[("nid","=", pnode["id"])]])
        return pnode

    def _delete_node(self, nid: Nid) -> None:
        if nid == self.root_id:
            raise ValueError("cannot delete root node")

        node = self.node(nid, ["children","parent"])
        [self._remove_parent(c) for c in node["children"]]
        self._remove_children(node["parent"], nid)
        self._delete([[("id", "=", nid)]])

    def _remove_children(self, nid: Nid, cnid: Nid):
        node = self.node(nid, ["id", "children"])
        if cnid in node["children"]:
            node["children"].remove(cnid)
        self._update("nodes", [("children", node["children"])], [[("id", "=", node["id"])]])

    def _remove_parent(self, nid: Nid):
        node = self.node(nid, ["id"])
        self._update("nodes", [("parent", None)], [[("id", "=", node["id"])]])

    def _parse_selection(
        self, 
        nodes: Nodes,
        order_by: Fields,
        axis: int,
        limit: int | None
    ) -> list[dict[str,Any]]:
        if order_by:
            nodes = sorted(nodes, key= lambda x: [x[f] for f in order_by])
        if axis == 1 and limit:
            nodes = nodes[:limit]
        elif axis == 0 and limit:
            nodes = nodes[::-1][:limit]
        elif axis == 0 and limit is None:
            nodes = nodes[::-1]
        return nodes