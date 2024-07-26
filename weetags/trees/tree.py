from __future__ import annotations

from collections import deque, defaultdict, OrderedDict
from itertools import chain
from typing import Literal, TypeVar, Optional, Any, Callable

from weetags.database.namespace import NameSpace
from weetags.database.db import _Db


Payload = dict[str, Any]
Style = Literal["ascii", "ascii-ex", "ascii-exr", "ascii-emh", "ascii-emv", "ascii-em"]
Path = TreeName = TableName = FieldName = Operator = str
_Nid = TypeVar("_Nid", str, int)
_SqliteTypes = TypeVar("_SqliteTypes", str, int, dict, list, bytes)






class Tree(_Db):
    """
    
    
    :attributes:
        :name:
        :tables:
        :namespace:
        :rootid:
    """
    
    def __init__(
        self,
        name: TreeName,
        path: str | None = "db.db",
        permanent: bool = True,
        **params) -> None:
        print(f"___________________ {name}")
        
        super().__init__(path, permanent, name, **params)
        print(self.tables)
        self.name = name
        self.remove_orphans = True
        self.root_id = None
        if self.tree_size() > 0:
            self.root_id = self.root()

    def tree_size(self) -> int:
        nodes = self.tables["nodes"].table_name
        return self.table_size(nodes)
    
    def tree_depth(self) -> int:
        metadata = self.tables["metadata"].table_name
        return self.max_depth(metadata)
    
    def export(self, format: str, fields: list[FieldName]):
        pass
    
    def info(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "permanent": self.permanent,
            "size": self.tree_size(),
            "depth": self.tree_depth(),
            "uri": self.uri,
            "model": {f.fname:f.ftype for f in self.namespace.values()}
        }
    
    def root(self) -> Any:
        return self.read_one(fields=["id"], conds=[("depth", "=", 0)])["id"]
    
    def node(self, nid: _Nid, fields: list[FieldName] = ["*"]) -> Payload:
        return self.read_one(fields=fields, conds=[("id", "=", nid)])
    
    def nodes_where(
        self,
        conds: list[tuple[FieldName,str, Any]] | None = None, 
        fields: list[FieldName] = ["*"],
        order: Optional[list[FieldName]] | None = None,
        axis: int = 1,
        limit: int | None = None
        ) -> Payload:
        return self.read_many(fields, conds, order, axis, limit)
    
    def nodes_relation_where(
        self,
        relation: Callable,
        conds: list[tuple[FieldName,str, Any]] | None = None, 
        fields: list[FieldName] = ["*"],
        order: Optional[list[FieldName]] | None = None,
        axis: int = 1,
        limit: int | None = None,
        include_base: bool = False
        ) -> list[Payload]:
        nodes = self.nodes_where(conds, list(set(["id"] + fields)), order, axis, limit)
        if include_base:
            return [[n] + relation(n["id"], fields) for n in nodes]
        else:
            return [relation(n["id"], fields) for n in nodes]
        
    def parent_node(self, nid: _Nid, fields: list[FieldName] = ["*"]) -> Payload:
        node = self.node(nid, ["id","parent"])
        return self.node(node["parent"], fields)
    
    def children_nodes(self, nid: _Nid, fields: list[FieldName] = ["*"]) -> list[Payload]:
        node = self.node(nid, ["id","children"])        
        return [self.node(c, fields) for c in node["children"]]
    
    def siblings_nodes(self, nid: _Nid, fields: list[FieldName] = ["*"]) -> list[Payload]:
        node = self.node(nid, ["id","parent"])        
        pnode = self.node(node["parent"], ["children"])
        return [self.node(c, fields) for c in pnode["children"] if c != nid]
        
    def ancestors_nodes(self, nid: _Nid, fields: list[FieldName] = ["*"]) -> list[Payload]:
        ancestors = []
        node = self.node(nid, ["id","parent"])
        while node["parent"]:
            node = self.node(node["parent"], fields)
            ancestors.append(node)
        return ancestors
            
    def descendants_nodes(self, nid: _Nid, fields: list[FieldName] = ["*"]) -> list[Payload]:
        node = self.node(nid, ["id","children"])
        descendants, queue = [], deque(node["children"])
        while len(queue) > 0:
            cid = queue.pop()
            node = self.node(cid, fields)
            children = self.node(cid, ["children"])
            queue.extendleft(children["children"]) # weird to do that, have to do 2 I/O operations
            descendants.append(node)
        return descendants
            
    def orphans_nodes(
        self, 
        fields: list[FieldName] = ["*"], 
        order: Optional[list[FieldName]] | None = None,
        axis: int = 1,
        limit: Optional[int] = None
        ) -> list[Payload]:
        orphans = self.nodes_where([("parent","is", None)], fields, order, axis, limit)
        for i in range(len(orphans)):
            if orphans[i]["id"] == self.root_id:
                orphans.pop(i)
                break
        return orphans
    
    def path(self, node:_Nid, to:_Nid, fields: list[FieldName] = ["*"]) -> list[Payload]:
        from_node = [self.node(node, list(set(["id", "parent"] + fields)))]
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
                
             
    
    
    def add_node(self, node: Payload) -> None:
        node = self.tables["nodes"].validate_node(node)
        pid = node.get("parent", None)
        if pid is None and self.root_id is not None:
            raise ValueError("tree can only have one root")
        elif pid is None:
            self._add_node(node, 0, True, True)
        else:
            pnode = self._add_children(pid, node["id"])
            self._add_node(node, pnode["depth"] + 1)

    def update_node(self, nid: _Nid, set_values:list[tuple[FieldName, _SqliteTypes]]) -> None:
        nodes_table = self.tables["nodes"].table_name
        meta_table = self.tables["metadata"].table_name
        fid_map = {nodes_table: "id", meta_table: "nid"}
        
        queries = defaultdict(list)
        for fname, v in set_values:
            tn = self.namespace[fname].table
            queries[tn].append(tuple((fname, v)))
        
        for table_name, set_values in queries.items():
            self.update(set_values, [(fid_map[table_name], "=", nid)])

    def delete_node(self, nid: _Nid) -> None:
        self._delete_node(nid)
        if self.remove_orphans:
            self.delete_dead_branches()
        
    def delete_nodes_where(self, conds: list[tuple[FieldName, str, Any]]) -> None:
        nodes = self.nodes_where(conds, ["id"])
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
        [self.delete([("id","=", node["id"])]) for node in nodes]

    def delete_orphans(self):
        orphans = self.orphans_nodes(["id"])
        [self.delete([("id","=", o["id"])]) for o in orphans]




    def draw_tree(self, nid: Optional[_Nid] | None = None, style:Style="ascii-ex", extra_space: bool=False) -> str:
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
        MAX_DEPTH = self.tree_depth()
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
        
    def show_tree(self, nid: Optional[_Nid] | None= None, style:Style="ascii-ex", extra_space: bool=False) -> None:
        tree = self.draw_tree(nid, style, extra_space)
        print(tree)


    def draw_path(self, node: _Nid, to: _Nid):
        path = self.path(node, to, fields=["id","parent","depth"])
        layers = [[] for _ in range(self.tree_depth() + 1)]
        [layers[node["depth"]].append(node["id"]) for node in path]
        top_depth = layers[0][0]
        
        tree = ""
        for i in range(len(layers)):
            layer = layers[i]
            # top layer
            if i == 0:
                tree += " --- ".join(layer)
            
            # lower lawers
            else:
                pass
                
        # print(' /\n/','\n', '\\\n  \\')
        # for node in path:
        #     print(node["id"], node["depth"])
            
    def show_path(self):
        pass


    def _add_node(self, node: Payload, depth: int=0, is_root: bool= False, is_leaf: bool = True) -> None:
        nodes_table = self.tables["nodes"].table_name
        meta_table = self.tables["metadata"].table_name
        self.write(nodes_table, list(node.keys()), list(node.values()))   
        self.write(meta_table, ["nid", "depth", "is_root", "is_leaf"], [node["id"], depth, is_root, is_leaf])
        
    def _add_children(self, nid: _Nid, cnid: _Nid) -> Payload:
        pnode = self.node(nid, ["id", "depth", "children"])
    
        assert(pnode is not None)
        
        pnode.update({"children": list((set(pnode["children"] + [cnid])))})
        self.update([("children", pnode["children"])], [("id", "=", nid)])
        self.update([("is_leaf", False)], [("nid","=", pnode["id"])])
        return pnode
    
    def _delete_node(self, nid: _Nid) -> None:
        if nid == self.root_id:
            raise ValueError("cannot delete root node")
        
        node = self.node(nid, ["children","parent"])
        [self._remove_parent(c) for c in node["children"]]
        self._remove_children(node["parent"], nid)
        self.delete([("id", "=", nid)])

    def _remove_children(self, nid: _Nid, cnid):
        node = self.node(nid, ["id", "children"])
        node["children"].remove(cnid)
        self.update([("children", node["children"])], [("id", "=", node["id"])]) 
        
    def _remove_parent(self, nid: _Nid):
        node = self.node(nid, ["id"])
        self.update([("parent", None)], [("id", "=", node["id"])]) 




if __name__ == "__main__":
    from pprint import pprint
    tree = Tree("topics", "./volume/db.db")
    # tree.show_tree()
    tree.draw_path("Doctor", "Human trafficking")
