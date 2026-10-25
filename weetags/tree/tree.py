from __future__ import annotations

from pathlib import Path
from sqlalchemy import ColumnElement
from typing import Any, Generator, Type

from weetags.common import Engine, EngineURI
from weetags.common.loaders import Loader, YamlLoader
from weetags.common.configs import FieldType, TreeConfig, CacheDefinition
from weetags.common.types import TraversalOrder, OnCollision
from weetags.tree.tree_engine import TreeEngine
from weetags.common.alteration import Alteration
from weetags.tree.tree_cache import TreeCache
from weetags.tree.drawer import DrawStyle, TreeDrawer
from weetags.tree.importer import Importer
from weetags.tree.node import Node
from weetags.tree.fields import TreeFieldsCollection, RelationsCollection, Relation


class TreeAlteration(Alteration):
    """Alteration Handler helper structure

    Handle Alterations Methods for runtime tree Alterations.
    """
    def __init__(self, engine: TreeEngine) -> None:
        super().__init__(engine)

class TreeTopology:
    """Topology Handler helper structure
    
    Tree topology define and handle the structure of tree. 
    Topology object allow to add, remove, move nodes of a tree.
    """
    name: str
    _engine: TreeEngine

    def __init__(self, name: str, engine: TreeEngine) -> None:
        self.name = name
        self._engine = engine

    def add_node(self, name: str, parent: str | None = None, metadata: dict[str, Any] | None = None) -> None:
        """Insert a new node inside a tree.

        :param name: New node name
        :type name: str
        :param parent: parent node name, defaults to None. Only a root nodes has no parent name.
        :type parent: str | None, optional
        :param metadata: new node metadata, defaults to None
        :type metadata: dict[str, Any] | None, optional
        """
        self._engine.add_node(name, parent, metadata)

    def graft_nodes(self, nodes: list[dict[str, Any]], keymap: dict[str, Any] | None = None, on_collision: OnCollision = "raise") -> None:
        """Add multiple nodes into a tree

        nodes must be inline node data such as: `{"name": "...", "parent": "...", ...other metadata key values}`

        :param nodes: Nodes data
        :type nodes: list[dict[str, Any]]
        :param keymap: when set, translate nodes keys into weetags keys, defaults to None
        :type keymap: dict[str, Any] | None, optional
        :param on_collision: adopted strategy when a node payload collide with an existing node. `raise`, `ignore`, `update`, `replace`, defaults to "raise"
        :type on_collision: OnCollision, optional
        """
        Importer(self.name, self._engine, batch_size=100).loads(nodes, keymap, on_collision)

    def import_from_file(self, path: str | Path, keymap: dict[str, Any] | None = None, on_collision: OnCollision = "raise") -> None:
        """Import Nodes from a jsonlines file.

        :param path: File path
        :type path: str | Path
        :param keymap: when set, translate nodes keys into weetags keys, defaults to None
        :type keymap: dict[str, Any] | None, optional
        :param on_collision: adopted strategy when a node payload collide with an existing node. `raise`, `ignore`, `update`, `replace`, defaults to "raise"
        :type on_collision: OnCollision, optional
        """
        Importer(self.name, self._engine, batch_size=100).load(path, keymap, on_collision)

    def export_to_file(self, outfile: str | Path, subtree: str | None = None, order: TraversalOrder = "pre") -> None:
        """Export tree or subtree.

        :param outfile: export file destination
        :type outfile: str | Path
        :param subtree: subtree to export. When None, the whole tree is exported, defaults to None
        :type subtree: str | None, optional
        :param order: exported nodes traversal ordering, defaults to "pre"
        :type order: TraversalOrder, optional
        :raises weetags.common.exceptions.NodeError: Unknown node name.
        """
        self._engine.export_to_file(outfile, subtree, order)

    def move_subtree(self, subtree: str, parent_new_name: str) -> None:
        """Move subtree from one place to another.

        :param subtree: Name of the subtree root node.
        :type subtree: str
        :param parent_new_name: destination parent node
        :type parent_new_name: str
        :raises weetags.common.exceptions.NodeError: Unknown node name.
        """
        self._engine.move_subtree(subtree, parent_new_name)

    def remove_node(self, name: str, force: bool = False) -> None:
        """Node removal from a tree

        `remove_node` doesn't support subtree removal. Any nodes with children will raise a ValueError, unless `force` is set to True

        `force` does turn `remove_node` into `prune_subtree`, which allow subtree removal.

        :param name: Node name to be removed
        :type name: str
        :param force: Force removal of nodes, even when they anchor a subtree, defaults to False
        :type force: bool, optional
        :raises ValueError: Node anchor a subtree.
        :raises weetags.common.exceptions.NodeError: Unknown node name.
        """
        self._engine.remove_node(name, force)

    def remove_nodes(self, *conditions: ColumnElement | Relation, force: bool = False) -> None:
        """Remove Filtered nodes based on conditions and relations.

        `remove_nodes` doesn't support subtree removal. Any nodes with children will raise a ValueError, unless `force` is set to True

        `force` does turn `remove_nodes` into `prune_subtree`, which allow subtree removal.

        .........


        :param conditions: Conditions and relations filters
        :type conditions: ColumnElement | Relation
        :param force: Force removal of nodes, even when they anchor a subtree, defaults to False
        :type force: bool, optional
        """
        self._engine.remove_nodes(*conditions, force=force)

    def prune_subtree(self, name: str) -> None:
        """Prune a subtree

        :param name: Node name of the anchoring subtree node
        :type name: str
        :raises weetags.common.exceptions.NodeError: Unknown node name.
        """
        self._engine.prune_subtree(name)

class TreeMetadata:
    """Metadata Handler helper structure

    Tree Metadata define and handle Data structure of each nodes inside a tree.
    Metadata is mutable, and the Metadata object allow to update, set, append, extend, pop, ... metadata fields of a tree.
    """
    name: str
    _engine: TreeEngine

    def __init__(self, name: str, engine: TreeEngine) -> None:
        self.name = name
        self._engine = engine

    def update_node(self, name: str, **values: Any) -> None:
        """Update a node metadata

        `id`, `name`, `parent`, `children`, `level`, `path` are topological namespaces and cannot be updated.

        :param name: Node name
        :type name: str
        :param values: pairs of `field name` : `value` to update.
        :type values: Any 
        :raises weetags.common.exceptions.NodeError: Unknown node name.
        """
        self._engine.update_node(name, values)

    def update_nodes(self, *conditions: ColumnElement | Relation, **values: Any) -> None:
        """Update filtered nodes based on conditions and relations

        ......................................

        :param conditions: Conditions and relations filters
        :type conditions: ColumnElement | Relation
        :param values: pairs of `field name` : `value` to update.
        :type values: Any 
        """
        self._engine.update_nodes(*conditions, values=values)

    def append_list(self, name: str, key: str, value: Any) -> None:
        """Append a json array field

        :param name: Node name
        :type name: str
        :param key: Json array field name
        :type key: str
        :param value: Value to append
        :type value: Any
        :raises weetags.common.exceptions.NodeError: Unknown node name.
        """
        self._engine.append_list(name, key, value)

    def extend_list(self, name: str, key: str, value: list[Any]) -> None:
        """Extend a json array field

        :param name: Node name
        :type name: str
        :param key: Json array field name
        :type key: str
        :param value: List of values to extend
        :type value: list[Any]
        :raises weetags.common.exceptions.NodeError: Unknown node name.
        """
        self._engine.extend_list(name, key, value)

    def pop_list(self, name: str, key: str, index: int) -> None:
        """Pop a json array element index

        :param name: node name
        :type name: str
        :param key: Json array field name
        :type key: str
        :param index: Index of the poped element 
        :type index: int
        :raises weetags.common.exceptions.NodeError: Unknown node name.
        """
        self._engine.pop_list(name, key, index)

    def set_object_key(self, name: str, path: str, value: Any) -> None:
        """Set or update a json key

        :param name: Node name
        :type name: str
        :param path: Key path to be set. Key path is `.` seperated.
        :type path: str
        :param value: Set value
        :type value: Any
        :raises weetags.common.exceptions.NodeError: Unknown node name.
        """
        self._engine.set_object_key(name, path, value)

    def pop_object_key(self, name: str, path: str,) -> None:
        """Remove a json key

        :param name: Node name
        :type name: str
        :param path: Key path to be poped. Key path is `.` seperated.
        :type path: str
        :raises weetags.common.exceptions.NodeError: Unknown node name.
        """
        self._engine.pop_object_key(name, path)

    def object_append_list(self, name: str, path: str, value: Any) -> None:
        """Append a json array key.

        :param name: Node name
        :type name: str
        :param path: Key path to be poped. Key path is `.` seperated.
        :type path: str
        :param value: append value
        :type value: Any
        :raises weetags.common.exceptions.NodeError: Unknown node name.
        """
        self._engine.object_append_list(name, path, value)

    def object_extend_list(self, name: str, path: str, value: list[Any]) -> None:
        """Extend a json array key.

        :param name: Node name
        :type name: str
        :param path: Key path to be poped. Key path is `.` seperated.
        :type path: str
        :param value: extend value
        :type value: list[Any]
        :raises weetags.common.exceptions.NodeError: Unknown node name.
        """
        self._engine.object_extend_list(name, path, value)

    def object_pop_list(self, name: str, path: str, index: int) -> None:
        """pop a json array key.

        :param name: Node name
        :type name: str
        :param path: Key path to be poped. Key path is `.` seperated.
        :type path: str
        :param index: pop array element index
        :type index: int
        :raises weetags.common.exceptions.NodeError: Unknown node name.
        """
        self._engine.object_pop_list(name, path, index)


class Tree:
    """Tree Structure
    
    Main Abstraction that materialize and handle permanent or temporary Tree representation.
    
    **Alteration**: Handle Alterations Methods for runtime tree Alterations.

    **Topology**: Tree topology define and handle the structure of tree. 
    Topology object allow to add, remove, move nodes of a tree.

    **Metadata**: Tree Metadata define and handle Data structure of each nodes inside a tree.
    Metadata is mutable, and the Metadata object allow to update, set, append, extend, pop, ... metadata fields of a tree.

    **c**: c is an helper collection of a tree fields (TreeFieldsCollection). 
    c object collection can be used as proxy for handling filtering or complex data structure handling such as json arrays or jsons.  

    **r**: r is an helpter collecion of tree relations (RelationsCollection).
    r object collcetion can be used for handling filtering established tree nodes relations.

    """
    name: str
    c: TreeFieldsCollection
    _engine: TreeEngine
    
    def __init__(
        self, 
        name: str, 
        engine: TreeEngine, 
    ) -> None:
        """Initialize a tree structure

        :param name: Tree name
        :type name: str
        :param engine: Underlying Tree engine
        :type engine: TreeEngine
        """
        self.name = name
        self._engine = engine
        self.sync()
    
    def __repr__(self) -> str:
        return f"<Tree: {self.name}>"

    @property
    def r(self) -> RelationsCollection:
        """Collection of Relations that help filtering nodes.

        :return: Relation filter helper class
        :rtype: RelationsCollection
        """
        return RelationsCollection()

    @property
    def Alteration(self) -> Alteration:
        """Alteration class Interfacing tree alteration methods

        :return: Interface for tree alteration methods
        :rtype: Alteration
        """
        return TreeAlteration(self._engine)

    @property
    def Topology(self) -> TreeTopology:
        """Topology class Interfacing tree topology methods

        :return: Interface for tree topology methods
        :rtype: TreeTopology
        """
        return TreeTopology(self.name, self._engine)

    @property
    def Metadata(self) -> TreeMetadata:
        """Metadata class Interfacing tree metadata methods

        :return: Interface for tree Metadata methods
        :rtype: TreeMetadata
        """
        return TreeMetadata(self.name, self._engine)

    @property
    def size(self) -> int:
        """Number of nodes contained by a tree

        :return: tree size
        :rtype: int
        """
        return self._engine.tree_size()

    @property
    def depth(self) -> int:
        """Maximum branch depth in a tree

        :return: tree depth
        :rtype: int
        """
        return self._engine.tree_depth()

    @property
    def degree(self) -> int:
        """Maximum number of children of a tree node.

        :return: Maximum number of children
        :rtype: int
        """
        return self._engine.tree_degree()

    @property
    def breadth(self) -> int:
        """Number of leaves in a tree

        :return: Number of leaves
        :rtype: int
        """
        return self._engine.breadth()

    @property
    def empty(self) -> bool:
        """True when a tree contains no nodes

        :return: Is tree empty.
        :rtype: bool
        """
        return self.size == 0

    @property
    def root(self) -> Node | None:
        """Tree root. None when a tree is empty.

        :return: Root Node or None when a tree is empty
        :rtype: Node | None
        """
        return self._into_node(self._engine.root())

    @property
    def leaves(self) -> list[Node]:
        """All tree leaf nodes

        :return: List of leaf nodes
        :rtype: list[Node]
        """
        return self._into_nodes(self._engine.leaves())

    @property
    def topology_fields(self) -> list[str]:
        """List of Tree Topology reserved namespace

        :return: list of topology field keys
        :rtype: list[str]
        """
        return self._engine._topology.columns.keys()

    @property
    def metadata_fields(self) -> list[str]:
        """List of tree Metadata field keys

        :return: List of metadata field keys
        :rtype: list[str]
        """
        return self._engine._metadata.columns.keys()

    @property
    def topology_fields_dtype(self) -> dict[str, str]:
        """Mapping of topology keys:dtype

        :return: Mapping topology keys, sql dtypes
        :rtype: dict[str, str]
        """
        return {c.name:FieldType.from_sqlalchemy(c.type).value for c in self._engine._topology.columns.values()}

    @property
    def metadata_fields_dtype(self) -> dict[str, str]:
        """Mapping of metadata keys:dtype

        :return: Mapping metadata keys, sql dtypes
        :rtype: dict[str, str]
        """
        return {c.name:FieldType.from_sqlalchemy(c.type).value for c in self._engine._metadata.columns.values()}

    @property
    def infos(self) -> dict[str, Any]:
        """Mapping of tree main properties

        :return: Mapping of tree main properties
        :rtype: dict[str, Any]
        """
        cache = self._engine.cache
        if cache is not None:
            cache = cache.cache_engine.name
        return {
            "name": self.name,
            "size": self.size,
            "depth": self.depth,
            "degree": self.degree,
            "breadth": self.breadth,
            "topology": self.topology_fields_dtype,
            "metadata": self.metadata_fields_dtype,
            "engine": {
                "database": {
                    "dialect": self._engine.uri.dialect,
                    "database": self._engine.uri.database,
                },
                "cache": cache
            }
        }

    @classmethod
    def initialize(cls, name: str, uri: EngineURI, cache: TreeCache | None = None) -> Tree:
        """Initialize An already created Tree.

        :param name: Tree name
        :type name: str
        :param uri: Database uri
        :type uri: EngineURI
        :param cache: Optional tree caching system, defaults to None
        :type cache: TreeCache | None, optional
        :return: Tree structure
        :rtype: Tree
        """
        engine = TreeEngine.build(name, uri, cache)
        return cls(name, engine)

    @classmethod
    def from_engine(cls, name: str, engine: Engine, cache: TreeCache | None = None) -> Tree:
        """Create a Tree Structure by passing an existing Engine. 

        `from_engine` is mainly used to pass TreeBuilder engine to it's resulting Tree structure

        :param name: Tree name
        :type name: str
        :param engine: Existing Engine class object
        :type engine: Engine
        :param cache: Optional tree caching system, defaults to None
        :type cache: TreeCache | None, optional
        :return: Tree structure
        :rtype: Tree
        """
        tree_engine = TreeEngine.from_engine(name, engine, cache)
        return cls(name, tree_engine)

    @classmethod
    def from_specs(cls, path: Path | str, loader: Type[Loader] = YamlLoader) -> Tree:
        """Initialize Tree structure from a given spec file.

        :param path: Path of the specs file
        :type path: Path | str
        :param loader: Specific loader for the specs file. Handle Yaml files by default. defaults to YamlLoader
        :type loader: Type[Loader], optional
        :return: Tree structure
        :rtype: Tree
        """
        path = Path(path)
        configs = TreeConfig.parse_file(path, loader)
        return cls.from_configs(configs)

    @classmethod
    def from_configs(cls, configs: TreeConfig) -> Tree:
        """Initialize Tree structure from a TreeConfig.

        :param configs: TreeConfig Structure
        :type configs: TreeConfig
        :return: Tree Structure
        :rtype: Tree
        """
        kwargs = configs.tree_inline

        cache, c = None, kwargs.pop("cache", None)
        if c is not None:
            assert configs.cache is not None
            cache = TreeCache(configs.cache.cache, **configs.cache.opts) # pyright: ignore
        return cls.initialize(**kwargs, cache=cache)


    def sync(self) -> None:
        """Sync Tree properties with current tree database state"""
        self._engine.reflect()
        self.c = TreeFieldsCollection.from_table(self._engine.tree)

    def set_cache(self, cache: TreeCache | None = None) -> None:
        """Set caching for the tree structure

        :param cache: caching system, defaults to None. When None, Remove tree caching implementation.
        :type cache: TreeCache | None, optional
        """
        self._engine.cache = cache

    def width(self, level: int) -> int:
        """Number of nodes for a given tree level.

        :param level: Tree level. Root is at level 0
        :type level: int
        :return: Number of nodes from a tree level.
        :rtype: int
        """
        return self._engine.width(level)

    def node(self, name: str) -> Node | None:
        """Get a Node from name

        :param name: node name
        :type name: str
        :return: Return a Node when the node name exist, else None.
        :rtype: Node | None
        """
        return self._into_node(self._engine.node(name))

    def node_or_raise(self, name: str) -> Node:
        """Get a Node from name. raise `weetags.common.exceptions.NodeError` when node name doesn't exist

        :param name: node name
        :type name: str
        :return: Return a Node
        :rtype: Node
        :raises weetags.common.exceptions.NodeError: Unknown node name.
        """
        return self._into_node_or_raise(self._engine._node_or_raise(name))

    def nodes(self, *conditions: ColumnElement | Relation) -> list[Node]:
        """Filter tree nodes with a list of conditions and/or relations.
        
        nodes can be filtered using either ``TreeField`` stored in ``Tree.c`` or
        relations accessible in ``Tree.c``.


        examples:
            ``tree.c.code == "fra"``
            ``tree.c.name.ilike("man")``
            ``tree.c.aliases.json_each() == "elsass"``
            ``tree.c.data.json_each("$.aaa") == "bbb"``

        examples:
            ``tree.r.Branch("german")``
            ``tree.r.Ancestors("french")``
        
        :param conditions: Conditions and relations filters
        :type conditions: ColumnElement | Relation
        :return: List of nodes matching the given filters
        :rtype: list[Node]
        """ 
        
        return self._into_nodes(self._engine.nodes(*conditions))

    def parent_node(self, name: str) -> Node | None:
        """Get parent node

        :param name: Node Name
        :type name: str
        :return: parent node as Node or None if the node is a root.
        :rtype: Node | None
        :raises weetags.common.exceptions.NodeError: Unknown node name.
        """
        
        return self._into_node(self._engine.parent_node(name))

    def children_nodes(self, name: str) -> list[Node]:
        """Get node children

        :param name: node name
        :type name: str
        :return: liste of children Nodes
        :rtype: list[Node]
        :raises weetags.common.exceptions.NodeError: Unknown node name.
        """
        return self._into_nodes(self._engine.children_nodes(name))

    def sibling_nodes(self, name: str, include_self: bool = False) -> list[Node]:
        """Get node siblings

        :param name: node name
        :type name: str
        :param include_self: Returned list contains the node described by `name` parameter, defaults to False
        :type include_self: bool, optional
        :return: List of siblings Nodes.
        :rtype: list[Node]
        :raises weetags.common.exceptions.NodeError: Unknown node name.
        """
        return self._into_nodes(self._engine.sibling_nodes(name, include_self=include_self))

    def ancestor_nodes(self, name, include_self: bool = False) -> list[Node]:
        """Get ancestor nodes

        :param name: _description_
        :type name: _type_
        :param include_self: Returned list contains the node described by `name` parameter, defaults to False
        :type include_self: bool, optional
        :return: List of ancetor nodes
        :rtype: list[Node]
        :raises weetags.common.exceptions.NodeError: Unknown node name.
        """
        
        return self._into_nodes(self._engine.ancestor_nodes(name, include_self=include_self))

    def descendant_nodes(self, name: str, include_self: bool = False) -> list[Node]:
        """Get descendant nodes

        :param name: Node name
        :type name: str
        :param include_self: Returned list contains the node described by `name` parameter, defaults to False
        :type include_self: bool, optional
        :return: List of descendant nodes
        :rtype: list[Node]
        :raises weetags.common.exceptions.NodeError: Unknown node name.
        """
        return self._into_nodes(self._engine.descendant_nodes(name, include_self=include_self))

    def branch_nodes(self, name: str) -> list[Node]:
        """Get nodes from a same branch

        :param name: Node name
        :type name: str
        :return: List of nodes from the same branch
        :rtype: list[Node]
        :raises weetags.common.exceptions.NodeError: Unknown node name.
        """
        return self._into_nodes(self._engine.branch_nodes(name))

    def closest(self, name: str, *conditions: ColumnElement | Relation) -> list[tuple[int, Node]]:
        """Get closest Nodes with matching conditions

        :param name: Node name
        :type name: str
        :param conditions: List of filters
        :type conditions: ColumnElement | Relation
        :return: _description_
        :rtype: list[tuple[int, Node]]
        :raises weetags.common.exceptions.NodeError: Unknown node name.
        """
        return [(v["distance"], self._into_node_or_raise(v["node"]))for v in self._engine.closest_nodes(name, *conditions)]

    def distance(self, name: str, other_name: str) -> int:
        """Get distance between 2 nodes

        :param name: Node name
        :type name: str
        :param other_name: other node name
        :type other_name: str
        :return: distance between the 2 nodes
        :rtype: int
        :raises weetags.common.exceptions.NodeError: Unknown node name.
        """
        return self._engine.distance(name, other_name)

    def lowest_common_ancestor(self, name: str, other_name: str) -> Node | None:
        """Get first common ancestor between 2 nodes

        :param name: Node name
        :type name: str
        :param other_name: other node name
        :type other_name: str
        :return: Closest ancestor node
        :rtype: Node | None
        :raises weetags.common.exceptions.NodeError: Unknown node name.
        """
        return self._into_node(self._engine.lowest_common_ancestor(name, other_name))

    def traversal(self, subtree: str | None = None, order: TraversalOrder = "pre") -> Generator[Node]:
        """Subtree traversal

        :param subtree: Specific subtree to traverse. When None, traversal of the entire tree, defaults to None
        :type subtree: str | None, optional
        :param order: define traversal between `pre`, `in`, `post`, `level`, defaults to "pre"
        :type order: TraversalOrder, optional
        :yield: Yield ordered nodes of the subtree
        :rtype: Generator[Node]
        :raises weetags.common.exceptions.NodeError: Unknown node name.
        """

        for d in self._engine.traversal(subtree, order):
            node = self._into_node(d)
            assert node is not None
            yield node

    def draw(self, subtree: str | None = None, style: DrawStyle = "ascii-ex", extra_spacing: bool = False) -> None:
        """Draw Subtree
        
        :param subtree: Specific subtree to draw. When None, draw entire tree. defaults to None
        :type subtree: str | None, optional
        :param style: define tree style (`ascii`, `ascii-ex`, `ascii-exr`, `ascii-em`, `ascii-emv`, `ascii-emh`), defaults to "ascii-ex"
        :type style: DrawStyle, optional
        :param extra_spacing: add an extra space between branches, defaults to False
        :type extra_spacing: bool, optional
        :raises weetags.common.exceptions.NodeError: Unknown node name.
        """
        for line in self._draw(subtree, style, extra_spacing):
            print(line)

    def _draw(self, subtree: str | None = None, style: DrawStyle = "ascii-ex", extra_spacing: bool = False) -> Generator[str]:
        yield from TreeDrawer(self._engine).draw(subtree, style, extra_spacing)

    def _into_node(self, data: dict[str, Any] | None) -> Node | None:
        if data is None:
            return None
        return Node(**data)

    def _into_node_or_raise(self, data: dict[str, Any] | None) -> Node:
        node = self._into_node(data)
        if node is None:
            raise ValueError("Node is None")
        return node

    def _into_nodes(self, data: list[dict[str, Any]]) -> list[Node]:
        return [Node(**d) for d in data]