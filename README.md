# Weetags
Weetags is a **persistent tree engine** based on **Sqlite / Postgres**.

## Weetags Server
...

## weetags FrameWork
```bash
pip install weetags
```

### Quick Start
#### Database
Weetags is originally oriented towards sqlite database.
*since weetags v2.0.0*, weetags started supporting PostgresQL database. Although, psql support is not yet stable or tested yet, it is recommended to stick with Sqlite for now. 

```python
from weetags import EngineURI
# In memory database
uri = EngineURI()

# persistent trees
uri = EngineURI(database="weetags.sqlite")

# PSQL support
uri = EngineURI(dialect="postgresql", username="user", password="password", host="localhost", port=5432)
```

#### Building a tree
...

```python
from weetags import TreeBuilder, Tree, EngineURI

uri = EngineUri(database="weetags.sqlite")
builder = TreeBuilder.from_uri(uri)
tree = builder.build(
    name="tree_name",
    fields=[
        FieldDefinition("aliases", type="json"), 
        FieldDefinition("code", type="text"), 
        FieldDefinition("data", type="json")
    ],
    data=[
        {"name":"Root", "parent": None, "aliases": [], "code": "root", "data": {"aaa": "bbb", "ccc": {"ddd": "eee"}}},
        {"name":"french", "parent":"Root", "aliases": ["fr", "fra"], "code": "fra", "data": {"aaa": "zzz", "ccc": {"ddd": "xxx"}}},
        {"name":"german", "parent":"Root", "aliases": ["gr", "ger"], "code": "ger", "data": {"aaa": "bbb", "ccc": {"ddd": "eee"}}},
        {"name":"bavarois", "parent":"german", "aliases": ["bar"], "code": "bar", "data": {"aaa": "bbb", "ccc": {"ddd": "eee"}}},
        {"name":"doc", "parent":"french", "aliases": ["langdoc"], "code": "doc", "data": {"aaa": "bbb", "ccc": {"ddd": "eee"}}},
        {"name":"alsacien", "parent":"french", "aliases": ["elsass"], "code": "als", "data": {"aaa": "bbb", "ccc": {"ddd": "eee"}}},
        {"name":"lorrain", "parent":"french", "aliases": [], "code": "lor", "data": {"aaa": "bbb", "ccc": {"ddd": "eee"}}}   
    ],
    indexes=[["code"]],
    unique_constraints=[["code"]],
    not_exist=True
)

```

or from a specs file
```python
tree = builder.build_from_specs("path/to/specs/file.yml")
```

#### Loading an existing tree

```python
from weetags import Tree, TreeCache
tree = Tree.initialize("tree_name", EngineURI(), cache=None)

# Tree with caching mechanism
# local caching
cache = TreeCache("local")

# memcached
cache = TreeCache("memcached", server="localhost:11211", workers=4, prefix="weetags_")

tree = Tree.initialize("tree_name", EngineURI(), cache=cache)
```

#### Searching tree nodes



filtered nodes search can be done with the help of `tree.c`, `tree.r` and `SQL functions`, where:
* `tree.c` is the `fields namespace`, which allow to do some fields comparisons. E.g:
  * `tree.c.code == "fra"`
  * `tree.c.name.ilike("man")`
  * `tree.c.name.in_("man")`
  * ...
* `tree.r` is the `relations namespace`, which allow to filter in / out nodes related to each others. E.g:
  * `tree.r.Branch("german")`
  * `tree.r.Ancestors("french")`
  * ...
* `SQL functions` can handle more complex structures such as `json arrays` or `json objects`. The functions are stored inside `tree.c` fields structures. E.g:
  * `json_each()` functions allow to unpack `arrays` or `objects` and interact with as if they were a regular field:
    * `tree.c.aliases.json_each() == "elsass"`, unpack `aliases` field and filter it's value.
    * `tree.c.data.json_each("$.aaa") == "bbb"`, unpack `data` field `aaa` key and filter it's value.

as a whole:
```python
# filter nodes, where $.data.aaa key == "bbb" and the node is part of the "german" branch
nodes = tree.nodes(tree.c.data.json_each("$.aaa") == "bbb", tree.r.Branch("german"))

# get the root node
root = tree.root

# search specific nodes
node = tree.node(name="node_name")

# Do a tree traversal
for node in tree.traversal():
  print(node)

# filter nodes per relations (parent, children, descendants, ancestors, branch)
nodes = tree.ancestors("german")
```

or many other methods for interacting with trees.

#### Updating nodes metadata
You are free to manipulate your tree nodes metadata. `metadata` encompass every fields that are not part of the topology of a tree
(id, name, parent, children, path, level are topological fields. Therefore cannot be updated via metadata methods).

`Tree.Metadata` encompass every tools at your disposition for handling tree metadata, such as:
* updating a node or a group of nodes metadata fields
  * `Tree.Metadata.update_node("german", {"code": "deu"})`
  * `Tree.Metadata.update_nodes(tree.r.Branch("german"), values={"aliases": ["test"]})`
* updating `json arrays` fields:
  * `Tree.Metadata.append_list("german", aliases, value="test")`
  * `Tree.Metadata.extend_list("german", aliases, value=["test", "v"])`
  * `Tree.Metadata.pop_list("german", aliases, index=-1)`
* updating `json objects` fields by removing/updating keys:
  * `Tree.Metadata.set_object_key("german", "data.aaa", "xxx")`
  * `Tree.Metadata.pop_object_key("german", "data.aaa")`
* updating `json objects` arrays fields keys:
  * `Tree.Metadata.object_append_list("german", "data.aaa", "xxx")`
  * `Tree.Metadata.object_extend_list("german", "data.aaa", ["xxx", "yyy"])`
  * `Tree.Metadata.object_pop_list("german", "data.aaa", index=-1)`

#### Modifying a Tree topology
The tree `topology` define the tree structure and nodes relations. Using `Tree.Topology`, you can handle any topological interactions from:
* Adding a node or a group of nodes:
  * `Tree.Topology.add_node("node_name", "parent_name", metadata={...})`
  * ```python
    Tree.Topology.add_node(nodes= [
      {"name":"french", "parent":"Root", "aliases": ["fr", "fra"], "code": "fra", "data": {"aaa": "zzz", "ccc": {"ddd": "xxx"}}},
      {"name":"doc", "parent":"french", "aliases": ["langdoc"], "code": "doc", "data": {"aaa": "bbb", "ccc": {"ddd": "eee"}}},
      {"name":"alsacien", "parent":"french", "aliases": ["elsass"], "code": "als", "data": {"aaa": "bbb", "ccc": {"ddd": "eee"}}},
      {"name":"lorrain", "parent":"french", "aliases": [], "code": "lor", "data": {"aaa": "bbb", "ccc": {"ddd": "eee"}}}
    ])

  ```
* Removing a node or a group of nodes:
  * `Tree.Topology.remove_node("german", force=True)`, 
  * `Tree.Topology.remove_nodes(tree.c.data.json_each("$.aaa") == "bbb", force=True)`
    * `remove_node` & `remove_nodes` methods can only remove leaves nodes by default and would raise an error if removing the nodes generate some orphans. `force=True` can force the removal of the subtree by applying some `pruning` instead.
  * `Tree.Topology.prune_subtree("node_name")`. Pruning is your default option for removing a whole subtree.
* Moving part of the tree (subtree):
  * `Tree.Topology.move_subtree("german", "french")` would put the `german` subtree under the `french` node.
* Import / export the tree:
  * `Tree.Topology.import_from_file("path/to/data.jl")`
  * `Tree.Topology.export_to_file("path/to/export.jl")`

#### Tree Alterations
Tree Alterations are possible through the `Tree.Alteration` object. However, it is currently very limited and unstable. Altering a tree in te middle of your runtime is not recommended in the current release.


## Tasks
* [ ] adding unit tests
* **engine**
  * [x] Drop tree capabilities
* **builder**
  * [ ] accept nested payload format
  * [x] build indexes
  * [x] build unique constraints
  * [ ] Cmp existing / importer payload structure & typing for the `alteration` path.
  * [ ] handle basic alteration for the `alteration` path.
* **configs**
  * [ ] handle all new builders arguments
  * [ ] add default value support
* **drawer**
  * [ ] build an optimized tree drawer
* **cache**
  * [ ] continue adding tree methods support
  * [ ] btree storage of the topology & more granular storage.
  * [ ] more granular storage of the references

* **known issues**:
  * [ ] not null and default clash. a not adding a non nullable despite having a default raise an Integrity error.
  * [ ] engine does'nt reflect tree alterations.
  * [ ] error during node creation process yield situations where topology exist, but not metadata. remove topology on failled insertion

## Importation
### Accepted payload format
accepted json format:
* nested format:
```json
{"name": str, "parent": str, "metadata": {...}}
```
* inline format:
```json
{"name": str, "parent": str, ...}
```





## Engine

```



```
