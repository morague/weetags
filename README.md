# Weetags
Weetags is a **persistent tree engine** based on **Sqlite / Postgres** that can be dployed either as a **Framework** or as a **Webserver**

## Weetags Web Server
Weetags propose a Webserver alternative that expose all the Tree methods, & more in the future.

### Setup
Weetags provide a [Dockerfile](/Dockerfile) as well as [docker compose](/compose-examples/compose-example/) and [quadlet](/compose-examples/quadlet-example/) setup examples. DOcker images will be later published to DockerHub.

```yaml
services:
  weetags:
    build: .
    container_name: weetags
    restart: unless-stopped
    ports:
      - 127.0.0.1:8000:8000
    environment:
      WEETAGS_SERVER_CONFIGS_PATH: "server.yml"
      WEETAGS_SECRET_KEY: "..."
      MEMCACHED_SERVER: "localhost:11211"
    command:
      - --host=0.0.0.0
      - --port=8000
      - --workers=4
    volumes:
      - ./data:/app/data
      - ./trees:/app/trees
      - ./databases:/app/databases
      - ./server.yml:/app/server.yml
```
### Configuration
More detailed documentation can be found [here](/docs/). Env variable can be injected inside the configuration files by annotating `${<ENV_VARIABLE_NAME>}`

#### Server Configuration
[Weetags server configuration file](/docs/server_configurations.md). Configuration file path can be referenced in `WEETAGS_SERVER_CONFIGS_PATH` environment variable.
* This file aim at mainly configurating the *enabled api versions* & the *authentication* system.
* `auth.uri` define the database that will store the server `users` & `access rules`.
  * The current weetags version only support **Sqlite** as database for handling authentication operations.

Example:
```yaml
name: weetags
trees: ./trees

api:
  v1: True

configs:
  secret_key: ${WEETAGS_SECRET_KEY}

auth:
  max_age: 10800
  secret_key: ${WEETAGS_SECRET_KEY}
  maximum_priority: 1000
  
  builder:
    recreate_rules: false
    default: deny

  uri:
    dialect: sqlite
    database: "databases/weetags.sqlite"

  rules:
    - type: path
      priority: 1
      methods: ["*"]
      rule: /docs
      role: "*"

    - type: path
      priority: 1
      methods: ["*"]
      rule: /v1/trees
      role: <role_name>

    - type: path
      priority: 9
      methods: ["PUT", "PATCH", "DELETE"]
      rule: /v1/trees/<tree_name>*
      role: <role_name>

    - type: path
      priority: 10
      methods: ["*"]
      rule: /v1/trees/<tree_name>*
      role: <role_name>  
```

#### Tree configuration
Weetags only support Trees defined by a specs file. [Specs file configuration system](/docs/specs_files.md)

Example:
```yaml
name: <tree_name>
type: tree

uri:
  dialect: sqlite
  database: databases/weetags.sqlite
  
cache:
  cache: memcached
  opts:
    server: ${MEMCACHED_SERVER}
    workers: 4
    prefix: weetags_

data:
  path: /path/to/data.jl

builder:
  not_exist: True
  recreate: False
  on_change: raise
  on_collision: raise
  skip_init: False
  skip_import: False

structure:
  fields:
    - name: <field_name>
      type: text
      unique: True
      nullable: false
      index: True

    - name: <field_name>
      type: json
      unique: False
      nullable: True

    - name: <field_name>
      type: json
      unique: False
      nullable: False
```


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


## Development
### Tasks

* [ ] adding unit tests
* **Tree**:
  * [ ] Handle `not_in` with `Relation` class.
  * [ ] Try handling runtime Tree Alteration. Currently, alterations are not reflected at runtime and force to restart processes.
* **Tree Builder**:
  * [ ] fix / handle fields default values
  * [ ] handle more `on_collision` strategies
  * [ ] handle more `on_change` strategies
* **Forest**:
  * [ ] Add support for Forest structures