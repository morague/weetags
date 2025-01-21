# Weetags
**weetags** is a small and simple permanent tree database builded on top of Sqlite.

## Prerequisites
* python >= 3.11
* poetry >= 1.8.3
* sqlite3 >= 3.37.2
* docker >= 24.0.7 (App only)

## How to use
### Building a tree
#### Initial data
A tree can be builded from initial data or from a model describing the fields  names & types pairs. Currently A tree model cannot be modified after it is created, so make sure it encompasse all the data you want to store.
* **Without initial data**: you must provide a `model` for the data. this model can be as simple as a python dictionary describing all the fields & types pairs that are going to be stored in the tree `{field_name:field_value_type, ...}`.
* **With initial data**: Weetags currently only accepts data from `json`, `jsonlines` & `python dictionaries` as initial data sources.
  * **initial data must be ordered**, which means that the first node is always the root. The rule is that no parent node must be after it's children nodes.
  * **Nodes necessary fields**: all nodes must at least have the following fields `{"id": "your_node_id (str)", "parent": "your_node_parent_id"}`. 
  * The children relation is not necessary, as it is builded automaticaly during the tree creation.

in practice, a list of initial data nodes can be represented as follow:
**as a python list of dictionaries**
```python
data = [
    {"id":"Root", "parent":None, **kwargs...},
    {"id":"Node1", "parent":"Root", **kwargs...},
    {"id":"Node2", "parent":"Root", **kwargs...},
    {"id":"Node3", "parent":"Node2", **kwargs...},
]
```

**as a jl file**
```json
    {"id":"Root", "parent":null, **kwargs...}
    {"id":"Node1", "parent":"Root", **kwargs...}
    {"id":"Node2", "parent":"Root", **kwargs...}
    {"id":"Node3", "parent":"Node2", **kwargs...}
```

#### From files
you can load data from one or multiple files, as long as `the file format is consistent` and `the data is ordered from parent to children`.

```python
from weetags.tree_builder import TreeBuilder

tree = TreeBuilder.build_tree("tree_name", database="path/to/your/db.db", data=["path0.jl","path1.jl",...])
```

#### From python dict

```python
from weetags.tree_builder import TreeBuilder

data = [
    {"id":"Root", "parent":None, **kwargs...},
    {"id":"Node1", "parent":"Root", **kwargs...},
    {"id":"Node2", "parent":"Root", **kwargs...},
    {"id":"Node3", "parent":"Node2", **kwargs...},
]

tree = TreeBuilder.build_tree("tree_name", database="path/to/your/db.db", data=data)
```

#### From model
When you don't have initial data to insert inside the tree, you must define a model to correlate your data types with the sql types.
```python
from weetags.tree_builder import TreeBuilder

# mapping python type to sql type mapping.
{"TEXT": str, "INTEGER": int, "JSONLIST": list, "JSON": dict, "BOOL": bool}

model = {
    "field_name1": "dtype1",
    ...
}

tree = TreeBuilder.build_tree("tree_name", database="path/to/your/db.db", model=model)
```

#### Options
1. By default, the `TreeBuilder` database is set to memory, in this case your tree operations are not permanent.
2. By default, the `TreeBuilder` build an sql index for the node ids. `indexes` allow to build indexes for other fields by providing the list of fields needing an index.
   1. Weetags trees can contain complex structures such as `list` and `dict` inside a field. Those are stored as JSON, but are automatically converted into there original data type during each operations. **You can build indexes on list or dict fields**.
   2. Building an index on a `list` field allow to search for the list components directly rather than the full list.
   3. Building an index on an element of a `dict` field allow to search directly for that element.
3. `read_only` mode allow to block any writing operations on the database.
4. `replace` when set to `True`, recreate the tree structure from 0 if the tree already exist in the database




### Working with Trees

**Reading some nodes**
```python
from weetags.tree import Tree

tree = Tree("tree_name", database="path/to/your/db.db")

# Find a node from it's Node id. By default, all fields are returned.
node = tree.node("Healthcare", fields=["id", "parent", "children"])
# returning: {'id': 'Healthcare', 'parent': 'topicsRoot', 'children': ['Medication', 'Doctor', 'Disabilities']}

# Or find relations of a Node
node = tree.parent_node("Healthcare") # return the parent node
nodes = tree.children_nodes("Healthcare") # return list of children nodes
nodes = tree.siblings_nodes("Healthcare") # return list of siblings nodes
nodes = tree.ancestors_nodes("Healthcare") # return list of ancestors nodes
nodes = tree.descendants_nodes("Healthcare") # return list of descendants nodes
```

**Conditions**
Weetags can parse complex combinations of conditions for reading, deleting or updating nodes. Conditions can be a bit of notation heavy.
Conditions are a list of one or multiple combination of conditions, such as: `conditions= [Combination0, Combination1, ...]`. If we translate this example into sql we would get: `WHERE (Combination1) AND (Combination2) AND ...`.
By default, all combination are seperated by an `AND` operator, However you can define yourself the type of operator seperating the combinations, such as: `conditions= [Combination0,"OR", Combination1, ...]`. which would translate into `WHERE (Combination1) OR (Combination2)`

Now lets dive into the Combination themselves.
Each combination is a `list[tuple[field_name, operator, value]]`. For instance: `[("id","=", "Healthcare"), ("depth", "<", 2)]` would translate into `(id = "Healthcare" AND depth < 2)`.
Similarly to said earlier for the combination, you can define yourself the seperator between each conditions. By default, it is an `AND` separator.
`[("id","=", "Healthcare"),"OR", ("depth", "<", 2)]` would translate into `(id = "Healthcare" OR depth < 2)`.

Putting it together:
`conditions= [[("id","=", "Healthcare"), ("depth", "<", 2)], "OR", [("parent", "=", "topicsRoot)]]`
would translate into: `WHERE (id = "Healthcare" AND depth < 2) OR (parent = "topicsRoot")`

**Find nodes based on a set of conditions**

```python
# Find nodes  from a given set of conditions
nodes = tree.nodes_where(conditions=[[]], limit=1)
nodes = tree.nodes_relation_where("Any Relation", conditions=[[]])
```


**Updates nodes**
Necessary fields such as `id`, `parent` and `children` cannot be modified with an update statement.
Use `set_values` argument to pass the fields to be modified with their new values: `set_values=[("field_name", value), ...]`

```python
# update a given node
tree.update_node(nid="Healthcare", set_values=[("name", "healthcare"), ...])

# update all nodes complying with a set conditions
tree.update_nodes_where(conditions=[[("depth",">", 1)]], set_values=[("name", "healthcare"), ...])
```

you can `append` or `extend` `JSONLIST` fields directly with the following methods.
```python
# append a JSONLIST field with a value of the same type.
tree.append_node(nid="Healthcare", field_name="alias", value="health")

# extend a JSONLIST field with a list of values of the same type
tree.append_node(nid="Healthcare", field_name="alias", values=["health", "Hospital"])
```


**Create node**
To add a node, you must pass a dictionnary containing all the field_name / values pairs into the `node` argument.
At least, you must pass `id` and `parent` fields and all the non nullable fields that exist in the tree. 
Make sure that the `parent` node id you input already exist in the tree.

```python
tree.add_node({"id":"Doctor", "parent": "Healthcare", "name": "doctor", ...})
```

**Delete nodes**
```python
# deleting one specific node
tree.delete_node(nid="Doctor")

# deleting nodes complying with a set of conditions.
tree.delete_nodes_where(conditions= [[("depth",">", 1)]])
```

Deleting a node with that possess descendants create a `dead branch`.  By default, `dead branches` are also deleted during the process.
`parent` & `children` fields  of related nodes are updated according to the delation.

**Draw Tree Structure**

```python
# show Topics Subtree.
tree.show_tree(nid="Healthcare") # when nid is not specified, return the whole tree drawing.

# Healthcare
#   ├── Medication
#   ├── Doctor
#   └── Mental health
```