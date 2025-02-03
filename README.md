# Weetags
**weetags** is a small and simple permanent tree database builded on top of Sqlite.

## Prerequisites
* python >= 3.8
* sqlite3 >= 3.37.2

## How to use
### Building a tree
#### Initial data
A tree can be builded With Initial data from **python dictionnaries**, **Jsonlines files** or **Json files**.
1. **At least One Root node must be given as initial data** when building a tree.
2. **The Data model behind a tree is infered from the initial data nodes** during the building process.
   1. a tree data model is rigid, it currently cannot be modified after it is created, so make sure it encompass all the data you want to store.
   2. make sure the data model is consistent over all your intial nodes.
   3. A tree can contain the folowing types: `string`, `integer`, `float`,`boolean`, `list` as a json string, `dict` as a json string.
      1. Both `list` & `dict` are automatically converted at each operations.
   4. The sources of the initial data can be multiple but must be consistent. All files must of the same format.
3. The nodes must be ordered from Root to leaves.
4. A node design must be as follow: `{"id": "YourNodeID", "parent":"YourNodeParentID", ...}`
   1. `id` and `parent` are 2 necessary fields, You must set them.
   2. `children` relations are infered during the building process.
   3. A Root node parent must be set to `None`.
   4. `...` can contain all the key/values pairs you want to store in the tree.

**as a python list of dictionaries**
```python
data = [
    {"id":"Root", "parent":None, ...},
    {"id":"Node1", "parent":"Root", ...},
    {"id":"Node2", "parent":"Root", ...},
    {"id":"Node3", "parent":"Node2", ...},
]
```
**as a jl file**
```json
    {"id":"Root", "parent":null, "AnyOther":"DataYouWantToStore"}
    {"id":"Node1", "parent":"Root", "AnyOther":"DataYouWantToStore"}
    {"id":"Node2", "parent":"Root", "AnyOther":"DataYouWantToStore"}
    {"id":"Node3", "parent":"Node2", "AnyOther":"DataYouWantToStore"}
```

#### Indexing
Weetags takes advantage of SQL database indexing feature.
1. fastening searches for indexed keys.
2. Allowing to search individual values from `dict` and `list` collections of indexed keys.
   1. Indexing a field of type `list` do unpack the values and index them individually. As a result, it allow to search for any element of a `list` with a simple query.
   2. Indexing a field of type `dict` is a bit different. It allow to select a field from a `dict` such as `dict.x` or `dict.y.z` and to extract it. Similarly to the `list` indexing an extracted element is searchable individually.


#### Building
1. By default, the `TreeBuilder` database is set to memory, in this case your tree operations are not permanent.
2. By default, the `TreeBuilder` build an sql index for the node ids. `indexes` allow to build indexes for other fields by providing the list of fields needing an index.
3. `read_only` mode allow to block any writing operations on the database.
4. `replace` when set to `True`, recreate the tree structure from 0 if the tree already exist in the database

**From files**
<br>you can load data from one or multiple files, as long as `the file format is consistent` and `the data is ordered from parent to children`.

```python
from weetags.tree_builder import TreeBuilder

tree = TreeBuilder.build_tree("tree_name", database="path/to/your/db.db", data=["path0.jl","path1.jl",...], indexes=["key1", "key3", "key4.x"])
```

**From a dict**
```python
from weetags.tree_builder import TreeBuilder

data = [
    {"id":"Root", "parent":None, ...},
    {"id":"Node1", "parent":"Root", ...},
    {"id":"Node2", "parent":"Root", ...},
    {"id":"Node3", "parent":"Node2", ...},
]

tree = TreeBuilder.build_tree("tree_name", database="path/to/your/db.db", data=data, indexes=["key1", "key3", "key4.x"])
```

### Working with Trees
```python
from weetags.tree import Tree

tree = Tree("tree_name", database="path/to/your/db.db")
```

**Reading some nodes**
```python
# Find a node from it's Node id. By default, all fields are returned.
node = tree.node("Healthcare", fields=["id", "parent", "children"])
# {'id': 'Healthcare', 'parent': 'topicsRoot', 'children': ['Medication', 'Doctor', 'Disabilities'], ...}
```

**Nodes relations**
The main perks of weetags is it's tree structure and it's possibility search for any possible existing relations between nodes, such as `ancestors`, `parent`, `siblings`, `children` and `descendants` relations.
Weetags provide a simple api for querying those relations.

```python
node = tree.parent_node("Healthcare") # return the parent node
# return {'id': 'HealthcareParentID', 'parent': 'NodeParentID', 'children': ['Healthcare', ...], ...}
```

Any other relational methods return a list of nodes corresponding to the searched relations.
those returns can be refined with the use of few arguments:
* `order_by` (list[str]): define a list of keys on which the list order is base ( from primary key to more secondary keys): e.g `order_by=["id", "name", ...]`
* `axis` (int): define whether the ordering is ASC (1) or DESC (0). By default, axis value is 1 for an ASC return.
* `limit` (int): define the maximal number of nodes returned by the query.
* `fields` (list[str]): define the nodes fields returned by the query. 

```python
nodes = tree.children_nodes("Healthcare") 
# return all children nodes: [node0, node1, ...]

nodes = tree.siblings_nodes("Healthcare") 
# return all siblings nodes: [node0, node1, ...]

nodes = tree.ancestors_nodes("Healthcare")
# return all ancestors ( nodes higher up in the base node branch ) [node0, node1, ...]

nodes = tree.descendants_nodes("Healthcare")
# return all descendants ( node lower up in the base node branch ) [node0, node1, ...]
```

**Find nodes based on a set of conditions**

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

```python
# Find nodes  from a given set of conditions
nodes = tree.nodes_where(conditions=[[("id","=", "Healthcare"), ("depth", "<", 2)]], limit=1)
# return a list of nodes complying with the conditions [node0]

nodes = tree.nodes_relation_where("ancestors", conditions=[[("id","=", "Healthcare"), ("depth", "<", 2)]])

```

`nodes_relation_where` first search for all nodes complying with the set of conditions first, then looks for the relations of those nodes.

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


**Create a node**

To add a node, you must pass a dictionnary containing all the field_name / values pairs into the `node_values` argument.
* At least, you must pass `id` and `parent` fields and all the non nullable fields that exist in the tree. 
* Make sure that the `parent` node id you input already exist in the tree.
* if not added inside `node_values` arguments, all nullable fields are set to `None` for the added node.
* if not added inside `node_values` arguments, all fields of type `list` or `dict` are added as an empty collection of their respective type.

```python
tree.add_node(nid="Doctor", parent="Healthcare", node_values= {"name": "doctor", ...})
```

**Delete nodes**

Deleting a node with that possess descendants create a `dead branch`.  By default, `dead branches` are also deleted during the process.
`parent` & `children` fields  of related nodes are updated according to the delation.

```python
# deleting one specific node
tree.delete_node(nid="Doctor")

# deleting nodes complying with a set of conditions.
tree.delete_nodes_where(conditions= [[("depth",">", 1)]])
```

**Draw Tree Structure**

```python
# show Topics Subtree.
tree.show_tree(nid="Healthcare") # when nid is not specified, return the whole tree drawing.

# Healthcare
#   ├── Medication
#   ├── Doctor
#   └── Mental health
```


and many other features...