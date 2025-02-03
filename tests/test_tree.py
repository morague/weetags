import pytest
import sqlite3
import tests.data as data
from weetags.tree import Tree
from weetags.tree_builder import TreeBuilder

@pytest.mark.tree
def test_tree_builder_1():
    tree = TreeBuilder.build_tree("topics", "volume/db.db", ["tags/topics.jl"], indexes=["alias"], replace=True)

@pytest.fixture
def tree() -> Tree:
    return Tree("topics", "volume/db.db", timeout=1)


@pytest.mark.tree
def test_node(tree: Tree):

    node = tree.node("Pet")
    assert node == None

    node = tree.node("Pets")
    assert node == data.PETS

    fields = ["id", "parent", "depth", "is_root", "is_leaf"]
    node1 = tree.node("Pets", fields=fields)
    assert node1 == {k:v for k,v in data.PETS.items() if k in fields}


@pytest.mark.tree
def test_node_where(tree: Tree):

    nodes = tree.nodes_where(conditions=[[("parent","=", "Pets"), "AND", ("id","=", "Pet car")]])
    assert nodes == []

    nodes = tree.nodes_where(conditions=[[("parent","=", "Pets"), "AND", ("id","=", "Pet care")]])
    assert nodes == [data.CARE]

    nodes = tree.nodes_where(conditions=[[("id","ILIKE", "pets")]])
    assert nodes == [data.PETS]

    nodes = tree.nodes_where(conditions=[[("parent","=", "Pets"), ("id","=", "Pet care")]])
    assert nodes == [data.CARE]

    nodes = tree.nodes_where(conditions=[[("id","=", "Pet care"), "OR", ("id", "=", "Employment")]])
    assert nodes == [data.EMPLOYMENT, data.CARE]

    nodes = tree.nodes_where(conditions=[[("parent", "=", "Social services"), ("depth", ">", 1)]])
    assert nodes == [data.CHILDCARE, data.INTEGRATION, data.SSHOTLINES]

    fields = ["id", "parent", "children",  "depth", "is_root", "is_leaf"]
    nodes = tree.nodes_where(conditions=[[("parent", "=", "Social services"), ("depth", ">", 1)]], fields= fields)
    assert nodes == [{k:v for k,v in d.items() if k in fields} for d in [data.CHILDCARE, data.INTEGRATION, data.SSHOTLINES]]

    nodes = tree.nodes_where(conditions=[[("parent", "=", "Social services"), ("depth", ">", 1)]], limit=2)
    assert nodes == [data.CHILDCARE, data.INTEGRATION]

    nodes = tree.nodes_where(conditions=[[("parent", "=", "Social services"), ("depth", ">", 1)]], order_by=["id"], axis=0)
    assert nodes == [data.INTEGRATION, data.SSHOTLINES, data.CHILDCARE]

    nodes = tree.nodes_where(conditions=[[("parent", "=", "Social services"), ("depth", ">", 1)]], fields=fields, limit=2, order_by=["name_ukr"], axis=0)
    assert nodes == [{k:v for k,v in d.items() if k in fields} for d in [data.CHILDCARE, data.SSHOTLINES]]

@pytest.mark.tree
def test_nodes_parent(tree: Tree):

    node = tree.parent_node("Pet car")
    assert node == None

    node = tree.parent_node("Pet care")
    assert node == data.PETS

    fields = ["id", "parent", "children",  "depth", "is_root", "is_leaf"]
    node = tree.parent_node("Pet care", fields=fields)
    assert node == {k:v for k,v in data.PETS.items() if k in fields}

@pytest.mark.tree
def test_nodes_children(tree: Tree):

    nodes = tree.children_nodes("Social service")
    assert nodes == []

    nodes = tree.children_nodes("Social services")
    assert nodes == [data.CHILDCARE, data.SSHOTLINES, data.INTEGRATION]

    fields = ["id", "parent", "children",  "depth", "is_root", "is_leaf"]
    nodes = tree.children_nodes("Social services", fields=fields)
    assert nodes == [{k:v for k,v in d.items() if k in fields} for d in [data.CHILDCARE, data.SSHOTLINES, data.INTEGRATION]]

@pytest.mark.tree
def test_nodes_ancestors(tree: Tree):

    nodes = tree.ancestors_nodes("Pet car")
    assert nodes == []

    nodes = tree.ancestors_nodes("Pet care")
    assert nodes == [data.PETS, data.ROOT]

    fields = ["id", "parent", "children",  "depth", "is_root", "is_leaf"]
    nodes = tree.ancestors_nodes("Pet care", fields=fields)
    assert nodes == [{k:v for k,v in d.items() if k in fields} for d in [data.PETS, data.ROOT]]

@pytest.mark.tree
def test_nodes_descendants(tree: Tree):

    nodes = tree.descendants_nodes("Social service")
    assert nodes == []

    nodes = tree.descendants_nodes("Social services")
    assert nodes == [data.SSHOTLINES, data.INTEGRATION, data.CHILDCARE]

    fields = ["id", "parent", "children",  "depth", "is_root", "is_leaf"]
    nodes = tree.descendants_nodes("Social services", fields=fields)
    assert nodes == [{k:v for k,v in d.items() if k in fields} for d in [data.SSHOTLINES, data.INTEGRATION, data.CHILDCARE]]

@pytest.mark.tree
def test_nodes_siblings(tree: Tree):

    nodes = tree.siblings_nodes("Childcar")
    assert nodes == []

    nodes = tree.siblings_nodes("topicsRoot")
    assert nodes == []

    nodes = tree.siblings_nodes("Childcare")
    assert nodes == [data.SSHOTLINES, data.INTEGRATION]

    fields = ["id", "parent", "children",  "depth", "is_root", "is_leaf"]
    nodes = tree.siblings_nodes("Childcare", fields=fields)
    assert nodes == [{k:v for k,v in d.items() if k in fields} for d in [data.SSHOTLINES, data.INTEGRATION]]

@pytest.mark.tree
def test_nodes_relation_where(tree: Tree):
    ...

    # nodes = tree.nodes_relation_where("")

@pytest.mark.tree
def test_related(tree: Tree):

    rel = tree.is_related("Integration", "Hotline for social services", False)
    assert rel is False

    rel = tree.is_related("Integration", "Hotline for social services", True)
    assert rel is True

    rel = tree.is_related("Integration", "topicsRoot", False)
    assert rel is True

@pytest.mark.tree
def test_path(tree: Tree):

    fields = ["id", "parent"]
    path = tree.path("topicsRoot", "Pet care")
    assert path == [
        {k:v for k,v in data.ROOT.items() if k in fields},
        {k:v for k,v in data.PETS.items() if k in fields},
        {k:v for k,v in data.CARE.items() if k in fields}
    ]

@pytest.mark.tree
def test_draw(tree: Tree):
    tree.show_tree()
    tree.show_tree("Pets")

@pytest.mark.tree
def test_add_nodes(tree: Tree):

    node_data = {"name_eng": "TEST_ENG"}
    tree.add_node(nid="TEST", parent="Pets", node_values=node_data)

    node = tree.node("TEST")
    pnode = tree.node("Pets")
    res = {'id': 'TEST', 'parent': 'Pets', 'children': [], 'alias': [], 'name_eng': 'TEST_ENG', 'name_ukr': None, 'nid': 'TEST', 'depth': 2, 'is_root': 0, 'is_leaf': 1}
    pres = {'id': 'Pets', 'parent': 'topicsRoot', 'children': ['TEST', 'Pet care'], 'alias': ['Тварини', 'Животные'], 'name_eng': 'Pets', 'name_ukr': 'Домашні тварини', 'nid': 'Pets', 'depth': 1, 'is_root': 0, 'is_leaf': 0}
    pres2 = {'id': 'Pets', 'parent': 'topicsRoot', 'children': ['Pet care', 'TEST'], 'alias': ['Тварини', 'Животные'], 'name_eng': 'Pets', 'name_ukr': 'Домашні тварини', 'nid': 'Pets', 'depth': 1, 'is_root': 0, 'is_leaf': 0}
    assert node == res
    assert pnode == pres or pnode == pres2



@pytest.mark.tree
def test_delete_node(tree: Tree):

    node_data = {"name_eng": "TEST_ENG"}
    node_data1 = {"name_eng": "TEST1_ENG"}

    node = tree.node("TEST")
    if node is None:
        tree.add_node(nid="TEST", parent="Pets", node_values= node_data)

    tree.delete_node("TEST")
    node = tree.node("TEST")
    pnode = tree.node("Pets")
    assert node is None
    assert  "TEST" not in pnode["children"]

    tree.add_node(nid="TEST", parent="Pets", node_values=node_data)
    tree.add_node(nid="TEST1", parent="Pet care", node_values=node_data1)
    tree.delete_node("Pets")


    pnode = tree.node("topicsRoot")
    node = tree.node("Pets")
    snode = tree.node("TEST")
    cnode = tree.node("TEST1")

    assert "Pets" not in pnode["children"]
    assert not all([node, snode, cnode])


    tree.delete_nodes_where([[("parent","=", "Education")]])
    n0 = tree.node("Primary education")
    n1 = tree.node("Secondary education")
    assert not all([n0, n1])



@pytest.mark.tree
def test_update_node(tree: Tree):
    d0 = [("id", "TEST")]
    d1 = [("children", ["Pets"])]
    d2 = [("children", "Pets")]
    d3 = [("parent", "Pets")]
    d4 = [("id", "TEST")]
    d6 = [("depth", 3)]
    d5 = [("alias", "Pets")]

    for payload in [d0, d2, d1, d3, d4, d6]:
        with pytest.raises(KeyError):
            tree.update_node(nid="Doctor", set_values=payload)
    with pytest.raises(ValueError):
        tree.update_node(nid="Doctor", set_values=d5)


    payload = [("alias", ["aaa", "bbb", "ccc"]), ("name_eng", "TEST_UPDATE"), ("name_ukr", "TEST_UKR")]
    tree.update_node(nid="Doctor", set_values=payload)
    node = tree.node("Doctor")
    assert node["alias"] == payload[0][1] and node["name_eng"] == payload[1][1] and node["name_ukr"] == payload[2][1]

    payload = [("alias", ["zzz", "www"])]
    tree.update_node(nid="Healthcare", set_values=payload)
    node = tree.node("Healthcare")
    assert node["alias"] == payload[0][1]

    tree.update_nodes_where(conditions=[[("depth","=",0)]], set_values=[("alias", ["ggg", "hhh"])])
    node = tree.node("topicsRoot")
    assert node["alias"] == ["ggg", "hhh"]

    tree.update_nodes_where(conditions=[[("depth","=",1)]], set_values=[("alias", ["rrr", "sss"])])
    node = tree.node("Healthcare")
    assert node["alias"] == ["rrr", "sss"]

@pytest.mark.tree
def test_append_node(tree: Tree):
    tree.append_node(nid="Doctor", field_name="alias", value="ddd")

    node = tree.node("Doctor")
    assert node["alias"] == ["aaa", "bbb", "ccc", "ddd"]

    with pytest.raises(TypeError):
        tree.append_node(nid="Doctor", field_name="name_eng", value="ddd")



@pytest.mark.tree
def test_extend_node(tree: Tree):
    tree.extend_node(nid="Doctor", field_name="alias", values=["eee", "fff", "ggg"])
    node = tree.node("Doctor")
    assert node["alias"] == ["aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg"]

    with pytest.raises(TypeError):
        tree.extend_node(nid="Doctor", field_name="name_eng", values=["ddd"])