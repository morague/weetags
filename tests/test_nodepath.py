import pytest
from weetags.common.path_utils import NodePath, NodePathCollection

@pytest.fixture
def p():
    path = "aaa.bbb.ccc.ddd.eee.fff.ggg"
    p = NodePath(path)
    return p 

@pytest.fixture
def npc():
    paths = ["aa.bb.cc.dd.ee", "aa.bb.cc.ff.gg", "aa.bb.cc.hh", "aa.bb.cc.ii.mm.nn", "rr.ss.tt"]
    c = NodePathCollection(*paths)
    return c


def test_nodepath_contains_node(p) -> None:
    assert p.contains_node("aaa") is True
    assert p.contains_node("zzz") is False
    
def test_nodepath_contains_subpath(p) -> None:
    assert p.contains_subpath("bbb.ccc.ddd") is True
    assert p.contains_subpath("aaa.zzz") is False

def test_nodepath_suffix(p) -> None:
    assert p.prefix(1) == "aaa"
    assert p.prefix(3) == "aaa.bbb.ccc"
    assert p.prefix(len(p.nodes)) == "aaa.bbb.ccc.ddd.eee.fff.ggg"

    with pytest.raises(ValueError):
        p.prefix(0)
        p.prefix(-1)
        p.prefix(8)

def test_nodepath_prefix(p) -> None:
    assert p.suffix(1) == "ggg"
    assert p.suffix(3) == "eee.fff.ggg"
    assert p.suffix(len(p.nodes)) == "aaa.bbb.ccc.ddd.eee.fff.ggg"

    with pytest.raises(ValueError):
        p.suffix(0)
        p.suffix(-1)
        p.suffix(8)

def test_nodepath_subpath(p) -> None:
    s = p.subpath("aaa", node_is="root")
    assert s == "aaa.bbb.ccc.ddd.eee.fff.ggg"

    s = p.subpath("aaa", node_is="leaf")
    assert s == "aaa"

    s = p.subpath("ggg", node_is="root")
    assert s == "ggg"

    s = p.subpath("ggg", node_is="leaf")
    assert s == "aaa.bbb.ccc.ddd.eee.fff.ggg" 

    s = p.subpath("ccc", node_is="leaf")
    assert s == "aaa.bbb.ccc"

    s = p.subpath("ccc", node_is="root")
    assert s == "ccc.ddd.eee.fff.ggg"

    with pytest.raises(ValueError):
        p.subpath("zzz", node_is="root")

def test_nodepath_distance(p) -> None:
    assert p.distance("aaa", "bbb") == 1
    assert p.distance("aaa", "aaa") == 0
    with pytest.raises(ValueError):
        p.distance("zzz", "bbb")
        p.distance("bbb", "zzz")

def test_nodepath_iter(p) -> None:
    with pytest.raises(ValueError):
        p.iter_from("zzz")
        p.iter_until("zzz")

    v = [v for v in p.iter_until("ccc")]
    assert len(v) == 3
    assert v[-1] == "ccc"

    v = [v for v in p.iter_from("ccc")]
    assert len(v) == 5
    assert v[0] == "ccc"

def test_nodepath_lstrip(p) -> None:
    assert p.lstrip_until("ccc") == "ccc.ddd.eee.fff.ggg"
    assert p.lstrip_until("ccc", include_node=True) == "ddd.eee.fff.ggg"
    assert p.lstrip_until("ggg") == "ggg"

    with pytest.raises(ValueError):
        p.lstrip_until("zzz")
        p .lstrip_until("ggg", include_node=True)


def test_nodepath_rstrip(p) -> None:
    assert p.rstrip_from("ccc") == "aaa.bbb.ccc"
    assert p.rstrip_from("ccc", include_node=True) == "aaa.bbb"
    assert p.rstrip_from("aaa") == "aaa"

    with pytest.raises(ValueError):
        p.rstrip_from("zzz")
        p .rstrip_from("aaa", include_node=True)

def test_npc_ancestors(npc):
    assert npc.ancestors_of("cc") == ["aa", "bb"]
    assert npc.ancestors_of("aa") == []
    with pytest.raises(ValueError):
        npc.ancestors_of("oo")

def test_npc_descendants(npc):
    assert npc.descendants_of("tt") == []
    assert npc.descendants_of("ii") == ["mm", "nn"]
    assert npc.descendants_of("cc") == ['dd', 'ee', 'ff', 'gg', 'hh', 'ii', 'mm', 'nn']

def test_npc_branch(npc):
    assert npc.branch_of("cc") == ['aa', 'bb', 'cc', 'dd', 'ee', 'ff', 'gg', 'hh', 'ii', 'mm', 'nn']
    assert npc.branch_of("ii") == ['aa', 'bb', 'cc', 'ii', 'mm', 'nn']