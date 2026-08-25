from __future__ import annotations

from typing import Literal

type TreeTypes = Literal["tree", "forest"]
type AcceptedFieldType = Literal["integer", "text", "bool", "json", "datetime"]

type BaseRelations = Literal["parent", "children", "sibling", "ancestor", "descendant", "branch"] 
type Relation = Literal["parent", "children", "sibling", "ancestor", "descendant", "branch", "distance", "closest_ancestor"]
type TraversalOrder = Literal["pre", "in", "post", "level"]
type DrawStyle = Literal["ascii", "ascii-ex", "ascii-exr", "ascii-em", "ascii-emv", "ascii-emh"]

type OnChange = Literal["raise", "ignore", "alter", "recreate"]
type OnCollision = Literal["raise", "ignore", "update", "replace"]
type BatchT = Literal["update", "replace", "insert", "parent_update"]
type CacheType = Literal["local", "memcached"]