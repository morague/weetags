from __future__ import annotations

from typing import Literal


Relation = Literal["parent", "children", "sibling", "ancestor", "descendant", "branch", "distance", "closest_ancestor"]
TraversalOrder = Literal["pre", "in", "post", "level"]
DrawStyle = Literal["ascii", "ascii-ex", "ascii-exr", "ascii-em", "ascii-emv", "ascii-emh"]

OnChange = Literal["raise", "ignore", "alter", "recreate"]
OnCollision = Literal["raise", "ignore", "update", "replace"]
BatchT = Literal["update", "replace", "insert", "parent_update"]