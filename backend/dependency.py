"""
Dependency graph builder — topological sort for FK-aware migration ordering.
Groups tables into levels so independent tables can be migrated in parallel.
"""
from __future__ import annotations

from collections import defaultdict, deque


def build_dependency_graph(
    table_names: list[str],
    fk_edges: list[tuple[str, str]],
) -> dict[str, set[str]]:
    """Build adjacency dict: table -> set of tables it depends on (parents).

    Only includes edges where both child and parent are in table_names.
    """
    name_set = set(table_names)
    deps: dict[str, set[str]] = {t: set() for t in table_names}

    for child, parent in fk_edges:
        if child in name_set and parent in name_set and child != parent:
            deps[child].add(parent)

    return deps


def topological_levels(
    deps: dict[str, set[str]],
) -> list[list[str]]:
    """Return tables grouped into dependency levels (Kahn's algorithm).

    Level 0: tables with no dependencies (no FK parents)
    Level 1: tables whose parents are all in level 0
    ...

    If cycles exist, remaining tables are appended as a final level
    with a best-effort ordering.
    """
    # Build in-degree and reverse adjacency
    in_degree: dict[str, int] = {}
    reverse: dict[str, set[str]] = defaultdict(set)

    for table, parents in deps.items():
        in_degree[table] = len(parents)
        for parent in parents:
            reverse[parent].add(table)

    # Seed with zero-dependency tables
    queue = deque(t for t, d in in_degree.items() if d == 0)
    levels: list[list[str]] = []

    while queue:
        current_level = sorted(queue)  # deterministic order within a level
        queue.clear()
        levels.append(current_level)

        for table in current_level:
            for child in reverse[table]:
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)

    # Handle cycles: collect remaining tables
    remaining = [t for t, d in in_degree.items() if d > 0]
    if remaining:
        levels.append(sorted(remaining))

    return levels
