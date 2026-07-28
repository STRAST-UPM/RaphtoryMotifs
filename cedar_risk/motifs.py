"""Motif detection and maximum-clique expansion (collusion Step 1-2), and
the entity<->group quadruplet motifs of the favoritism pipeline."""
from collections import defaultdict
from itertools import combinations

from .config import THRESHOLDS



def org_motifs(T):
    """3-organization -> 1-tender motifs (org+bid are merged)."""
    trip = defaultdict(int)
    for t in T.values():
        for tr in combinations(t["participants"], 3):
            trip[tr] += 1
    return trip

def suspicious_groups(trip):
    return {tr: c for tr, c in trip.items() if c >= THRESHOLDS["REPEAT"]}


def max_cliques(groups):
    """Motif-based expansion: merge motifs sharing 2 orgs (central actors)."""
    keys = list(groups)
    parent = {i: i for i in range(len(keys))}

    def find(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    for i, j in combinations(range(len(keys)), 2):
        if len(set(keys[i]) & set(keys[j])) >= 2:
            parent[find(i)] = find(j)

    merged = defaultdict(list)
    for i in range(len(keys)):
        merged[find(i)].append(keys[i])

    cliques = []
    for motifs in merged.values():
        orgs = sorted(set().union(*motifs))
        cliques.append({
            "orgs": orgs, "motifs": motifs,
            "participation": {o: sum(groups[m] for m in motifs if o in m)
                              for o in orgs}})
    return cliques

def entity_group_motifs(T):
    """Quadruplet motifs: entity + org triplet, counted when the triplet
    participates in the entity's tender and one of them wins."""
    quad = defaultdict(int)
    for t in T.values():
        for tr in combinations(t["participants"], 3):
            if t["winner"] in tr:
                quad[(t["entity"], tr)] += 1
    return quad
