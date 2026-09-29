"""Motif detection and maximum-clique expansion (collusion Step 1-2), and
the entity<->group quadruplet motifs of the favoritism pipeline.

The original scenarios use triplets (GROUP_SIZE = 3). `size` lets the fallback
scoring reuse the same code for pairs (2) and single organizations (1)."""
from collections import defaultdict
from itertools import combinations

from .utils.config import THRESHOLDS
from .utils.score_utils import GROUP_SIZE



def org_motifs(T, size=GROUP_SIZE):
    """3-organization -> 1-tender motifs (org+bid are merged)."""
    trip = defaultdict(int)
    for t in T.values():
        for tr in combinations(t["participants"], size):
            trip[tr] += 1
    return trip

def suspicious_groups(trip):
    return {tr: c for tr, c in trip.items() if c >= THRESHOLDS["REPEAT"]}


def max_cliques(groups):
    """Motif-based expansion: merge motifs sharing 2 orgs (central actors)."""
    shared = max(GROUP_SIZE - 1, 1)
    keys = list(groups)
    parent = {i: i for i in range(len(keys))}

    def find(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    for i, j in combinations(range(len(keys)), 2):
        if len(set(keys[i]) & set(keys[j])) >= shared:
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

def entity_group_motifs(T, size=GROUP_SIZE):
    """Quadruplet motifs: entity + org triplet, counted when the triplet
    participates in the entity's tender and one of them wins.
    Tenders without an issuing entity are skipped."""
    quad = defaultdict(int)
    for t in T.values():
        if t["entity"] is None:
            continue
        for tr in combinations(t["participants"], size):
            if t["winner"] in tr:
                quad[(t["entity"], tr)] += 1
    return quad