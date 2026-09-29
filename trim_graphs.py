"""Build a trimmed variant of each pilot's Raphtory graph: same graph, minus
the responsible_person nodes and the item nodes (and every relationship that
touched them). Everything else -- labels, relationships, properties, the
unified fields -- stays exactly as `temporal_graph.build_temporal_graph`
already produces it, since this script feeds it the same rows minus those two
labels.

Raphtory has no "delete node" operation once a graph is built, so this
rebuilds rather than edits: it prefers each pilot's <pilot>_raw.json (written
by load_pilots.py, no Neo4j hit needed) and only falls back to the live
endpoint if that file is not there.

    python trim_graphs.py                   # it, ua, sl
    python trim_graphs.py it sl
    python trim_graphs.py --graphs graphs --out graphs
"""

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

from dotenv import load_dotenv
from raphtory import Graph

from cedar_risk import temporal_graph
from cedar_risk.unify import norm
from cedar_risk.utils.neo4j_store import read_from_neo4j
from load_pilots import pilot_setting  # same GRAPH_ENDPOINT_<PILOT> / GRAPH_API_KEY_<PILOT> lookup

PILOTS = ("it", "ua", "sl")
REMOVE_LABELS = {norm("responsible_person"), norm("item")}


def load_rows(pilot, graphs_dir):
    """(nodes, rels), from <pilot>_raw.json if load_pilots.py already saved it,
    else straight from the live Neo4j endpoint."""
    raw_path = Path(graphs_dir) / f"{pilot}_raw.json"
    if raw_path.exists():
        data = json.loads(raw_path.read_text())
        print(f"[{pilot}] reading {raw_path}")
        return data["nodes"], data["rels"]

    endpoint = pilot_setting("GRAPH_ENDPOINT", pilot,
                             f"https://kg-{pilot}.cedar-heu-project.eu/neo4j/query")
    api_key = pilot_setting("GRAPH_API_KEY", pilot)
    if not api_key:
        raise RuntimeError(f"no {raw_path} and no API key: set GRAPH_API_KEY_{pilot.upper()}")
    print(f"[{pilot}] no {raw_path}; reading {endpoint}")
    return read_from_neo4j(endpoint, api_key)


def trim(nodes, rels):
    """Drop responsible_person and item nodes, and every relationship that
    touched one of them (whatever its type)."""
    removed_by_label = Counter()
    removed_ids = set()
    kept_nodes = []
    for n in nodes:
        if norm(n["label"] or "") in REMOVE_LABELS:
            removed_by_label[n["label"] or "(none)"] += 1
            if n.get("id") is not None:
                removed_ids.add(n["id"])
        else:
            kept_nodes.append(n)

    removed_rels_by_type = Counter()
    kept_rels = []
    for r in rels:
        if r["src"] in removed_ids or r["dst"] in removed_ids:
            removed_rels_by_type[r["rel"]] += 1
        else:
            kept_rels.append(r)

    return kept_nodes, kept_rels, removed_by_label, removed_rels_by_type


def report(pilot, nodes, rels, kept_nodes, kept_rels, removed_by_label,
           removed_rels_by_type, g):
    print(f"\n{'=' * 78}\nPILOT {pilot.upper()}\n{'=' * 78}")
    print(f"  nodes: {len(nodes)} -> {len(kept_nodes)}"
          + (f"   removed: {dict(removed_by_label)}" if removed_by_label else "   (nothing to remove)"))
    print(f"  relationships: {len(rels)} -> {len(kept_rels)}"
          + (f"   removed: {dict(removed_rels_by_type)}" if removed_rels_by_type else ""))
    print(f"  raphtory graph: {g.count_nodes()} nodes, {g.count_edges()} edges, "
          f"layers: {g.unique_layers}")

    remaining_labels = Counter(norm(n["label"] or "") for n in kept_nodes)
    still_there = REMOVE_LABELS & set(remaining_labels)
    if still_there:
        print(f"  WARNING: {still_there} still present after filtering")

    T = temporal_graph.tender_index(g)
    with_official = sum(1 for t in T.values() if t["official"])
    print(f"  tenders indexed: {len(T)}   with a responsible signee on the tender itself: "
          f"{with_official}/{len(T)}  (unaffected -- that property lives on the tender, not "
          f"on the responsible_person node)")


def process(pilot, nodes, rels, out):
    kept_nodes, kept_rels, by_label, by_rel = trim(nodes, rels)
    g = temporal_graph.build_temporal_graph(kept_nodes, kept_rels)
    report(pilot, nodes, rels, kept_nodes, kept_rels, by_label, by_rel, g)

    out.mkdir(parents=True, exist_ok=True)
    path = out / f"{pilot}_trimmed.raphtory"
    g.save_to_file(str(path))
    g2 = Graph.load_from_file(str(path))
    same = (g2.count_nodes(), g2.count_edges()) == (g.count_nodes(), g.count_edges())
    print(f"  saved {path}  (reload: {'same' if same else 'DIFFERENT -- investigate'})")
    return g


if __name__ == "__main__":
    load_dotenv()
    ap = argparse.ArgumentParser()
    ap.add_argument("pilots", nargs="*", help="it ua sl (default: all)")
    ap.add_argument("--graphs", default="graphs", help="where <pilot>_raw.json is read from")
    ap.add_argument("--out", default="graphs", help="where <pilot>_trimmed.raphtory is written")
    args = ap.parse_args()

    failed = []
    for p in args.pilots or PILOTS:
        try:
            nodes, rels = load_rows(p, args.graphs)
            process(p, nodes, rels, Path(args.out))
        except Exception as exc:
            failed.append(p)
            print(f"\n[{p}] FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
    sys.exit(1 if failed else 0)