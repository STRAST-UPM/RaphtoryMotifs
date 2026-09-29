"""Load each CEDAR pilot graph (it / ua / sl) from Neo4j, build its Raphtory
graph, check it against Neo4j, and save it as its own graph.

    python load_pilots.py                  # all three pilots
    python load_pilots.py it sl            # only some
    python load_pilots.py --out graphs     # output folder (default: graphs/)

.env (per pilot, falling back to the plain names):
    GRAPH_API_KEY_IT / GRAPH_API_KEY_UA / GRAPH_API_KEY_SL     (or GRAPH_API_KEY)
    GRAPH_ENDPOINT_IT ...   optional, default https://kg-<pilot>.cedar-heu-project.eu/neo4j/query

Per pilot it writes, in the output folder:
    <pilot>.raphtory   the Raphtory graph
    <pilot>_raw.json   the nodes / relationships exactly as read from Neo4j
                       (rebuild offline later without touching the endpoint)

Read-only against Neo4j.
"""

import argparse
import json
import os
import sys
from collections import Counter
from pathlib import Path

from dotenv import load_dotenv
from raphtory import Graph

from cedar_risk import temporal_graph, unify
from cedar_risk.utils.neo4j_store import read_from_neo4j, run_cypher

PILOTS = ("it", "ua", "sl")
NONE = "(none)"

# Independent counts straight from Neo4j (not derived from the rows we read).
Q_NODES = ("MATCH (n) RETURN count(n) AS n, count(n.id) AS with_id, "
           "count(DISTINCT n.id) AS distinct_id")
Q_RELS = "MATCH ()-[r]->() RETURN count(r) AS n"
Q_LABELS = "MATCH (n) RETURN labels(n)[0] AS label, count(*) AS n"
Q_TYPES = "MATCH ()-[r]->() RETURN type(r) AS rel, count(*) AS n"


# ------------------------------------------------------------------ Neo4j side
def neo4j_counts(endpoint, api_key):
    a = run_cypher(Q_NODES, endpoint, api_key)[0]
    return {
        "nodes": a["n"],
        "nodes_with_id": a["with_id"],
        "nodes_distinct_id": a["distinct_id"],
        "rels": run_cypher(Q_RELS, endpoint, api_key)[0]["n"],
        "by_label": {(r["label"] or NONE): r["n"]
                     for r in run_cypher(Q_LABELS, endpoint, api_key)},
        "by_rel": {r["rel"]: r["n"]
                   for r in run_cypher(Q_TYPES, endpoint, api_key)},
    }


def fetched_stats(nodes, rels):
    """What Raphtory *should* end up with, given the rows that were read."""
    id_count = Counter(n["id"] for n in nodes if n.get("id") is not None)
    label_of = {}
    for n in nodes:
        if n.get("id") is not None:
            label_of.setdefault(n["id"], n["label"] or NONE)
    ok_rels = [r for r in rels if r.get("src") is not None and r.get("dst") is not None]
    dangling = {x for r in ok_rels for x in (r["src"], r["dst"])} - set(label_of)
    pairs_by_rel = {}
    for r in ok_rels:
        pairs_by_rel.setdefault(r["rel"], set()).add((r["src"], r["dst"]))
    return {
        "nodes": len(nodes),
        "null_id": sum(1 for n in nodes if n.get("id") is None),
        "duplicate_ids": sum(c - 1 for c in id_count.values() if c > 1),
        "unique_ids": set(id_count),
        "dangling": len(dangling),
        "expected_nodes": len(label_of) + len(dangling),
        "expected_by_label": Counter(label_of.values()) + Counter({NONE: len(dangling)}),
        "rels": len(rels),
        "by_rel": dict(Counter(r["rel"] for r in rels)),
        "expected_edges_by_rel": {k: len(v) for k, v in pairs_by_rel.items()},
        "expected_edges": len({p for v in pairs_by_rel.values() for p in v}),
        "id_count": id_count,
    }


# -------------------------------------------------------------- Raphtory side
def raphtory_counts(g):
    return {
        "nodes": g.count_nodes(),
        "edges": g.count_edges(),            # distinct (src, dst) pairs, all layers
        "temporal_edges": g.count_temporal_edges(),
        "by_type": Counter(n.node_type or NONE for n in g.nodes),
        "by_layer": {l: g.layers([l]).count_edges() for l in g.unique_layers},
    }


def property_fidelity(g, nodes, id_count):
    """Compare each node's Raphtory properties with what Neo4j holds, after the
    in-place conversion of the used properties (unify.unify_props). Any missing,
    changed or EXTRA property (e.g. a duplicate) counts as a difference."""
    checked, differ, missing, examples = 0, 0, 0, []
    for n in nodes:
        nid = n.get("id")
        if nid is None or id_count[nid] != 1:      # duplicated ids merge on purpose
            continue
        node = g.node(nid)
        if node is None:
            missing += 1
            continue
        want = unify.unify_props(n["label"], n.get("props"))
        got = node.properties.as_dict()
        checked += 1
        bad = sorted({k for k in want if got.get(k) != want[k]} | (set(got) - set(want)))
        if bad:
            differ += 1
            if len(examples) < 3:
                examples.append((nid, bad))
    return checked, differ, missing, examples


# --------------------------------------------------------------------- report
def _status(neo, fetched, expected, raphtory):
    if neo != fetched:
        return "FETCH-MISMATCH"
    if raphtory != expected:
        return "LOAD-MISMATCH"
    return "OK" if neo == raphtory else "OK*"


def _row(name, neo, fetched, expected, raph):
    print(f"  {name:<34}{neo!s:>9}{fetched!s:>9}{expected!s:>9}{raph!s:>9}   "
          f"{_status(neo, fetched, expected, raph)}")


def report(pilot, nodes, rels, neo, g):
    fs, rc = fetched_stats(nodes, rels), raphtory_counts(g)
    print(f"\n{'=' * 78}\nPILOT {pilot.upper()}\n{'=' * 78}")
    print(f"  {'':<34}{'neo4j':>9}{'fetched':>9}{'expected':>9}{'raphtory':>9}")

    _row("nodes", neo["nodes"], fs["nodes"], fs["expected_nodes"], rc["nodes"])
    for label in sorted(set(neo["by_label"]) | set(rc["by_type"])):
        _row(f"  {label}", neo["by_label"].get(label, 0),
             Counter(n["label"] or NONE for n in nodes)[label],
             fs["expected_by_label"].get(label, 0), rc["by_type"].get(label, 0))

    _row("edges (all layers)", neo["rels"], fs["rels"], fs["expected_edges"], rc["edges"])
    for rel in sorted(set(neo["by_rel"]) | set(rc["by_layer"])):
        _row(f"  {rel}", neo["by_rel"].get(rel, 0), fs["by_rel"].get(rel, 0),
             fs["expected_edges_by_rel"].get(rel, 0), rc["by_layer"].get(rel, 0))

    print("\n  neo4j = independent count query; fetched = rows read by the loader;")
    print("  expected = what Raphtory should hold from those rows (distinct ids / distinct")
    print("  (src,dst) per layer); OK* = differs from neo4j only through the causes below.")
    print(f"  raphtory temporal edge updates: {rc['temporal_edges']}")

    notes = []
    if neo["nodes"] != neo["nodes_with_id"]:
        notes.append(f"{neo['nodes'] - neo['nodes_with_id']} nodes have no `id` (not loaded)")
    if neo["nodes_with_id"] != neo["nodes_distinct_id"]:
        notes.append(f"{neo['nodes_with_id'] - neo['nodes_distinct_id']} nodes share an `id` "
                     f"with another node (merged into one Raphtory node)")
    parallel = fs["rels"] - fs["expected_edges"]
    if parallel:
        notes.append(f"{parallel} relationships repeat an existing (src, dst, type) or "
                     f"(src, dst) pair (collapsed: Raphtory keeps one edge per pair and layer)")
    if fs["dangling"]:
        notes.append(f"{fs['dangling']} relationship endpoints are not among the nodes read "
                     f"(created as untyped nodes)")
    for note in notes:
        print(f"  - {note}")

    checked, differ, missing, examples = property_fidelity(g, nodes, fs["id_count"])
    print(f"\n  properties: {checked - differ}/{checked} nodes match Neo4j (used ones converted in place, no extras)"
          + (f", {missing} missing in Raphtory" if missing else ""))
    for nid, what in examples:
        print(f"    differs: {nid}  {what}")

    times = [n.earliest_time.dt for n in g.nodes]
    default = sum(1 for t in times if t.year == 1970)
    if times:
        print(f"  time range: {min(times):%Y-%m-%d} -> {max(times):%Y-%m-%d}"
              f"   nodes at the default time (no usable date): {default}")
    unified_report(nodes, g)
    return fs, rc


def unified_report(nodes, g):
    """How the raw values were unified (raw value -> canonical value)."""
    def raw_props(label):
        return [n.get("props") or {} for n in nodes if unify.norm(n["label"] or "") == label]

    print("\n  used properties, converted in place (raw value -> value in Raphtory)")

    flags = Counter()
    for props in raw_props("bid"):
        raw = unify.pick(props, "successful_bid")
        flags[("(missing)" if raw is None else repr(raw), unify.to_bool(raw))] += 1
    for (raw, val), c in flags.most_common(10):
        print(f"    bid    successful_bid          {raw:<24} -> {val!s:<5}  x{c}")

    for label, field in (("bid", "contract_price"), ("tender", "awarded_price")):
        ok = bad = missing = 0
        bad_examples = Counter()
        for props in raw_props(label):
            raw = unify.pick(props, field)
            if raw is None:
                missing += 1
            elif unify.to_float(raw) is None:
                bad += 1
                bad_examples[repr(raw)] += 1
            else:
                ok += 1
        line = f"    {label:<6} {field:<23} float x{ok}   missing x{missing}   unreadable x{bad}"
        print(line + (f"  e.g. {', '.join(list(bad_examples)[:3])}" if bad else ""))

    T = temporal_graph.tender_index(g)
    winners = Counter(min(sum(b["successful"] for b in t["bids"]), 2) for t in T.values())
    print(f"    tenders by number of successful bids:  0 -> {winners[0]}   "
          f"1 -> {winners[1]}   2+ -> {winners[2]}   (tenders indexed: {len(T)})")

# ------------------------------------------------------------- degree checks
# (title, node label, relationship, direction, neighbour label, Neo4j count query)
DEGREE_CHECKS = [
    ("bids per tender", "tender", "SUBMITTED_FOR", "in", "bid",
     "MATCH (t:tender) OPTIONAL MATCH (b:bid)-[r:SUBMITTED_FOR]->(t) "
     "RETURN t.id AS id, count(DISTINCT b) AS distinct, count(r) AS rels"),
    ("bids per organization", "organization", "SUBMITS", "out", "bid",
     "MATCH (o:organization) OPTIONAL MATCH (o)-[r:SUBMITS]->(b:bid) "
     "RETURN o.id AS id, count(DISTINCT b) AS distinct, count(r) AS rels"),
    ("tenders per public entity", "public_entity", "ISSUES", "out", "tender",
     "MATCH (p:public_entity) OPTIONAL MATCH (p)-[r:ISSUES]->(t:tender) "
     "RETURN p.id AS id, count(DISTINCT t) AS distinct, count(r) AS rels"),
]


def raphtory_degrees(g, label, layer, direction, other):
    """{node id: number of distinct neighbours of type `other` over `layer`}"""
    out = {}
    for n in g.nodes:
        if unify.norm(n.node_type or "") != unify.norm(label):
            continue
        near = set()
        for e in (n.in_edges if direction == "in" else n.out_edges):
            nb = e.src if direction == "in" else e.dst
            if layer in e.layer_names and unify.norm(nb.node_type or "") == unify.norm(other):
                near.add(nb.name)
        out[n.name] = len(near)
    return out


def degree_check(g, neo_query):
    """Per node: number of bids of each tender, bids submitted by each
    organization, tenders issued by each public entity -- Neo4j vs Raphtory."""
    print("\n  degree checks, node by node (distinct neighbours; Neo4j = count query)")
    for title, label, layer, direction, other, query in DEGREE_CHECKS:
        try:
            rows = neo_query(query)
        except Exception as exc:
            print(f"    {title:<28} Neo4j query failed: {type(exc).__name__}: {exc}")
            continue
        neo = {r["id"]: (r["distinct"], r["rels"]) for r in rows}
        rap = raphtory_degrees(g, label, layer, direction, other)
        if not neo and not rap:
            print(f"    {title:<28} no {label} nodes in Neo4j or Raphtory")
            continue
        ids = sorted(set(neo) | set(rap), key=str)
        bad = [(i, neo.get(i), rap.get(i)) for i in ids
               if (neo[i][0] if i in neo else None) != rap.get(i)]
        n_tot = sum(v[0] for v in neo.values())
        n_rel = sum(v[1] for v in neo.values())
        r_tot = sum(rap.values())
        status = "MISMATCH" if bad else ("OK*" if n_rel != n_tot else "OK")
        print(f"    {title:<28} {label} nodes: neo4j {len(neo)} / raphtory {len(rap)}   "
              f"matching {len(ids) - len(bad)}/{len(ids)}   "
              f"total neo4j {n_tot} / raphtory {r_tot}   "
              f"max {max((v[0] for v in neo.values()), default=0)} / "
              f"{max(rap.values(), default=0)}   {status}")
        dist_n = Counter(v[0] for v in neo.values())
        dist_r = Counter(rap.values())
        show = lambda d: ", ".join(f"{k}:{d[k]}" for k in sorted(d)[:8]) + (" ..." if len(d) > 8 else "")
        print(f"        distribution (count:nodes)  neo4j [{show(dist_n)}]"
              + ("" if dist_n == dist_r else f"   raphtory [{show(dist_r)}]"))
        if n_rel != n_tot:
            print(f"        neo4j holds {n_rel - n_tot} repeated relationships (same pair twice); "
                  f"compared on distinct neighbours, as Raphtory keeps one edge per pair")
        for i, a, b in bad[:5]:
            print(f"        differs: {i}  neo4j={a[0] if a else 'absent'}"
                  f"{f' (rels {a[1]})' if a else ''}  raphtory={b if b is not None else 'absent'}")
        if len(bad) > 5:
            print(f"        ... and {len(bad) - 5} more")


# ----------------------------------------------------------------------- main
def pilot_setting(name, pilot, default=None):
    return os.getenv(f"{name}_{pilot.upper()}") or os.getenv(name) or default


def process(pilot, nodes, rels, neo, out, neo_query=None):
    g = temporal_graph.build_temporal_graph(nodes, rels)
    report(pilot, nodes, rels, neo, g)
    if neo_query:
        degree_check(g, neo_query)

    out.mkdir(parents=True, exist_ok=True)
    graph_path, raw_path = out / f"{pilot}.raphtory", out / f"{pilot}_raw.json"
    g.save_to_file(str(graph_path))
    raw_path.write_text(json.dumps({"nodes": nodes, "rels": rels, "neo4j_counts": neo},
                                   ensure_ascii=False, default=str))
    g2 = Graph.load_from_file(str(graph_path))
    same = (g2.count_nodes(), g2.count_edges()) == (g.count_nodes(), g.count_edges())
    print(f"\n  saved {graph_path}  (reload: {g2.count_nodes()} nodes, {g2.count_edges()} "
          f"edges -> {'same' if same else 'DIFFERENT'})\n  saved {raw_path}")
    return g


def run_pilot(pilot, out):
    endpoint = pilot_setting("GRAPH_ENDPOINT", pilot,
                             f"https://kg-{pilot}.cedar-heu-project.eu/neo4j/query")
    api_key = pilot_setting("GRAPH_API_KEY", pilot)
    if not api_key:
        raise RuntimeError(f"no API key: set GRAPH_API_KEY_{pilot.upper()} in .env")
    print(f"[{pilot}] reading {endpoint}")
    nodes, rels = read_from_neo4j(endpoint, api_key)
    neo = neo4j_counts(endpoint, api_key)
    return process(pilot, nodes, rels, neo, out,
                   neo_query=lambda q: run_cypher(q, endpoint, api_key))


if __name__ == "__main__":
    load_dotenv()
    ap = argparse.ArgumentParser()
    ap.add_argument("pilots", nargs="*", help="it ua sl (default: all)")
    ap.add_argument("--out", default="graphs")
    args = ap.parse_args()

    failed = []
    for p in args.pilots or PILOTS:
        try:
            run_pilot(p, Path(args.out))
        except Exception as exc:                      # keep going with the other pilots
            failed.append(p)
            print(f"\n[{p}] FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
    sys.exit(1 if failed else 0)