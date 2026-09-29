"""Score the saved pilot graphs (made by load_pilots.py) and check that every
organization, public entity and tender got a risk score. No Kafka involved.

    python score_pilots.py                 # it, ua, sl
    python score_pilots.py it              # one pilot
    python score_pilots.py --graphs graphs --out scores

Writes <out>/<pilot>_scores.json with the three score maps.
"""

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

from raphtory import Graph

from cedar_risk import temporal_graph, motifs
from cedar_risk.scoring import organization_scores, entity_scores, tender_scores
from cedar_risk.utils.config import THRESHOLDS

PILOTS = ("it", "ua", "sl")


def score_graph(g):
    T = temporal_graph.tender_index(g)
    ids = temporal_graph.node_ids(g)
    trip = motifs.org_motifs(T)
    cliques = motifs.max_cliques(motifs.suspicious_groups(trip))
    quad = motifs.entity_group_motifs(T)
    org = organization_scores(T, trip, cliques, all_orgs=ids["organization"])
    ent = entity_scores(T, quad, all_entities=ids["public_entity"])
    ten = tender_scores(T, org, ent)
    return T, ids, org, ent, ten


def summarize(title, scores, expected_ids):
    vals = [s["overall"] for s in scores.values()]
    missing = sorted(set(expected_ids) - set(scores))
    line = f"  {title:<14} scored {len(scores)}/{len(expected_ids)}"
    if vals:
        line += (f"   min {min(vals):.3f}  mean {sum(vals) / len(vals):.3f}  max {max(vals):.3f}"
                 f"   > 0: {sum(v > 0 for v in vals)}"
                 f"   >= {THRESHOLDS['SUSPICIOUS']}: {sum(v >= THRESHOLDS['SUSPICIOUS'] for v in vals)}")
    print(line)
    if missing:
        print(f"    NOT SCORED: {missing[:5]}{' ...' if len(missing) > 5 else ''}")
    basis = Counter(s["basis"] for s in scores.values() if "basis" in s)
    if basis:
        print("    based on: " + ", ".join(f"{b} x{c}" for b, c in basis.most_common()))
    for s in sorted(scores.values(), key=lambda s: -s["overall"])[:3]:
        if s["overall"] > 0:
            print(f"    top: {s['node_id']}  {s['overall']}"
                  + (f"  ({s['description']})" if s.get("description") else ""))


def run(pilot, graphs, out):
    path = Path(graphs) / f"{pilot}.raphtory"
    g = Graph.load_from_file(str(path))
    print(f"\n{'=' * 78}\nPILOT {pilot.upper()}  ({path})\n{'=' * 78}")
    T, ids, org, ent, ten = score_graph(g)
    print(f"  graph: {g.count_nodes()} nodes, {g.count_edges()} edges; tenders indexed: {len(T)}")
    summarize("organizations", org, ids["organization"])
    summarize("public entities", ent, ids["public_entity"])
    summarize("tenders", ten, ids["tender"])
    closed = sum(1 for t in T.values() if t["winner"] is not None)
    print(f"  tenders with a winner: {closed}/{len(T)}   "
          f"without an issuing entity: {sum(t['entity'] is None for t in T.values())}")
    out.mkdir(parents=True, exist_ok=True)
    (out / f"{pilot}_scores.json").write_text(
        json.dumps({"organizations": org, "public_entities": ent, "tenders": ten},
                   ensure_ascii=False, indent=1, default=str))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("pilots", nargs="*", help="it ua sl (default: all)")
    ap.add_argument("--graphs", default="graphs")
    ap.add_argument("--out", default="scores")
    args = ap.parse_args()
    failed = []
    for p in args.pilots or PILOTS:
        try:
            run(p, args.graphs, Path(args.out))
        except Exception as exc:
            failed.append(p)
            print(f"\n[{p}] FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
    sys.exit(1 if failed else 0)