"""
Loading the Pilots (it , sl , ua ) Neo4j Graph into the Raphtory Temporal Graph Format
"""

from datetime import datetime
from raphtory import Graph
from .unify import norm as _norm, pick, parse_time, to_bool, unify_props


DEFAULT_TIME = datetime(1970, 1, 1) # only for nodes with no usable time at all


TENDER_TIME_FIELDS = ("Time_Publication", "Time_Creation", "Time_Open", "Time_Deadline")
BID_TIME_FIELDS = ("Time_Submission",)       # if absent, the bid takes its tender's time


# For each relationship, the endpoint whose time the edge takes.
EDGE_ANCHOR = {
    "SUBMITS": "dst",          # organization -> bid          : bid time
    "SUBMITTED_FOR": "src",    # bid -> tender                : bid time
    "ISSUES": "dst",           # public_entity -> tender      : tender time
    "ISSUED_BY": "src",        # tender -> public_entity      : tender time
    "REQUIRES": "src",         # tender -> item               : tender time
    "RESPONSIBLE_OF": "dst",   # responsible_person -> entity : entity time
}

def _first_time(props, fields):
    for f in fields:
        dt = parse_time(pick(props, f))
        if dt is not None:
            return dt
    return None


def _resolve_times(by_id, rels):
    t = {}
    for nid, n in by_id.items():
        if _norm(n["label"]) == "tender":
            dt = _first_time(n["props"], TENDER_TIME_FIELDS)
            if dt:
                t[nid] = dt
    bids_without_time = []
    for nid, n in by_id.items():
        if _norm(n["label"]) == "bid":
            dt = _first_time(n["props"], BID_TIME_FIELDS)
            if dt:
                t[nid] = dt
            else:
                bids_without_time.append(nid)
    missing = set(bids_without_time)
    for r in rels:
        if r["rel"] == "SUBMITTED_FOR" and r["src"] in missing and r["dst"] in t:
            t.setdefault(r["src"], t[r["dst"]])

    # everything else (organizations, public entities, items, persons, ...)
    # takes the earliest time among its already-timed neighbours
    for _ in range(4):
        cand = {}
        for r in rels:
            s, d = r["src"], r["dst"]
            if s in t and d in by_id and d not in t:
                cand[d] = min(cand.get(d, t[s]), t[s])
            if d in t and s in by_id and s not in t:
                cand[s] = min(cand.get(s, t[d]), t[d])
        if not cand:
            break
        t.update(cand)
    return t


def _edge_time(r, t):
    ts, td = t.get(r["src"], DEFAULT_TIME), t.get(r["dst"], DEFAULT_TIME)
    anchor = EDGE_ANCHOR.get(r["rel"])
    if anchor == "src":
        return ts
    if anchor == "dst":
        return td
    return max(ts, td)


def _clean(props):
    return {k: v for k, v in (props or {}).items() if v is not None}



def _harmonize(prop_dicts, what):
    """Standardizing Node Properties Across the Graph"""
    seen = {}
    for d in prop_dicts:
        for k, v in d.items():
            seen.setdefault(k, set()).add(type(v))
    for k, types in sorted(seen.items()):
        if len(types) > 1:
            target = float if types == {int, float} else str
            for d in prop_dicts:
                if k in d:
                    d[k] = target(d[k])
            print(f"[graph]   {what} property {k!r} had mixed types "
                  f"({', '.join(sorted(t.__name__ for t in types))}) -> {target.__name__}")


def build_temporal_graph(nodes, rels):
    """Nodes = Neo4j labels  with all their properties;
    edges = relationship types as layers, with all their properties."""
    by_id = {}
    dropped = 0
    for n in nodes:
        if n.get("id") is None:
            dropped += 1
            continue
        by_id[n["id"]] = {"label": n["label"], "props": n.get("props") or {}}
    rels = [r for r in rels if r.get("src") is not None and r.get("dst") is not None]

    t = _resolve_times(by_id, rels)

    node_props = {nid: unify_props(n["label"], n["props"]) for nid, n in by_id.items()}
    edge_props = [_clean(r.get("props")) for r in rels]
    _harmonize(list(node_props.values()), "node")
    _harmonize(edge_props, "edge")

    g = Graph()
    for nid, n in by_id.items():
        g.add_node(t.get(nid, DEFAULT_TIME), nid,
                   properties=node_props[nid], node_type=n["label"])
    for r, props in zip(rels, edge_props):
        g.add_edge(_edge_time(r, t), r["src"], r["dst"],
                   properties=props, layer=r["rel"])
    if dropped:
        print(f"[graph]   {dropped} nodes without `id` were not loaded")
    return g


def node_ids(g):
    """Ids of every organization, public entity and tender in the graph"""
    kinds = {"organization": "organization", "publicentity": "public_entity",
             "tender": "tender"}
    out = {"organization": [], "public_entity": [], "tender": []}
    for n in g.nodes:
        kind = kinds.get(_norm(n.node_type or ""))
        if kind:
            out[kind].append(n.name)
    return out


def tender_index(g):
    """
    One pass over the temporal graph; the analysis reads only this.
    Types are the same for every pilot (the used properties are converted in place):

    idx[tender_id] = {
        "entity":      str | None      public entity that issued the tender
        "official":    str | None      tender responsible signee
        "winner":      str | None      organization of the successful bid (None = no winner)
        "winner_bid":  float | None    awarded price
        "bids": [{"bid": str, "bid_id": ..., "org": str,
                  "amount": float | None, "person": str | None, "successful": bool}],
        "participants": [str]          sorted organizations that bid
    }
    """
    idx = {}
    skipped_bids = 0
    for t in g.nodes:
        if _norm(t.node_type or "") != "tender":
            continue
        tp = t.properties.as_dict()
        recs = []
        for e in t.in_edges:
            if "SUBMITTED_FOR" not in e.layer_names:
                continue
            b = e.src
            bp = b.properties.as_dict()
            orgs = sorted({ie.src.name for ie in b.in_edges
                           if "SUBMITS" in ie.layer_names})
            if not orgs:
                skipped_bids += 1
                continue
            recs.append({"bid": b.name, "bid_id": pick(bp, "id_bid"),
                         "org": orgs[0],
                         "amount": pick(bp, "contract_price"),
                         "person": pick(bp, "responsible_signee_name"),
                         "successful": to_bool(pick(bp, "successful_bid"))})
        recs.sort(key=lambda r: r["bid"])

        winner = None
        flagged = [r for r in recs if r["successful"]]
        if flagged:
            win_id = pick(tp, "winning_bid")
            chosen = next((r for r in flagged
                           if win_id is not None and r["bid_id"] is not None
                           and str(r["bid_id"]) == str(win_id)), None)
            if chosen is None:
                chosen = min(flagged, key=lambda r: (r["amount"] is None,
                                                     r["amount"] or 0.0, r["bid"]))
            winner = chosen["org"]

        entities = sorted({e.src.name for e in t.in_edges
                           if "ISSUES" in e.layer_names})
        idx[t.name] = {"entity": entities[0] if entities else None,
                       "official": pick(tp, "responsible_signee_name"),
                       "winner": winner,
                       "winner_bid": pick(tp, "awarded_price"),
                       "bids": recs,
                       "participants": sorted({r["org"] for r in recs})}
    if skipped_bids:
        print(f"[graph]   {skipped_bids} bids with no organization were left out of the tender index")
    return idx