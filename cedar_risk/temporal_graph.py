"""Raphtory temporal graph with the CEDAR data-model names"""

from raphtory import Graph


def build_temporal_graph(rows):
    """
    
    Build a Raphtory temporal graph from the Neo4j Graph:
    
    Node types :  PublicEntity / Tender / Bid / Organization,
    
    Layers (temporal edges ) :  ISSUES / SUBMITS / SUBMITTED_FOR, 
    
    Time = time_submission.
    
    """
    g = Graph()
    for r in rows:
        g.add_node(r["time_publication"], r["id_tender"],
                   properties={"winning_bid": r["winning_bid"],
                               "awarded_price": r["awarded_price"],
                               "responsible_signee_name": r["tender_signee"]},
                   node_type="Tender")
        g.add_node(r["time_publication"], r["id_public_entity"],
                   node_type="PublicEntity")
        g.add_edge(r["time_publication"], r["id_public_entity"],
                   r["id_tender"], layer="ISSUES")
        g.add_node(r["time_submission"], r["id_organization"],
                   node_type="Organization")
        g.add_node(r["time_submission"], r["id_bid"],
                   properties={"contract_price": r["contract_price"],
                               "responsible_signee_name": r["bid_signee"],
                               "successful_bid": r["successful_bid"]},
                   node_type="Bid")
        g.add_edge(r["time_submission"], r["id_organization"], r["id_bid"],
                   layer="SUBMITS")
        g.add_edge(r["time_submission"], r["id_bid"], r["id_tender"],
                   layer="SUBMITTED_FOR")
    return g


def tender_index(g):
    """One pass over the temporal graph, following the data model's relations;
    the analysis reads only this."""
    
    idx = {}
    for t in g.nodes.type_filter(["Tender"]):
        recs, winner = [], None
        for e in t.in_edges:
            if "SUBMITTED_FOR" not in e.layer_names:
                continue
            b = e.src
            org = [ie.src.name for ie in b.in_edges
                   if "SUBMITS" in ie.layer_names][0]
            recs.append({"bid": b.name, "org": org,
                         "amount": b.properties.get("contract_price"),
                         "person": b.properties.get("responsible_signee_name")})
            if b.properties.get("successful_bid"):
                winner = org
        entity = [e.src.name for e in t.in_edges if "ISSUES" in e.layer_names][0]
        idx[t.name] = {"entity": entity,
                       "official": t.properties.get("responsible_signee_name"),
                       "winner": winner,
                       "winner_bid": t.properties.get("awarded_price"),
                       "bids": recs,
                       "participants": sorted({r["org"] for r in recs})}
    return idx
