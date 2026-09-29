"""CEDAR risk-scoring pipeline.

    Neo4j pilot graph(s) (it / ua / sl) -> Raphtory temporal graph -> motif extraction
    -> per-node anomaly scores (organization, public entity, tender)
    -> publish to Kafka (VICOM topic) following the VICOM template

Which pilots to run, in order of precedence:
    1. --pilots on the command line        python main.py --pilots it sl
    2. PILOTS in .env (space/comma separated)   PILOTS=it,sl
    3. every pilot (it, ua, sl)

Run:  python main.py                    -> all pilots, run once then watch each for changes
      python main.py --pilots it sl     -> only those two
      DRY_RUN=1 python main.py --pilots sl   -> compute and print, do not publish

.env:
    GRAPH_API_KEY_IT / GRAPH_API_KEY_UA / GRAPH_API_KEY_SL   (or plain GRAPH_API_KEY)
    GRAPH_ENDPOINT=https://kg-{pilot}.cedar-heu-project.eu/neo4j/query
        "{pilot}" is replaced with the pilot code (it / ua / sl). A per-pilot
        override such as GRAPH_ENDPOINT_IT (with or without "{pilot}") still wins
        over the plain GRAPH_ENDPOINT for that one pilot.
"""


import argparse
import os
import sys
import time
from dotenv import load_dotenv


from cedar_risk.utils import neo4j_store
from cedar_risk import temporal_graph , motifs
from cedar_risk.scoring import organization_scores, entity_scores, tender_scores
from cedar_risk.utils.kafka_client import make_producer, publish_scores
from cedar_risk.utils.config import DELAY
from cedar_risk.utils.neo4j_store import pilot_endpoint , pilot_setting , resolve_pilots


def run(endpoint, api_key):
    """Read the graph from Neo4j, extract motifs, and return the anomaly output
    for organizations, public entities and tenders (three {id: dict} maps)."""
    nodes, rels = neo4j_store.read_from_neo4j(endpoint=endpoint, api_key=api_key)
    print(f"[storage] {len(nodes)} nodes, {len(rels)} relationships from Neo4j")

    g = temporal_graph.build_temporal_graph(nodes, rels)
    T = temporal_graph.tender_index(g)
    print(f"[graph]   {g.count_nodes()} nodes, {g.count_edges()} edges, "
          f"tenders indexed={len(T)}")
    if not T:
        print("[graph]   no tenders in this graph -> nothing to score")
        return {}, {}, {}

    trip = motifs.org_motifs(T)
    cliques = motifs.max_cliques(motifs.suspicious_groups(trip))
    quad = motifs.entity_group_motifs(T)

    ids = temporal_graph.node_ids(g)      # every node gets a score, even with no connection
    org_anom = organization_scores(T, trip, cliques, all_orgs=ids["organization"])
    entity_anom = entity_scores(T, quad, all_entities=ids["public_entity"])
    tender_anom = tender_scores(T, org_anom, entity_anom)   # inherits from the two above
    print(f"[scores]  {len(org_anom)} organizations, {len(entity_anom)} entities, "
          f"{len(tender_anom)} tenders")
    return org_anom, entity_anom, tender_anom


def publish_current(producer, topic, org_anom, entity_anom, tender_anom):
    """Publish one template message carrying every organization / public-entity
    / tender anomaly score. Normal nodes carry just their score."""
    findings = []
    for anom in (org_anom, entity_anom,tender_anom):     
        for node_id, score in anom.items():
            overall = score["overall"]
            print(f"  {score['node_type']:<12} {node_id:<12} risk={overall}")
            findings.append({
                "typeEntity": score["node_type"],
                "entityID": node_id,
                "risk_level": float(overall),
                "description": score.get("description") or f"risk score {overall}",
                "additional_info": score.get("components") or None,
            })
    if not findings:
        print("[kafka]   nothing to publish")
        return None
    if producer is None:
        print(f"[dry-run] {len(findings)} indicators computed, not published")
        return None
    msg = publish_scores(producer, topic, findings)
    print(f"[kafka]   sent {len(findings)} indicators -> {topic} "
          f"(requestID={msg['requestID']})")
    return msg



class PilotContext:
    """Endpoint, API key and change-listener for one pilot."""

    def __init__(self, pilot):
        self.pilot = pilot
        self.endpoint = pilot_endpoint(pilot)
        self.api_key =  pilot_setting("GRAPH_API_KEY", pilot)
        assert self.endpoint and self.api_key, (
                "Set GRAPH_ENDPOINT_<PILOT> and GRAPH_API_KEY_<PILOT> "
                "(or GRAPH_ENDPOINT / GRAPH_API_KEY) in your .env.")
        self.listener = neo4j_store.Neo4jListener(endpoint=self.endpoint, api_key=self.api_key)

    def run_and_publish(self, producer, topic):
        print(f"\n[pilot]   {self.pilot} -> {self.endpoint}")
        scores = run(self.endpoint, self.api_key)
        publish_current(producer, topic, *scores)

    def close(self):
        self.listener.close()


if __name__ == "__main__":
    load_dotenv()

    ap = argparse.ArgumentParser()
    ap.add_argument("--pilots", nargs="+", metavar="PILOT",
                     help="pilots to run, e.g. --pilots it sl (default: PILOTS in "
                          ".env, else every pilot)")
    args = ap.parse_args()

    requested = resolve_pilots(args.pilots)
    contexts = []
    for pilot in requested:
        api_key = pilot_setting("GRAPH_API_KEY", pilot)
        if not api_key:
            print(f"[pilot]   {pilot}: skipped, no GRAPH_API_KEY_{pilot.upper()} "
                  f"(or GRAPH_API_KEY) in .env", file=sys.stderr)
            continue
        contexts.append(PilotContext(pilot))
    assert contexts, (
        f"No pilot had an API key set. Requested: {requested}. "
        f"Set GRAPH_API_KEY_<PILOT> (or GRAPH_API_KEY) in your .env.")
    print(f"[pilots]  running: {', '.join(c.pilot for c in contexts)}")

    dry_run = os.getenv("DRY_RUN", "").lower() in {"1", "true", "yes"}
    if dry_run:
        producer, topic = None, None
    else:
        producer = make_producer(
            bootstrap=os.environ["KAFKA_BOOTSTRAP"],
            username=os.environ["KAFKA_USERNAME"],
            password=os.environ["KAFKA_PASSWORD"],
            security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"),
            sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
        )
        topic = os.environ["KAFKA_TOPIC"]

    for ctx in contexts:
        ctx.run_and_publish(producer, topic)

    try:
        while True:
            for ctx in contexts:
                if ctx.listener.has_changed():
                    print(f"Graph change detected ({ctx.pilot}) " + "#" * 50)
                    ctx.run_and_publish(producer, topic)
            time.sleep(DELAY)
    finally:
        for ctx in contexts:
            ctx.close()
        if producer is not None:
            producer.close()