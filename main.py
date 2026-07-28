"""CEDAR risk-scoring pipeline.

    Neo4j (CEDAR data model) -> Raphtory temporal graph -> motif extraction
    -> per-node anomaly scores (organization, public entity, tender)
    -> publish to Kafka (VICOM topic) following the VICOM template


Run:  python main.py
"""

import os
import time

from dotenv import load_dotenv

from cedar_risk import temporal_graph, motifs, neo4j_store
from cedar_risk.scoring import organization_scores, entity_scores, tender_scores
from cedar_risk.kafka_client import make_producer, publish_scores
from cedar_risk.config import DELAY

def run(endpoint, api_key):
    """Read the graph from Neo4j, extract motifs, and return the anomaly output
    for organizations, public entities and tenders (three {id: dict} maps)."""
    rows = neo4j_store.read_from_neo4j(endpoint=endpoint, api_key=api_key)
    print(f"[storage] {len(rows)} bid rows from Neo4j")

    g = temporal_graph.build_temporal_graph(rows)
    T = temporal_graph.tender_index(g)
    print(f"[graph]   {g.count_nodes()} nodes, {g.count_edges()} edges, "
          f"tenders indexed={len(T)}")

    trip = motifs.org_motifs(T)
    cliques = motifs.max_cliques(motifs.suspicious_groups(trip))
    quad = motifs.entity_group_motifs(T)

    org_anom = organization_scores(T, trip, cliques)
    entity_anom = entity_scores(T, quad)
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
    msg = publish_scores(producer, topic, findings)
    print(f"[kafka]   sent {len(findings)} indicators -> {topic} "
          f"(requestID={msg['requestID']})")
    return msg


if __name__ == "__main__":
    load_dotenv()

    GRAPH_ENDPOINT = os.getenv("GRAPH_ENDPOINT")
    GRAPH_API_KEY = os.getenv("GRAPH_API_KEY")
    assert GRAPH_ENDPOINT and GRAPH_API_KEY, (
        "Set GRAPH_ENDPOINT and GRAPH_API_KEY in your .env.")

    producer = make_producer(
        bootstrap=os.environ["KAFKA_BOOTSTRAP"],
        username=os.environ["KAFKA_USERNAME"],
        password=os.environ["KAFKA_PASSWORD"],
        security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"),
        sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
    )
    topic = os.environ["KAFKA_TOPIC"]

    listener = neo4j_store.Neo4jListener(endpoint=GRAPH_ENDPOINT, api_key=GRAPH_API_KEY)

    scores = run(GRAPH_ENDPOINT, GRAPH_API_KEY)
    publish_current(producer, topic, *scores)

    try:
        while True:
            if listener.has_changed():
                print("Graph change detected " + "#" * 60)
                scores = run(GRAPH_ENDPOINT, GRAPH_API_KEY)
                publish_current(producer, topic, *scores)
            time.sleep(DELAY)
    finally:
        listener.close()
        producer.close()
