"""Generate the synthetic CEDAR market (data_generation.py) and load it into the
LOCAL Neo4j graph, with the planted anomaly scenarios.

The graph is written in the same schema as the pilot graphs (labels tender,
bid, organization, public_entity; ID_* / Successful_Bid / Contract_Price ...;
SUBMITS, SUBMITTED_FOR, ISSUES, ISSUED_BY), so main.py's reader and the
scoring work on it unchanged -- see test_synthetic.py.

The graph is EMPTIED first. It only ever talks to the local gateway: the
endpoint is SYNTHETIC_GRAPH_ENDPOINT, never GRAPH_ENDPOINT (the pilot graphs),
and any host other than the local ones is refused.

    docker compose -f synthetic_anomalies/docker-compose.yml up -d   # runs this automatically
    python synthetic_anomalies/create_neo4j_graph.py

.env:
    SYNTHETIC_GRAPH_ENDPOINT=http://localhost:8000/neo4j/query   (default)
    SYNTHETIC_GRAPH_API_KEY=<key of the local gateway, neo4j-api/>
"""
import argparse
import os
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))     # the cedar_risk package

import requests
from dotenv import load_dotenv

from data_generation import generate
from cedar_risk.utils.cypher_queries import SYNTHETIC_CLEAR, SYNTHETIC_LOAD
from cedar_risk.utils.logger import get_logger, setup_logging, describe_error
from cedar_risk.utils.neo4j_store import run_cypher

log = get_logger("synthetic")

DEFAULT_ENDPOINT = "http://localhost:8000/neo4j/query"
LOCAL_HOSTS = {"localhost", "127.0.0.1", "cedar-http-api"}


def staging_to_rows(df):
    """One row per bid, with what the graph stores (pilot property names)."""
    rows = []
    for tid, sub in df.groupby("TenderID"):
        winner = sub.Winner.iloc[0]
        wrow = sub.loc[sub.Organization == winner].iloc[0]
        for _, b in sub.iterrows():
            rows.append({
                "tender": tid,
                "entity": b.PublicEntity,
                "official": b.EntityOfficial,
                "time_publication": sub.SubmissionTime.min().isoformat(),
                "winning_bid": wrow.BidID,
                "awarded_price": float(wrow.BidAmount),
                "organization": b.Organization,
                "bid": b.BidID,
                "time_submission": b.SubmissionTime.isoformat(),
                "contract_price": float(b.BidAmount),
                "person": b.ResponsiblePerson,
                "successful": bool(b.Organization == winner),
            })
    return rows


def wait_for_gateway(endpoint, api_key, timeout=180):
    """The gateway starts after Neo4j: retry until it answers."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            run_cypher("RETURN 1 AS ok", endpoint, api_key)
            return
        except requests.HTTPError:
            raise                              # it answers, but refuses (e.g. wrong key)
        except requests.RequestException as e:
            if time.monotonic() > deadline:
                raise
            log.info("[seed]    waiting for the gateway (%s)", describe_error(e)[:80])
            time.sleep(3)


def seed(endpoint, api_key, seed_value=42):
    df, scenarios = generate(seed_value)
    log.info("[seed]    generated: %d tenders, %d organizations, %d public entities, "
             "%d bids (min bidders/tender %d)", df.TenderID.nunique(),
             df.Organization.nunique(), df.PublicEntity.nunique(), len(df),
             df.groupby("TenderID").Organization.nunique().min())

    rows = staging_to_rows(df)
    wait_for_gateway(endpoint, api_key)
    run_cypher(SYNTHETIC_CLEAR, endpoint, api_key)
    run_cypher(SYNTHETIC_LOAD, endpoint, api_key, {"rows": rows})
    counts = run_cypher("MATCH (n) RETURN labels(n)[0] AS label, count(*) AS n",
                        endpoint, api_key)
    log.info("[seed]    loaded into %s: %s", endpoint,
             ", ".join(f"{c['n']} {c['label']}" for c in counts))
    return scenarios


def main():
    load_dotenv()
    setup_logging()
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    endpoint = os.getenv("SYNTHETIC_GRAPH_ENDPOINT") or DEFAULT_ENDPOINT
    api_key = os.getenv("SYNTHETIC_GRAPH_API_KEY")
    if not api_key:
        sys.exit("Set SYNTHETIC_GRAPH_API_KEY (the local gateway's key) in .env")
    host = urlparse(endpoint).hostname
    if host not in LOCAL_HOSTS:
        sys.exit(f"Refusing to empty and seed {endpoint}: only the local gateway "
                 f"({', '.join(sorted(LOCAL_HOSTS))}) may be seeded")
    try:
        seed(endpoint, api_key, args.seed)
    except Exception as e:
        log.exception("[seed]    seeding %s failed: %s", endpoint, describe_error(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
