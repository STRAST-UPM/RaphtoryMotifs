"""Generate the synthetic CEDAR market and load it into Neo4j.

This is the ONLY place the generated DataFrame lives. Run it once (or whenever
you want fresh data) to populate the Neo4j graph that main.py reads from:

    python seed_neo4j.py [--seed 42]

It computes no scores — it only creates data and writes it to Neo4j.
main.py then reads that graph and produces the anomaly scores.
"""
import argparse
import os

from data_generation import generate
from dotenv import load_dotenv

from cedar_risk.utils import neo4j_store


def seed(seed_value, endpoint, api_key):
    df, _ = generate(seed_value)
    print(f"[data]   tenders={df.TenderID.nunique()} "
          f"orgs={df.Organization.nunique()} bids={len(df)} "
          f"(min bidders/tender={df.groupby('TenderID').Organization.nunique().min()})")

    rows = neo4j_store.staging_to_rows(df)
    neo4j_store.load_to_neo4j(rows, endpoint=endpoint, api_key=api_key)
    print(f"[neo4j]  loaded {len(rows)} bid rows into the graph")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    load_dotenv()
    endpoint = os.getenv("GRAPH_ENDPOINT")
    api_key = os.getenv("GRAPH_API_KEY")
    assert endpoint and api_key, (
        "Set GRAPH_ENDPOINT and GRAPH_API_KEY in your .env before seeding.")

    seed(args.seed, endpoint, api_key)