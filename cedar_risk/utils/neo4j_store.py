"""
Read the CEDAR pilot graphs through the HTTP Neo4j gateway.

The application only talks to an HTTP endpoint using an API key; the gateway
connects to Neo4j over Bolt. This module is read-only.
"""

import requests
import os

from .cypher_queries import READ_NODES, READ_RELS


def run_cypher(query, endpoint, api_key, parameters=None):
    """Execute a Cypher query through the HTTP gateway and return the rows.

    The gateway answers {"result": [...]}; a bare list is also accepted.
    """
    response = requests.post(
        endpoint,
        headers={"X-API-Key": api_key, "Content-Type": "application/json"},
        json={"query": query, "parameters": parameters or {}},
        timeout=60,
    )
    response.raise_for_status()
    data = response.json()
    if isinstance(data, dict) and "result" in data:
        return data["result"]
    return data


def read_from_neo4j(endpoint, api_key):
    """Return (nodes, rels), untouched:

    nodes: [{"label", "id", "props"}]   props = every property as stored
    rels : [{"rel", "src", "dst", "props"}]
    """
    nodes = run_cypher(READ_NODES, endpoint, api_key)
    rels = run_cypher(READ_RELS, endpoint, api_key)
    return nodes, rels


def pilot_setting(name, pilot, default=None):
    return os.getenv(f"{name}_{pilot.upper()}") or os.getenv(name) or default


def pilot_endpoint(pilot):
    """GRAPH_ENDPOINT_<PILOT> if set (used as-is, or with "{pilot}" replaced),
    else the plain GRAPH_ENDPOINT template with "{pilot}" replaced, else the
    same default endpoint load_pilots.py / trim_graphs.py fall back to."""
    return pilot_setting("GRAPH_ENDPOINT",pilot,f"https://kg-{pilot}.cedar-heu-project.eu/neo4j/query").format(pilot=pilot)


def resolve_pilots(cli_pilots):
    """--pilots, else PILOTS in .env (space or comma separated), else every pilot."""
    if cli_pilots:
        return list(cli_pilots)
    env_pilots = os.getenv("PILOTS")
    if env_pilots:
        return env_pilots.replace(",", " ").split()
    
    # backward compatible with the old single-pilot PILOT=<pilot> setting
    if os.getenv("PILOT"):
        return [os.getenv("PILOT")]
    assert True , "Pilot is not configured"