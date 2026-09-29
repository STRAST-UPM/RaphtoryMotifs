"""
Read the CEDAR pilot graphs through the HTTP Neo4j gateway.

The application only talks to an HTTP endpoint using an API key; the gateway
connects to Neo4j over Bolt. This module is read-only.
"""

import requests

from .cypher_queries import READ_NODES, READ_RELS, STATE


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


class Neo4jListener:
    """Polls the HTTP gateway to detect graph changes."""

    def __init__(self, endpoint, api_key):
        self.endpoint = endpoint
        self.api_key = api_key
        self.last_state = None

    def has_changed(self):
        record = run_cypher(STATE, self.endpoint, self.api_key)[0]
        current_state = (record["node_count"], record["relationship_count"])

        if self.last_state is None:
            self.last_state = current_state
            return False
        if current_state != self.last_state:
            self.last_state = current_state
            return True
        return False

    def close(self):
        """Kept for compatibility with the previous implementation."""
        pass