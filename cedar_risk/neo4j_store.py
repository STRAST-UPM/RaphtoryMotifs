"""
Persist and read the market as a CEDAR-data-model graph through an HTTP
Neo4j gateway.

The application communicates only with an HTTP endpoint using an API key.
The HTTP gateway is responsible for connecting to Neo4j over Bolt.
"""

import requests
import pandas as pd

from . import cypher_queries as Q


def run_cypher(query, endpoint, api_key, parameters=None):
    """
    Execute a Cypher query through the HTTP gateway.

    Parameters
    ----------
    query : str
        Cypher query.
    endpoint : str
        HTTP endpoint (e.g. http://localhost:8000/neo4j/query)
    api_key : str
        API key sent in the X-API-Key header.
    parameters : dict, optional
        Cypher parameters.

    Returns
    -------
    list
        JSON response returned by the gateway.
    """

    response = requests.post(
        endpoint,
        headers={
            "X-API-Key": api_key,
            "Content-Type": "application/json",
        },
        json={
            "query": query,
            "parameters": parameters or {},
        },
        timeout=60,
    )

    response.raise_for_status()

    return response.json()


def staging_to_rows(df):
    """
    One row per bid with the DATA MODEL attribute names.
    """

    rows = []

    for tid, sub in df.groupby("TenderID"):

        winner = sub.Winner.iloc[0]
        wrow = sub.loc[sub.Organization == winner].iloc[0]

        for _, b in sub.iterrows():

            rows.append({
                "id_tender": tid,
                "time_publication": sub.SubmissionTime.min(),
                "winning_bid": wrow.BidID,
                "awarded_price": float(wrow.BidAmount),
                "tender_signee": b.EntityOfficial,
                "id_public_entity": b.PublicEntity,
                "id_organization": b.Organization,
                "id_bid": b.BidID,
                "time_submission": b.SubmissionTime,
                "contract_price": float(b.BidAmount),
                "bid_signee": b.ResponsiblePerson,
                "successful_bid": bool(b.Organization == winner),
            })

    return rows


def load_to_neo4j(rows, endpoint, api_key):
    

    run_cypher(
        Q.CLEAR,
        endpoint,
        api_key,
    )
    
    run_cypher(
        Q.LOAD,
        endpoint,
        api_key,
        {
            "rows": [
                {
                    **r,
                    "time_publication": r["time_publication"].isoformat(),
                    "time_submission": r["time_submission"].isoformat(),
                }
                for r in rows
            ]
        },
    )


def read_from_neo4j(endpoint, api_key):

    rows = run_cypher(
        Q.READ,
        endpoint,
        api_key,
    )

    for r in rows:
        r["time_publication"] = pd.Timestamp(r["time_publication"])
        r["time_submission"] = pd.Timestamp(r["time_submission"])

    return rows


def fetch_rows(
    df,
    use_neo4j=True,
    endpoint=None,
    api_key=None,
):
    """
    Staging -> Neo4j -> read-back.
    Falls back to staging rows if the gateway is unavailable.
    """

    rows = staging_to_rows(df)

    if not use_neo4j:
        return rows, "neo4j skipped"

    try:

        load_to_neo4j(
            rows,
            endpoint,
            api_key,
        )

        return (
            read_from_neo4j(
                endpoint,
                api_key,
            ),
            "neo4j round-trip",
        )

    except Exception as exc:

        return rows, f"fallback ({type(exc).__name__})"


class Neo4jListener:
    """
    Polls the HTTP gateway to detect graph changes.
    """

    def __init__(self, endpoint, api_key):

        self.endpoint = endpoint
        self.api_key = api_key
        self.last_state = None

    def has_changed(self):

        record = run_cypher(
            """
            MATCH (n)
            OPTIONAL MATCH ()-[r]->()
            RETURN
                count(DISTINCT n) AS node_count,
                count(DISTINCT r) AS relationship_count
            """,
            self.endpoint,
            self.api_key,
        )[0]

        current_state = (
            record["node_count"],
            record["relationship_count"],
        )

        if self.last_state is None:
            self.last_state = current_state
            return False

        if current_state != self.last_state:
            self.last_state = current_state
            return True

        return False

    def close(self):
        """
        Kept for compatibility with the previous implementation.
        """
        pass