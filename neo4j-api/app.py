import os

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
from neo4j import GraphDatabase

app = FastAPI()

# the key clients must send (X-API-Key): SYNTHETIC_GRAPH_API_KEY of the root .env
# (synthetic_anomalies/docker-compose.yml passes it)
API_KEY = os.environ.get("SYNTHETIC_GRAPH_API_KEY")
if not API_KEY:
    raise RuntimeError("Set SYNTHETIC_GRAPH_API_KEY in the root .env (the key clients must send)")

driver = GraphDatabase.driver(
    "bolt://neo4j:7687",
    auth=("neo4j", "password") 
)


class CypherQuery(BaseModel):
    query: str
    parameters: dict = {}


@app.post("/neo4j/query")
def run_query(
    body: CypherQuery,
    x_api_key: str = Header(...)
):

    if x_api_key != API_KEY:
        raise HTTPException(
            status_code=401,
            detail="Invalid API key"
        )

    with driver.session() as session:

        result = session.run(
            body.query,
            **body.parameters
        )

        return [dict(record) for record in result]