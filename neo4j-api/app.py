from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
from neo4j import GraphDatabase

app = FastAPI()

API_KEY = "cedar_local_0bc4c44d8c89412fbc4f6271eb28c3f4952f0e89f06c487bb6fb61d6e4fd95af"

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