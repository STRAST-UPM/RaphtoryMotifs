# CEDAR Risk Scoring — Modular Pipeline

Suspicious-activities detection in public procurement: risk scores for every
node (Organization, PublicEntity, Tender, Bid) as similarity to the CEDAR
anomaly scenarios (collusion between organizations, repeated favoritism).

## Pipeline
data generation -> Neo4j (CEDAR data model) -> Raphtory temporal graph
-> motif extraction (>= 3) -> maximum cliques -> scenario-similarity scores
-> per-node query interface

## Layout
```
main.py                     entry point (run + report + CSV export)
cedar_risk/
  config.py                 scenario thresholds + Neo4j settings
  data_generation.py        synthetic market with planted scenarios & ladder
  cypher_queries.py         all Cypher (CLEAR / LOAD / READ) in one place
  neo4j_store.py            staging -> Neo4j -> read-back (with fallback)
  temporal_graph.py         Neo4j rows -> Raphtory graph + tender_index traversal
  motifs.py                 3-org motifs, suspicious groups, max cliques,
                            entity<->group quadruplet motifs
  scoring.py                similarity primitives, checks, org/entity/tender/bid scores
  query.py                  anomaly_score(ctx, node_type, node_id)
```

## Run
```
pip install raphtory neo4j pandas numpy
python main.py                          # uses local Neo4j at bolt://localhost:7687
python main.py --skip-neo4j             # build temporal graph directly from staging
python main.py --outdir results         # also save score tables as CSV
python main.py --neo4j-uri bolt://host:7687 --neo4j-user neo4j --neo4j-password pw
```
Without a reachable Neo4j the pipeline falls back to staging rows that have
exactly the shape of the Cypher read-back, so results are identical.

## Programmatic use
```python
from main import run
from cedar_risk.query import anomaly_score

ctx = run(use_neo4j=False)
print(anomaly_score(ctx, "Organization", "Org_16"))
```
