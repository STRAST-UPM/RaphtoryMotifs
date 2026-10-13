"""All Cypher used against the CEDAR data-model graph, in one place."""



READ_NODES_BACKUP = """
MATCH (n)
RETURN labels(n)[0] AS label,
       n.id AS id,
       properties(n) AS props
"""


# Every relationship with its endpoints' ids and all its properties.
READ_RELS_BACKUP = """
MATCH (a)-[r]->(b)
RETURN type(r) AS rel,
       a.id AS src,
       b.id AS dst,
       properties(r) AS props
"""



READ_NODES = """
MATCH (n)
RETURN labels(n)[0] AS label,
       CASE
           WHEN n:organization THEN toString(n.ID_Organization)
           WHEN n:tender THEN toString(n.ID_Tender)
           WHEN n:bid THEN toString(n.ID_Bid)
           WHEN n:public_entity THEN toString(n.ID_Public_Entity)
           ELSE toString(n.id)
       END AS id,
       properties(n) AS props
"""



READ_RELS = """
MATCH (a)-[r]->(b)
RETURN type(r) AS rel,

       CASE
           WHEN a:organization THEN toString(a.ID_Organization)
           WHEN a:tender THEN toString(a.ID_Tender)
           WHEN a:bid THEN toString(a.ID_Bid)
           WHEN a:public_entity THEN toString(a.ID_Public_Entity)
           ELSE toString(a.id)
       END AS src,

       CASE
           WHEN b:organization THEN toString(b.ID_Organization)
           WHEN b:tender THEN toString(b.ID_Tender)
           WHEN b:bid THEN toString(b.ID_Bid)
           WHEN b:public_entity THEN toString(b.ID_Public_Entity)
           ELSE toString(b.id)
       END AS dst,

       properties(r) AS props
"""






 


# Data-model audit (audit_graph.py): every node and relationship untouched,
# identified by elementId -- unique even when two nodes share a business id.
AUDIT_NODES = """
MATCH (n)
RETURN elementId(n) AS key, labels(n) AS labels, properties(n) AS props
"""

AUDIT_RELS = """
MATCH (a)-[r]->(b)
RETURN type(r) AS rel, elementId(a) AS src, elementId(b) AS dst
"""


# Synthetic anomaly graph (synthetic_anomalies/create_neo4j_graph.py), written
# in the same schema as the pilot graphs, so READ_NODES / READ_RELS and the
# scoring read it unchanged. LOCAL graph only: SYNTHETIC_CLEAR empties it.
SYNTHETIC_CLEAR = "MATCH (n) DETACH DELETE n"

SYNTHETIC_LOAD = """
UNWIND $rows AS r
MERGE (pe:public_entity {ID_Public_Entity: r.entity})
MERGE (t:tender {ID_Tender: r.tender})
  SET t.Winning_Bid = r.winning_bid,
      t.Awarded_Price = r.awarded_price,
      t.Responsible_Signee_Name = r.official,
      t.Time_Publication = r.time_publication
MERGE (pe)-[:ISSUES]->(t)
MERGE (t)-[:ISSUED_BY]->(pe)
MERGE (o:organization {ID_Organization: r.organization})
MERGE (b:bid {ID_Bid: r.bid})
  SET b.Time_Submission = r.time_submission,
      b.Contract_Price = r.contract_price,
      b.Responsible_Signee_Name = r.person,
      b.Successful_Bid = r.successful
MERGE (o)-[:SUBMITS]->(b)
MERGE (b)-[:SUBMITTED_FOR]->(t)
"""


# Change detector used by the listener.
STATE = """
MATCH (n)
OPTIONAL MATCH ()-[r]->()
RETURN count(DISTINCT n) AS node_count,
       count(DISTINCT r) AS relationship_count
"""
 