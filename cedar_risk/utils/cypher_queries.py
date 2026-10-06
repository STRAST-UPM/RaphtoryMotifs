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


# Change detector used by the listener.
STATE = """
MATCH (n)
OPTIONAL MATCH ()-[r]->()
RETURN count(DISTINCT n) AS node_count,
       count(DISTINCT r) AS relationship_count
"""
 