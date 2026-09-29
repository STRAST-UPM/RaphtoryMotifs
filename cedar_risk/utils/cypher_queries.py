"""All Cypher used against the CEDAR data-model graph, in one place."""



READ_NODES = """
MATCH (n)
RETURN labels(n)[0] AS label,
       n.id AS id,
       properties(n) AS props
"""


# Every relationship with its endpoints' ids and all its properties.
READ_RELS = """
MATCH (a)-[r]->(b)
RETURN type(r) AS rel,
       a.id AS src,
       b.id AS dst,
       properties(r) AS props
"""

 
# Change detector used by the listener.
STATE = """
MATCH (n)
OPTIONAL MATCH ()-[r]->()
RETURN count(DISTINCT n) AS node_count,
       count(DISTINCT r) AS relationship_count
"""
 