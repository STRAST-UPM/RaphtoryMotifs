"""All Cypher used against the CEDAR data-model graph, in one place."""

CLEAR = "MATCH (n) DETACH DELETE n"

LOAD = """
UNWIND $rows AS r
MERGE (pe:PublicEntity {id_public_entity: r.id_public_entity})
MERGE (t:Tender {id_tender: r.id_tender})
  SET t.winning_bid = r.winning_bid,
      t.awarded_price = r.awarded_price,
      t.responsible_signee_name = r.tender_signee,
      t.time_publication = r.time_publication
MERGE (pe)-[:ISSUES]->(t)
MERGE (o:Organization {id_organization: r.id_organization})
MERGE (b:Bid {id_bid: r.id_bid})
  SET b.time_submission = r.time_submission,
      b.contract_price = r.contract_price,
      b.responsible_signee_name = r.bid_signee,
      b.successful_bid = r.successful_bid
MERGE (o)-[:SUBMITS]->(b)
MERGE (b)-[:SUBMITTED_FOR]->(t)
"""


############## Read from the Neo4j Graph 
READ = """
MATCH (pe:PublicEntity)-[:ISSUES]->(t:Tender)
      <-[:SUBMITTED_FOR]-(b:Bid)<-[:SUBMITS]-(o:Organization)
RETURN t.id_tender AS id_tender,
       t.time_publication AS time_publication,
       t.winning_bid AS winning_bid,
       t.awarded_price AS awarded_price,
       t.responsible_signee_name AS tender_signee,
       pe.id_public_entity AS id_public_entity,
       o.id_organization AS id_organization,
       b.id_bid AS id_bid,
       b.time_submission AS time_submission,
       b.contract_price AS contract_price,
       b.responsible_signee_name AS bid_signee,
       b.successful_bid AS successful_bid
"""
