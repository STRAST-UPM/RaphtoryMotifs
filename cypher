// ======================================================================
// New Public Entity
MERGE (pe:PublicEntity {
    id_public_entity: "Entity_100"
})

// ======================================================================
// New Tender
MERGE (t:Tender {
    id_tender: "T100"
})

SET t.winning_bid = "B100",
    t.awarded_price = 120000.0,
    t.responsible_signee_name = "Official_100",
    t.time_publication = "2026-07-20T14:30:00"

// Entity issues the tender
MERGE (pe)-[:ISSUES]->(t)

// ======================================================================
// Organization 100 (Winner)

MERGE (o100:Organization {
    id_organization: "Org_100"
})

MERGE (b100:Bid {
    id_bid: "B100"
})

SET b100.time_submission = "2026-07-20T14:35:00",
    b100.contract_price = 120000.0,
    b100.responsible_signee_name = "Person_100",
    b100.successful_bid = true

MERGE (o100)-[:SUBMITS]->(b100)
MERGE (b100)-[:SUBMITTED_FOR]->(t)

// ======================================================================
// Organization 101

MERGE (o101:Organization {
    id_organization: "Org_101"
})

MERGE (b101:Bid {
    id_bid: "B101"
})

SET b101.time_submission = "2026-07-20T14:36:00",
    b101.contract_price = 125500.0,
    b101.responsible_signee_name = "Person_101",
    b101.successful_bid = false

MERGE (o101)-[:SUBMITS]->(b101)
MERGE (b101)-[:SUBMITTED_FOR]->(t)

// ======================================================================
// Organization 102

MERGE (o102:Organization {
    id_organization: "Org_102"
})

MERGE (b102:Bid {
    id_bid: "B102"
})

SET b102.time_submission = "2026-07-20T14:37:00",
    b102.contract_price = 129000.0,
    b102.responsible_signee_name = "Person_102",
    b102.successful_bid = false

MERGE (o102)-[:SUBMITS]->(b102)
MERGE (b102)-[:SUBMITTED_FOR]->(t);