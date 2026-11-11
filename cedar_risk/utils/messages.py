"""Messages of the CEDAR risk-scoring tool, following the CEDAR Kafka
message contract: validating a request, planning what it asks for, and
building the response. Sending them is kafka_client.py's job.

Flow
  1. The UI  publishes a request on the pilot's request topic (it_analyse, sl_analyse).
  2. main.py runs the requested indicators on the requested entities.
     This tool has two indicators (two use cases, one tool):
       collusion   UPM_RAPHTORY_UC5_AI9  organizations, and tenders (collusion risk)
       favoritism  UPM_RAPHTORY_UC2_AI6  public entities, and tenders (favoritism risk)
     One response item per requested indicator per entity: a tender asked for
     both gets two items, each with its own risk_level.
  3. main.py publishes ONE response on the pilot's results topic
     (it_results_available, sl_results_available).
  4. The UI matches the response to its request using requestID.
  
 every tender, organization and public entity of the graph.
"""

import json
import os
import time
import uuid

TOOL_NAME = "UPM_RAPHTORY_MOTIFS"
ALL = "all"

# This tool's two indicators, one per use case. A request is answered only if
# it lists at least one of them. Each can be renamed in .env.
DEFAULT_INDICATORS = {"collusion": "UPM_RAPHTORY_UC5_AI9",
                      "favoritism": "UPM_RAPHTORY_UC2_AI6"}
_INDICATOR_ENV = {"collusion": "INDICATOR_ID_COLLUSION",
                  "favoritism": "INDICATOR_ID_FAVORITISM"}

# the node types each use case scores
USE_CASE_TYPES = {"collusion": ("Organization", "Tender"),
                  "favoritism": ("PublicEntity", "Tender")}


def indicator_ids():
    """{use case: indicator_id}. Read when used (not at import), so the values
    of .env count even though it is loaded after this module is imported."""
    return {uc: os.getenv(env) or DEFAULT_INDICATORS[uc]
            for uc, env in _INDICATOR_ENV.items()}

NODE_TYPES = ("Organization", "PublicEntity", "Tender")

# entityType used in requests and in source.kg.entities[]. 
ENTITY_TYPES = {
    "Organization": "organization",
    "PublicEntity": "publicEntity",
    "Tender": "tender",
}

# entityType of a request (case-insensitive) -> node type; 
_REQUEST_TYPES = {"tender": "Tender", "organization": "Organization",
                  "publicentity": "PublicEntity", "public_entity": "PublicEntity",
                  "bid": None}


SCORE_RANGE = {"from": 0.0, "to": 1.0}

# Items measured on an organization group
# value      -> the measurement itself (e.g. number of tenders, success rate)
# risk_level -> its 0-1 risk score

ORG_GROUP_ITEMS = [
    ("Group recurrence", "appearances", "repetition", "tenders",
     "Number of tenders the group bid on together; the risk grows with it "
     "up to the repetition threshold."),
    ("Group success rate", "group_success_rate", "success_risk", None,
     "Share of the group's joint tenders won by one of its members; the risk "
     "also requires enough wins for the rate to be credible."),
    ("Fake competition", "fake_competition", "fake_competition", None,
     "Losing bids from the group stay consistently above the winning bid."),
    ("Winner rotation", "winner_rotation", "winner_rotation", None,
     "How evenly the wins are spread across the group's members."),
    ("Responsible-person consistency", "person_consistency", "person_consistency", None,
     "Share of the group's bids submitted by the same responsible person."),
]

ENTITY_GROUP_ITEMS = [
    ("Group recurrence", "appearances", "repetition", "tenders",
     "Number of this entity's tenders the group bid on; the risk grows with it "
     "up to the repetition threshold."),
    ("Group success in this entity's tenders", "success_rate", "success_risk", None,
     "Share of these tenders won by a member of the group; the risk also "
     "requires enough wins for the rate to be credible."),
    ("Targeting rate", "targeting_rate", "targeting_rate", None,
     "Share of all the group's tenders that were issued by this entity."),
    ("Official consistency", "official_consistency", "official_consistency", None,
     "Share of the group's wins awarded by the same entity official."),
    ("Restricted competition", "restricted_competition", "restricted_competition", None,
     "Group members as a share of all distinct bidders in these tenders."),
]

# (title, description) of each tender score -- value and risk are the same number
TENDER_COMPONENTS = {
    "collusion": ("Collusion risk",
                  "Awarded: risk of the winning organization. "
                  "Open: expected risk of a suspicious group among the bidders."),
    "favoritism": ("Favoritism risk",
                   "Risk of the issuing public entity."),
}



# Request (UI -> tool)

class InvalidRequest(ValueError):
    """The request does not follow the contract: it is logged and skipped."""


def build_request(entities, indicators=None, kg_version="latest",
                  issuer="ui", request_id=None):
    """A request as in the contract. `entities` is a list of
    {"entityType", "entityId"}; entityId may be "all"."""
    request = {
        "issuer": issuer,
        "requestID": request_id or _uid(),
        "entities": list(entities),
        "kg": {"versionId": kg_version},
        "accounting": {},
        "status": {},
    }
    if indicators:
        request["indicators"] = list(indicators)
    return request


def _text(x):
    return isinstance(x, str) and x.strip() != ""


def parse_request(value):
    """Decode (bytes / str / dict) and validate one request; raises InvalidRequest."""
    if isinstance(value, (bytes, bytearray)):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            raise InvalidRequest("not UTF-8 text")
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as e:
            raise InvalidRequest(f"not valid JSON ({e})")

    req = value
    if not isinstance(req, dict):
        raise InvalidRequest("not a JSON object")

    for key in ("issuer", "requestID"):
        if not _text(req.get(key)):
            raise InvalidRequest(f"'{key}' must be a non-empty string")

    entities = req.get("entities")
    if not isinstance(entities, list):
        raise InvalidRequest("'entities' must be a list")
    for i, e in enumerate(entities):
        if not isinstance(e, dict) or not _text(e.get("entityType")) \
                or not _text(e.get("entityId")):
            raise InvalidRequest(f"entities[{i}] needs string 'entityType' and 'entityId'")
        if e["entityType"].strip().lower() == ALL and e["entityId"].strip().lower() != ALL:
            raise InvalidRequest(f"entities[{i}]: entityType 'all' only goes with entityId 'all'")

    indicators = req.get("indicators")
    if indicators is not None and (not isinstance(indicators, list)
                                   or not all(isinstance(x, str) for x in indicators)):
        # the contract's common mistake: [{"indicatorId": "..."}] instead of strings
        raise InvalidRequest("'indicators' must be a list of indicator_id strings")

    kg = req.get("kg")
    if not isinstance(kg, dict) or not _text(kg.get("versionId")):
        raise InvalidRequest("'kg.versionId' must be a non-empty string")

    for key in ("accounting", "status"):
        if key in req and not isinstance(req[key], dict):
            raise InvalidRequest(f"'{key}' must be an object")
    return req


def request_plan(request):
    """What a valid request asks THIS tool for:
    {use case: {node_type: [ids] or ALL}}, e.g.
    {"collusion": {"Tender": ["T3"]}, "favoritism": {"Tender": ["T3"]}}.

    None when the request lists none of this tool's indicators (it is for
    another tool; the request topics are shared). A use case only takes the
    entity types it scores (collusion: organizations and tenders; favoritism:
    public entities and tenders); "bid" is never scored.
    """
    requested = set(request.get("indicators") or [])
    use_cases = [uc for uc, iid in indicator_ids().items() if iid in requested]
    if not use_cases:
        return None

    plan = {}
    for uc in use_cases:
        wanted = {}
        for e in request["entities"]:
            etype = e["entityType"].strip().lower()
            eid = e["entityId"].strip()
            types = NODE_TYPES if etype == ALL else (_REQUEST_TYPES.get(etype),)
            for node_type in types:
                if node_type not in USE_CASE_TYPES[uc]:
                    continue
                if eid.lower() == ALL:
                    wanted[node_type] = ALL
                elif wanted.get(node_type) != ALL:
                    ids = wanted.setdefault(node_type, [])
                    if eid not in ids:
                        ids.append(eid)
        if wanted:
            plan[uc] = wanted
    return plan


def describe_plan(plan):
    """'collusion: all Tender, 2 Organization; favoritism: all Tender' -- for the logs."""
    return "; ".join(
        f"{uc}: " + ", ".join(f"{'all' if ids == ALL else len(ids)} {node_type}"
                              for node_type, ids in wanted.items())
        for uc, wanted in plan.items()) or "nothing for this tool"



# Message pieces

def now_ms():
    return int(time.time() * 1000)


def _uid():
    return str(uuid.uuid4())


def _names(values):
    return ", ".join(str(v) for v in values)


def info_item(title, value, type_, units=None, description=None,
              risk_level=None, range_=None):
    """One additional_info[] entry; optional keys are left out when unset."""
    item = {"id": _uid(), "title": title, "type": type_, "value": value}
    optional = {"units": units, "description": description,
                "risk_level": risk_level, "range": range_}
    item.update({k: v for k, v in optional.items() if v is not None})
    return item


def _score_items(components, spec):
    """numeric additional_info items for the 0-1 component scores."""
    items = []
    for key, (title, description) in spec.items():
        if key in components:
            v = float(components[key])
            items.append(info_item(title, v, "numeric", description=description,
                                   risk_level=v, range_=SCORE_RANGE))
    return items


def _group_items(components, spec):
    """numeric additional_info items measured on a group:
    value = the measurement, risk_level = its 0-1 risk score."""
    items = []
    for title, value_key, risk_key, units, description in spec:
        if value_key not in components:
            continue
        value = components[value_key]
        value = int(value) if units else float(value)
        risk = float(components.get(risk_key, 0.0))
        items.append(info_item(title, value, "numeric", units=units,
                               description=description, risk_level=risk,
                               range_=None if units else SCORE_RANGE))
    return items


def _finding(score, use_case=None):
    """The text from descriptions.py, only when the node is above the threshold.
    For a tender, the text of the one use case (collusion or favoritism)."""
    if use_case is not None:
        text = (score.get("descriptions") or {}).get(use_case)
    else:
        text = score.get("description")
    return [info_item("Finding", text, "long_text")] if text else []


def _org_info(score):
    items = _finding(score)

    if score.get("bids_submitted") is not None:
        items.append(info_item("Bids submitted", int(score["bids_submitted"]), "numeric"))
        items.append(info_item("Success rate", float(score.get("success_rate", 0.0)),
                               "numeric", range_=SCORE_RANGE,
                               description="Tenders won by this organization / bids it submitted."))

    group = score.get("group")
    if not group or len(group) == 1:
        # no group (never bid, or scored on its own record):
        # nothing measured on a group is shown
        items.append(info_item("Organization group",
                               "The organization doesn't belong to any group.", "text"))
        return items

    items.append(info_item("Organization group", _names(group), "text"))
    items += _group_items(score.get("components") or {}, ORG_GROUP_ITEMS)
    return items


def _entity_info(score):
    items = _finding(score)

    if score.get("tenders_issued") is not None:
        items.append(info_item("Tenders issued", int(score["tenders_issued"]), "numeric"))

    group = score.get("group")
    if not group:
        # no organization group in its tenders: nothing measured on a group is shown
        return items

    items.append(info_item("Favored organization group", _names(group), "text"))
    items += _group_items(score.get("components") or {}, ENTITY_GROUP_ITEMS)
    return items


def _tender_info(score, use_case):
    """A tender's details for ONE use case: its collusion risk (with the
    suspicious group) or its favoritism risk (with the public entity and the
    favored group), plus the bidding stage, participants and winner."""
    items = _finding(score, use_case)
    components = score.get("components") or {}
    items += _score_items({use_case: components[use_case]} if use_case in components else {},
                          TENDER_COMPONENTS)

    ex = score.get("explanation") or {}
    if ex.get("bidding_stage") is not None:
        items.append(info_item("Bidding stage",
                               "open" if ex["bidding_stage"] else "awarded", "text"))
    if ex.get("participants"):
        items.append(info_item("Participants", _names(ex["participants"]), "text"))
    if ex.get("winning_organization"):
        items.append(info_item("Winning organization", str(ex["winning_organization"]), "text"))
    if use_case == "collusion" and ex.get("collusion_group"):
        items.append(info_item("Suspicious organization group",
                               _names(ex["collusion_group"]), "text"))
    if use_case == "favoritism":
        if ex.get("public_entity"):
            items.append(info_item("Public entity", str(ex["public_entity"]), "text"))
        if ex.get("favoritism_group"):
            items.append(info_item("Favored organization group",
                                   _names(ex["favoritism_group"]), "text"))
    return items


def _info(node_type, use_case, score):
    if node_type == "Tender":
        return _tender_info(score, use_case)
    return _org_info(score) if node_type == "Organization" else _entity_info(score)


def _risk(node_type, use_case, score):
    """A tender's risk for that use case; an organization's or a public
    entity's overall risk (they belong to one use case only)."""
    if node_type == "Tender":
        return float((score.get("components") or {}).get(use_case, 0.0))
    return float(score["overall"])


# Response (tool -> UI)

def _indicator(use_case, node_type, node_id, score):
    return {
        "id": _uid(),
        "indicator_id": indicator_ids()[use_case],
        "entityID": str(node_id),
        "risk_level": _risk(node_type, use_case, score),
        "additional_info": _info(node_type, use_case, score),
    }


def failed_indicator(use_case, entity_id, reason):
    """Contract: a failed analysis is still published, with value
    "Analysis Failed", risk_level 0.0 and the reason in description."""
    return {
        "id": _uid(),
        "indicator_id": indicator_ids()[use_case],
        "entityID": str(entity_id),
        "risk_level": 0.0,
        "additional_info": [info_item("Analysis status", "Analysis Failed", "text",
                                      description=reason, risk_level=0.0)],
    }


def _related(node_type, score):
    """Other entities the score was computed from (for source.kg.entities)."""
    if node_type == "Tender":
        ex = score.get("explanation") or {}
        related = [("Organization", p) for p in ex.get("participants") or []]
        if ex.get("public_entity"):
            related.append(("PublicEntity", ex["public_entity"]))
        return related
    return [("Organization", m) for m in score.get("group") or []]


def answer(plan, scores=None, failure=None):
    """(analysis.indicators[], source.kg.entities[]) for one request.

    plan    : from request_plan() -- {use case: {node_type: [ids] or ALL}}
    scores  : {node_type: {node_id: score}} from scoring.py
    failure : reason text when the analysis could not run -> every requested
              entity is answered with "Analysis Failed"

    One item per use case per entity: a tender asked for both use cases gets a
    collusion item and a favoritism item.
    """
    indicators, used = [], {}

    def use(node_type, node_id):
        key = (ENTITY_TYPES[node_type], str(node_id))
        used.setdefault(key, {"entityType": key[0], "entityId": key[1]})

    for use_case, wanted in plan.items():
        for node_type, ids in wanted.items():
            if failure is not None:
                for eid in ([ALL] if ids == ALL else ids):
                    indicators.append(failed_indicator(use_case, eid, failure))
                    use(node_type, eid)
                continue

            by_id = {str(k): v for k, v in ((scores or {}).get(node_type) or {}).items()}
            for eid in (list(by_id) if ids == ALL else ids):
                use(node_type, eid)
                score = by_id.get(eid)
                if score is None:
                    indicators.append(failed_indicator(
                        use_case, eid,
                        f"{ENTITY_TYPES[node_type]} {eid} was not found in the knowledge graph."))
                    continue
                indicators.append(_indicator(use_case, node_type, eid, score))
                for rel_type, rel_id in _related(node_type, score):
                    use(rel_type, rel_id)

    return indicators, list(used.values())


def build_message(indicators, entities, kg_version, start_ms,
                  end_ms=None, request_id=None, tool_name=TOOL_NAME):
    """Wrap the indicators in the contract's response envelope."""
    return {
        "requestID": request_id or _uid(),
        "responseID": _uid(),
        "toolName": tool_name,
        "analysis": {
            "startTime": int(start_ms),
            "endTime": int(end_ms if end_ms is not None else now_ms()),
            "indicators": indicators,
        },
        "source": {"kg": {"versionId": kg_version, "entities": entities}},
    }
