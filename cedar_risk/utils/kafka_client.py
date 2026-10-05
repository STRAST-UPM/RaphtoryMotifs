"""Kafka side of the CEDAR risk-scoring tool, following the CEDAR Kafka
message contract.

Flow
  1. The UI (or send_request.py) publishes a request on the pilot's request
     topic (it_analyse, sl_analyse).
  2. main.py runs the requested indicators on the requested entities.
  3. main.py publishes ONE response on the pilot's results topic
     (it_results_available, sl_results_available).
  4. The UI matches the response to its request using requestID.

Request (UI -> tool)
    {"issuer": "ui",
     "requestID": "5f1c2e0a-...",
     "entities": [{"entityType": "tender", "entityId": "B0835BC998"}],
     "indicators": ["UPM_RAPHTORY_TENDER_RISK"],   # optional; empty/missing = all of ours
     "kg": {"versionId": "v1.1"},
     "accounting": {}, "status": {}}

  Extension to the contract (ours):
    entityType  "tender" | "organization" | "publicEntity" | "all"
                ("bid" is accepted, but this tool has no bid indicator)
    entityId    an ID as in the knowledge graph, or "all" = every entity of
                that entityType. {"entityType": "all", "entityId": "all"}
                = every tender, organization and public entity of the graph.

Response (tool -> UI)
    {"requestID": <copied>, "responseID", "toolName",
     "analysis": {"startTime": <request published, ms>, "endTime": <ms>,
                  "indicators": [{"id", "indicator_id", "entityID", "risk_level",
                                  "additional_info": [{"id", "title", "type", "value",
                                       "units"?, "description"?, "risk_level"?, "range"?}]}]},
     "source": {"kg": {"versionId": <copied>,
                       "entities": [{"entityType", "entityId"}]}}}

Contract rules applied here: an invalid request is logged and skipped (no
response); a request naming none of this tool's indicators gets no response
(the request topics are shared with other tools); optional fields are left
out, never null; every indicator and additional_info item gets its own UUID;
a requested entity that cannot be analysed is still answered, with value
"Analysis Failed", risk_level 0.0 and the reason in description.
"""

import json
import os
import re
import time
import uuid

import kafka
from kafka.serializer import Serializer


TOOL_NAME = "UPM_RAPHTORY"
ALL = "all"

# indicator_id for each scored node type.
# TODO: replace with the ids assigned to UPM in the "Indicators map" excel.
INDICATOR_IDS = {
    "Organization": "UPM_RAPHTORY_ORG_COLLUSION",
    "PublicEntity": "UPM_RAPHTORY_ENTITY_FAVORITISM",
    "Tender": "UPM_RAPHTORY_TENDER_RISK",
}
NODE_TYPES = tuple(INDICATOR_IDS)

# entityType used in requests and in source.kg.entities[]. The contract only
# defines "tender" (and "bid"); the other two are ours -- confirm with the UI team.
ENTITY_TYPES = {
    "Organization": "organization",
    "PublicEntity": "publicEntity",
    "Tender": "tender",
}

# entityType of a request (case-insensitive) -> node type; "bid" has no indicator here
_REQUEST_TYPES = {"tender": "Tender", "organization": "Organization",
                  "publicentity": "PublicEntity", "public_entity": "PublicEntity",
                  "bid": None}

SCORE_RANGE = {"from": 0.0, "to": 1.0}

# Items measured on an organization group, shown ONLY when the node has one:
#   (title, value key, risk key, units, description)
# value      -> the measurement itself (e.g. number of tenders, success rate)
# risk_level -> its 0-1 risk score
# units=None means the value is already on the 0-1 scale.
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
                   "Risk of the issuing public entity (0 while the tender is open)."),
}


# --------------------------------------------------------------------------- #
# Settings and topics (.env)
# --------------------------------------------------------------------------- #

_TOPIC_SUFFIX = {"request": "analyse", "results": "results_available"}


def _split(value):
    """'it,sl' / 'it sl' -> ['it', 'sl']"""
    return [v for v in re.split(r"[,\s]+", value or "") if v]


def pilot_topic(pilot, kind):
    """Request ('request') or results ('results') topic of one pilot.

    In order of precedence:
      1. KAFKA_<KIND>_TOPIC_<PILOT>               KAFKA_REQUEST_TOPIC_IT=it_analyse
      2. the entry of KAFKA_<KIND>_TOPICS starting with "<pilot>_"
             KAFKA_REQUEST_TOPICS=it_analyse,sl_analyse
             KAFKA_RESULTS_TOPICS=it_results_available,sl_results_available
      3. the contract's naming: <pilot>_analyse / <pilot>_results_available
    """
    env = f"KAFKA_{kind.upper()}_TOPIC"
    override = os.getenv(f"{env}_{pilot.upper()}")
    if override:
        return override
    for topic in _split(os.getenv(f"{env}S")):
        if topic.lower().startswith(f"{pilot.lower()}_"):
            return topic
    return f"{pilot}_{_TOPIC_SUFFIX[kind]}"


def kafka_settings():
    """Broker connection settings from the environment (call load_dotenv first).
    No KAFKA_USERNAME -> plain connection (local broker)."""
    return {
        "bootstrap": os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
        "username": os.getenv("KAFKA_USERNAME") or None,
        "password": os.getenv("KAFKA_PASSWORD") or None,
        "security_protocol": os.getenv("KAFKA_SECURITY_PROTOCOL") or None,
        "sasl_mechanism": os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
    }


# --------------------------------------------------------------------------- #
# Producer / consumers
# --------------------------------------------------------------------------- #

MAX_MESSAGE_BYTES = 10 * 1024 * 1024    # a response for "all" holds the whole graph


def _connection(bootstrap="localhost:9092", username=None, password=None,
                security_protocol=None, sasl_mechanism="PLAIN"):
    if security_protocol is None:
        security_protocol = "SASL_PLAINTEXT" if username else "PLAINTEXT"
    conn = {"bootstrap_servers": bootstrap, "security_protocol": security_protocol}
    if security_protocol.upper().startswith("SASL"):
        conn.update(sasl_mechanism=sasl_mechanism,
                    sasl_plain_username=username,
                    sasl_plain_password=password)
    return conn


class JsonSerializer(Serializer):
    """dict -> UTF-8 JSON bytes. default=str keeps stray numpy/pandas types
    serialisable. Called as serialize(topic, headers, data) by kafka-python 3.x
    and serialize(topic, data) by 2.x -- the data is always the last argument."""

    def serialize(self, topic, *args):
        return json.dumps(args[-1], ensure_ascii=False, default=str).encode("utf-8")


def make_producer(max_request_size=MAX_MESSAGE_BYTES, **settings):
    """Producer for requests and responses (gzip-compressed JSON)."""
    return kafka.KafkaProducer(
        **_connection(**settings),
        compression_type="gzip",
        max_request_size=max_request_size,
        value_serializer=JsonSerializer(),
    )


def make_consumer(topics, group_id, auto_offset_reset="latest",
                  max_poll_interval_ms=30 * 60 * 1000, **settings):
    """Consumer of the request topics, for main.py.

    Values come back as raw bytes (parse_request decodes them, so a malformed
    message is logged instead of crashing the loop). One record per poll and
    no auto-commit: the caller commits after each request is answered. The
    long poll interval leaves time for a full graph analysis between polls.
    `group_id` must be this tool's own: the request topics are shared.
    """
    return kafka.KafkaConsumer(
        *topics,
        **_connection(**settings),
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset=auto_offset_reset,
        max_poll_records=1,
        max_poll_interval_ms=max_poll_interval_ms,
        max_partition_fetch_bytes=MAX_MESSAGE_BYTES,
    )


def tail_consumer(topics, **settings):
    """Consumer already positioned at the END of `topics` -- create it BEFORE
    sending a request, so the response cannot be missed (send_request.py)."""
    consumer = kafka.KafkaConsumer(**_connection(**settings), group_id=None,
                                   enable_auto_commit=False,
                                   max_partition_fetch_bytes=MAX_MESSAGE_BYTES)
    consumer.topics()                                   # fetch the metadata
    parts = []
    for topic in topics:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            consumer.close()
            raise ValueError(f"topic {topic!r} does not exist on the broker")
        parts += [kafka.TopicPartition(topic, p) for p in partitions]
    consumer.assign(parts)
    consumer.seek_to_end(*parts)
    for tp in parts:
        consumer.position(tp)            # resolve the end offset now, not at the first poll
    return consumer


def publish(producer, topic, message, timeout=30):
    """Send one message and wait for the broker's ack (raises if it is rejected,
    e.g. MessageSizeTooLargeError)."""
    producer.send(topic, value=message).get(timeout=timeout)
    return message


# --------------------------------------------------------------------------- #
# Request (UI -> tool)
# --------------------------------------------------------------------------- #

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
    """What a valid request asks THIS tool for: {node_type: [ids] or ALL}.

    None when the request names indicators and none of them is ours (it is
    for another tool). Entities whose type has no requested indicator of ours
    ("bid", or a type whose indicator was not asked for) are left out.
    """
    by_indicator = {v: k for k, v in INDICATOR_IDS.items()}
    named = request.get("indicators") or []
    if named:
        active = {by_indicator[i] for i in named if i in by_indicator}
        if not active:
            return None
    else:
        active = set(NODE_TYPES)

    plan = {}
    for e in request["entities"]:
        etype = e["entityType"].strip().lower()
        eid = e["entityId"].strip()
        types = NODE_TYPES if etype == ALL else (_REQUEST_TYPES.get(etype),)
        for node_type in types:
            if node_type not in active:
                continue
            if eid.lower() == ALL:
                plan[node_type] = ALL
            elif plan.get(node_type) != ALL:
                ids = plan.setdefault(node_type, [])
                if eid not in ids:
                    ids.append(eid)
    return plan


def describe_plan(plan):
    """'all Tender, 2 Organization' -- for the logs."""
    return ", ".join(f"{'all' if ids == ALL else len(ids)} {node_type}"
                     for node_type, ids in plan.items()) or "nothing for this tool"

# --------------------------------------------------------------------------- #
# Message pieces
# --------------------------------------------------------------------------- #

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


def _finding(score):
    """The text from descriptions.py, only when the node is above the threshold."""
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


def _tender_info(score):
    items = _finding(score)
    items += _score_items(score.get("components") or {}, TENDER_COMPONENTS)

    ex = score.get("explanation") or {}
    if ex.get("bidding_stage") is not None:
        items.append(info_item("Bidding stage",
                               "open" if ex["bidding_stage"] else "awarded", "text"))
    if ex.get("participants"):
        items.append(info_item("Participants", _names(ex["participants"]), "text"))
    if ex.get("winning_organization"):
        items.append(info_item("Winning organization", str(ex["winning_organization"]), "text"))
    if ex.get("public_entity"):
        items.append(info_item("Public entity", str(ex["public_entity"]), "text"))
    return items


_INFO = {"Organization": _org_info, "PublicEntity": _entity_info, "Tender": _tender_info}


# --------------------------------------------------------------------------- #
# Response (tool -> UI)
# --------------------------------------------------------------------------- #

def _indicator(node_type, node_id, score):
    return {
        "id": _uid(),
        "indicator_id": INDICATOR_IDS[node_type],
        "entityID": str(node_id),
        "risk_level": float(score["overall"]),
        "additional_info": _INFO[node_type](score),
    }


def failed_indicator(node_type, entity_id, reason):
    """Contract: a failed analysis is still published, with value
    "Analysis Failed", risk_level 0.0 and the reason in description."""
    return {
        "id": _uid(),
        "indicator_id": INDICATOR_IDS[node_type],
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

    plan    : from request_plan()
    scores  : {node_type: {node_id: score}} from scoring.py
    failure : reason text when the analysis could not run -> every requested
              entity is answered with "Analysis Failed"
    """
    indicators, used = [], {}

    def use(node_type, node_id):
        key = (ENTITY_TYPES[node_type], str(node_id))
        used.setdefault(key, {"entityType": key[0], "entityId": key[1]})

    for node_type, ids in plan.items():
        if failure is not None:
            for eid in ([ALL] if ids == ALL else ids):
                indicators.append(failed_indicator(node_type, eid, failure))
                use(node_type, eid)
            continue

        by_id = {str(k): v for k, v in ((scores or {}).get(node_type) or {}).items()}
        for eid in (list(by_id) if ids == ALL else ids):
            use(node_type, eid)
            score = by_id.get(eid)
            if score is None:
                indicators.append(failed_indicator(
                    node_type, eid,
                    f"{ENTITY_TYPES[node_type]} {eid} was not found in the knowledge graph."))
                continue
            indicators.append(_indicator(node_type, eid, score))
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