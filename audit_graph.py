"""Audit each pilot's Neo4j graph against the CEDAR data model and save every
inconsistency found, per pilot.

The data model is data_model/CEDAR__Data_Model_FinalVersion_Explanation.pdf
(6 entities, 39 attributes, 7 relations), transcribed in DATA_MODEL and
RELATIONSHIPS below. On top of it, the rules the risk scoring relies on:

    every bid      is submitted by exactly ONE organization        (SUBMITS)
                   for exactly ONE tender                          (SUBMITTED_FOR)
    every tender   is issued by exactly ONE public entity          (ISSUED_BY / ISSUES)
                   receives at least one bid, and exactly ONE of its bids
                   wins (successful_bid = true), the one its winning_bid names
    an organization submits at most one bid per tender
    ids are present, unique and not placeholders ("[HANDWRITTEN]", "nan")

The graph is read as stored (no unify step), and each node is identified by
its Neo4j elementId, so two nodes sharing a business id are still told apart.
The loader's own display properties (id, title, color, name, type -- lowercase)
are not data-model attributes and are left out of the comparison.

    python audit_graph.py                   # pilots in PILOTS (.env)
    python audit_graph.py it sl
    python audit_graph.py --out audits

Writes, per pilot, in <out>/<pilot>/:
    data_model_comparison.md      readable comparison with the data model: entities,
                                  attributes missing (on how many nodes), properties
                                  not in the model, relationship directions, the
                                  values stored in each boolean attribute, rule checks
    inconsistencies.json / .csv   one row per problem:
                                  severity, check, entity, id, field, value, detail, neo4j_id
    summary.json                  everything above as data: attribute coverage, value
                                  formats and distinct values, relationship directions
and <out>/data_model_comparison.md, the same comparison with the pilots side by side.

severity  error    breaks the data model or one of the rules above
          warning  readable, but not stored the way the data model says
                   (number or boolean stored as text, dd/mm/yyyy date, ...)
          info     allowed in the MVP (an attribute never populated), for coverage
"""

import argparse
import csv
import json
import math
import re
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

from cedar_risk.unify import norm, to_bool, to_float, parse_time
from cedar_risk.utils.cypher_queries import AUDIT_NODES, AUDIT_RELS
from cedar_risk.utils.logger import get_logger, setup_logging, describe_error
from cedar_risk.utils.neo4j_store import run_cypher, pilot_endpoint, pilot_setting, resolve_pilots

log = get_logger(__name__)


# --------------------------------------------------------------------------- #
# The data model (CEDAR Data Model, final version)
# --------------------------------------------------------------------------- #
DATA_MODEL_PDF = "data_model/CEDAR__Data_Model_FinalVersion_Explanation.pdf"

# entity -> id attribute and {attribute: kind};
# kind: "id", "ref" (id of another node), "text", "number", "bool", "datetime"
DATA_MODEL = {
    "Tender": {"id": "id_tender", "attributes": {
        "id_tender": "id", "starting_price": "number", "awarded_price": "number",
        "winning_bid": "ref", "type": "text", "responsible_signee_name": "text",
        "description": "text", "required_amount": "number",
        "time_creation": "datetime", "time_publication": "datetime",
        "time_open": "datetime", "time_deadline": "datetime",
        "time_awarded": "datetime", "time_executed": "datetime",
        "time_closed": "datetime"}},
    "PublicEntity": {"id": "id_public_entity", "attributes": {
        "id_public_entity": "id", "name": "text", "location": "text",
        "department": "text"}},
    "Item": {"id": "id_good_service", "attributes": {
        "id_good_service": "id", "name": "text", "measurement_unit": "text",
        "price_w_taxes": "number", "price_wo_taxes": "number", "type": "text"}},
    "Bid": {"id": "id_bid", "attributes": {
        "id_bid": "id", "time_submission": "datetime", "amount": "number",
        "responsible_signee_name": "text", "successful_bid": "bool",
        "contract_price": "number"}},
    "Organization": {"id": "id_organization", "attributes": {
        "id_organization": "id", "name": "text", "location": "text",
        "responsible_person_name": "text", "type": "text"}},
    "ResponsiblePerson": {"id": None, "attributes": {
        "first_name": "text", "last_name": "text", "time_joined": "datetime"}},
}

# required besides the id: the winner rule cannot be checked without it
REQUIRED = {"Bid": {"successful_bid"}}

# relationship type -> (source entity, target entity)
RELATIONSHIPS = {
    "ISSUED_BY": ("Tender", "PublicEntity"),
    "ISSUES": ("PublicEntity", "Tender"),
    "REQUIRES": ("Tender", "Item"),
    "SUBMITTED_FOR": ("Bid", "Tender"),
    "SUBMITS": ("Organization", "Bid"),
    "INVITES": ("Tender", "Organization"),
    "RESPONSIBLE_OF": ("ResponsiblePerson", "PublicEntity"),
}

# added by the graph loader for display, on every node -- not part of the model
LOADER_PROPERTIES = {"id", "title", "color", "name", "type"}

ENTITY_BY_NORM = {norm(e): e for e in DATA_MODEL}
SEVERITY_ORDER = {"error": 0, "warning": 1, "info": 2}
MAX_VALUES = 20         # distinct values kept per attribute in summary.json (booleans: all)


# --------------------------------------------------------------------------- #
# Reading the graph
# --------------------------------------------------------------------------- #

def fetch(query, endpoint, api_key, attempts=3):
    """run_cypher with a few retries: the gateway sometimes resets the connection."""
    for attempt in range(1, attempts + 1):
        try:
            return run_cypher(query, endpoint, api_key)
        except requests.RequestException as e:
            if attempt == attempts:
                raise
            log.warning("[audit]   query failed (%s), retry %d/%d in %ds",
                        describe_error(e), attempt, attempts - 1, 2 * attempt)
            time.sleep(2 * attempt)


def load_graph(pilot):
    endpoint, api_key = pilot_endpoint(pilot), pilot_setting("GRAPH_API_KEY", pilot)
    if not api_key:
        raise RuntimeError(f"no GRAPH_API_KEY_{pilot.upper()} (or GRAPH_API_KEY) in .env")
    log.info("[audit]   %s: reading %s", pilot, endpoint)
    return fetch(AUDIT_NODES, endpoint, api_key), fetch(AUDIT_RELS, endpoint, api_key)


def model_properties(props):
    """{normalized attribute name: [property names as stored]}, without the
    loader's display properties."""
    found = defaultdict(list)
    for k in props:
        if k not in LOADER_PROPERTIES:
            found[norm(k)].append(k)
    return found


# --------------------------------------------------------------------------- #
# Value formats
# --------------------------------------------------------------------------- #

PLACEHOLDER = re.compile(r"^\s*(\[.*\]|nan|none|null|n/?a|-+|\?+)\s*$", re.IGNORECASE)
NAN_TOKEN = re.compile(r"(^|[_\W])nan($|[_\W])", re.IGNORECASE)
ISO_DATETIME = re.compile(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:?\d{2})?")
ISO_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")


def check_value(kind, v):
    """(severity or None if fine, format name) of one stored value."""
    if isinstance(v, str) and not v.strip():
        return "error", "empty_string"
    if isinstance(v, (list, dict)):
        return "error", f"{type(v).__name__}_value"

    if kind in ("id", "ref"):
        s = str(v)
        if PLACEHOLDER.match(s):
            return "error", "placeholder"
        if NAN_TOKEN.search(s):
            return "warning", "built_from_missing_value"
        return None, "text"

    if kind == "text":
        return None, "text"

    if kind == "bool":
        if isinstance(v, bool):
            return None, "boolean"
        if isinstance(v, (int, float)):
            return ("warning", "number_0_1") if v in (0, 1) else ("error", "number_other")
        s = str(v).strip().lower()
        if s in {"true", "false"}:
            return "warning", "text_true_false"
        if s in {"yes", "no", "y", "n"}:
            return "warning", "text_yes_no"
        if s in {"0", "1"}:
            return "warning", "text_0_1"
        return "error", "unrecognized"

    if kind == "number":
        if isinstance(v, bool):
            return "error", "boolean"
        if isinstance(v, (int, float)):
            return (None, "number") if math.isfinite(v) else ("error", "not_finite")
        s = str(v).strip()
        if re.fullmatch(r"-?\d+(\.\d+)?", s):
            return "warning", "text_number"
        if re.fullmatch(r"-?\d+,\d+", s):
            return "warning", "text_decimal_comma"
        if to_float(s) is not None:
            return "warning", "text_number_with_separators"
        return "error", "unparseable"

    if kind == "datetime":
        if not isinstance(v, str):
            return (None, "datetime") if hasattr(v, "year") else ("error", "not_a_date")
        s = v.strip()
        if ISO_DATETIME.fullmatch(s):
            return None, "iso_datetime"
        if ISO_DATE.fullmatch(s):
            return None, "iso_date"
        if parse_time(s) is not None:
            return "warning", "non_iso_date"          # e.g. dd/mm/yyyy
        return "error", "unparseable"

    raise ValueError(f"unknown kind {kind!r}")


def value_key(v):
    """Hashable (type name, value) -- keeps "True" (str), True (bool) and 1 (int) apart."""
    hashable = v if isinstance(v, (str, int, float, bool)) else json.dumps(v, default=str)
    return type(v).__name__, hashable


def value_rows(kind, values, limit=None):
    """[{value, type, count, format, status}] of the distinct stored values,
    most frequent first; status = ok / warning / error against the data model."""
    rows = []
    for (tname, v), count in values.most_common(limit):
        severity, fmt = check_value(kind, v)
        rows.append({"value": v, "type": tname, "count": count,
                     "format": fmt, "status": severity or "ok"})
    return rows


# --------------------------------------------------------------------------- #
# The audit
# --------------------------------------------------------------------------- #

class Audit:
    def __init__(self, pilot, nodes, rels):
        self.pilot = pilot
        self.issues = []
        self.nodes = {}                       # elementId -> node
        for n in nodes:
            labels = n.get("labels") or []
            raw = labels[0] if labels else None
            entity = ENTITY_BY_NORM.get(norm(raw or ""))
            props = n.get("props") or {}
            self.nodes[n["key"]] = {
                "key": n["key"], "labels": labels, "raw_label": raw,
                "entity": entity, "props": props, "found": model_properties(props),
                "id": self._business_id(entity, props),
            }
        self.rels = rels
        self.out = defaultdict(list)          # (node, rel type) -> [target nodes]
        self.inc = defaultdict(list)          # (node, rel type) -> [source nodes]
        for r in rels:
            self.out[(r["src"], r["rel"])].append(r["dst"])
            self.inc[(r["dst"], r["rel"])].append(r["src"])

    # ---- helpers ----------------------------------------------------------
    @staticmethod
    def _business_id(entity, props):
        id_attr = DATA_MODEL[entity]["id"] if entity else None
        if id_attr is None:
            return None
        for k, v in props.items():
            if k not in LOADER_PROPERTIES and norm(k) == norm(id_attr) and v is not None:
                return str(v)
        return None

    def value(self, node, attr):
        keys = node["found"].get(norm(attr))
        return node["props"][keys[0]] if keys else None

    def ref(self, key):
        """Readable reference to a node: its business id, else its display id."""
        n = self.nodes.get(key)
        if n is None:
            return f"<missing node {key}>"
        return n["id"] or n["props"].get("id") or key

    def of(self, entity):
        return [n for n in self.nodes.values() if n["entity"] == entity]

    def add(self, severity, check, node=None, field=None, value=None, detail="",
            entity=None, id=None):
        self.issues.append({
            "severity": severity, "check": check,
            "entity": entity or (node and (node["entity"] or node["raw_label"])),
            "id": id if id is not None else (node and self.ref(node["key"])),
            "field": field, "value": value, "detail": detail,
            "neo4j_id": node["key"] if node else None,
        })

    # ---- 1. labels and attributes -----------------------------------------
    def check_attributes(self):
        report = {}
        unknown_labels = Counter()
        for n in self.nodes.values():
            if len(n["labels"]) > 1:
                self.add("warning", "multiple_labels", n, detail=f"labels {n['labels']}")
            if n["entity"] is None:
                unknown_labels[n["raw_label"]] += 1
        for label, count in unknown_labels.items():
            self.add("warning", "label_not_in_data_model", entity=label, id="",
                     detail=f"{count} nodes with a label the data model does not define")
        self.unknown_labels = dict(unknown_labels)

        for entity, spec in DATA_MODEL.items():
            nodes = self.of(entity)
            attrs = spec["attributes"]
            if not nodes:
                self.add("info", "entity_absent", entity=entity, id="",
                         detail="no node of this entity in the graph")
            fields = {}
            for attr, kind in attrs.items():
                required = attr == spec["id"] or attr in REQUIRED.get(entity, ())
                formats, spellings, values, present = Counter(), Counter(), Counter(), 0
                format_status, examples = {}, defaultdict(list)
                for n in nodes:
                    keys = n["found"].get(norm(attr), [])
                    spellings.update(keys)
                    if len(keys) > 1:
                        self.add("warning", "attribute_stored_twice", n, attr, keys,
                                 "the same attribute under several spellings")
                    v = n["props"][keys[0]] if keys else None
                    if v is None:
                        if required:
                            self.add("error", "missing_required_attribute", n, attr)
                        continue
                    present += 1
                    values[value_key(v)] += 1
                    severity, fmt = check_value(kind, v)
                    formats[fmt] += 1
                    format_status[fmt] = severity or "ok"
                    if len(examples[fmt]) < 3 and v not in examples[fmt]:
                        examples[fmt].append(v)
                    if severity:
                        self.add(severity, f"{kind}_{fmt}", n, attr, v,
                                 f"data model: {kind}")
                missing = len(nodes) - present
                if missing and not required:     # required ones are reported per node
                    self.add("info", "attribute_missing", entity=entity, id="", field=attr,
                             detail=f"missing on {missing} of {len(nodes)} nodes"
                                    + (" (never populated)" if not present else ""))
                fields[attr] = {
                    "kind": kind, "required": required,
                    "present": present, "missing": missing,
                    "formats": dict(formats.most_common()), "format_status": format_status,
                    "examples": dict(examples),
                    "distinct_values": len(values),
                    "values": value_rows(kind, values,
                                         limit=None if kind == "bool" else MAX_VALUES),
                    "spellings": dict(spellings.most_common()),
                }

            known = {norm(a) for a in attrs}
            extra = Counter(k for n in nodes for k in n["props"]
                            if k not in LOADER_PROPERTIES and norm(k) not in known)
            for prop, count in extra.most_common():
                self.add("warning", "property_not_in_data_model", entity=entity, id="",
                         field=prop, detail=f"on {count} of {len(nodes)} nodes")
            report[entity] = {
                "count": len(nodes),
                "labels_seen": dict(Counter(n["raw_label"] for n in nodes)),
                "attributes": fields,
                "properties_not_in_data_model": dict(extra.most_common()),
                "loader_properties": dict(Counter(
                    k for n in nodes for k in n["props"] if k in LOADER_PROPERTIES)),
            }
        return report

    # ---- 2. ids ------------------------------------------------------------
    def check_ids(self):
        by_entity = defaultdict(lambda: defaultdict(list))
        across = defaultdict(set)
        for n in self.nodes.values():
            if n["id"] is not None:
                by_entity[n["entity"]][n["id"]].append(n)
                across[n["id"]].add(n["entity"])
        for entity, ids in by_entity.items():
            for bid, nodes in ids.items():
                if len(nodes) > 1:
                    for n in nodes:
                        self.add("error", "duplicate_id", n, DATA_MODEL[entity]["id"], bid,
                                 f"{len(nodes)} {entity} nodes share this id")
        for bid, entities in across.items():
            if len(entities) > 1:
                for entity in sorted(entities):
                    for n in by_entity[entity][bid]:
                        self.add("error", "id_shared_across_entities", n,
                                 DATA_MODEL[entity]["id"], bid,
                                 f"also used by {sorted(entities - {entity})}: the risk "
                                 f"pipeline keys nodes by id, so these nodes are merged")

    # ---- 3. relationships --------------------------------------------------
    def check_relationships(self):
        report = {}
        by_type = defaultdict(list)
        for r in self.rels:
            by_type[r["rel"]].append(r)
        for rel_type in sorted(set(by_type) | set(RELATIONSHIPS)):
            rows = by_type.get(rel_type, [])
            expected = RELATIONSHIPS.get(rel_type)
            pairs = Counter()
            if not rows:
                self.add("info", "relationship_absent", entity=rel_type, id="",
                         detail="no relationship of this type in the graph")
            elif expected is None:
                self.add("warning", "relationship_not_in_data_model", entity=rel_type, id="",
                         detail=f"{len(rows)} relationships")
            for r in rows:
                src, dst = self.nodes[r["src"]], self.nodes[r["dst"]]
                pair = (src["entity"] or src["raw_label"], dst["entity"] or dst["raw_label"])
                direction = self.direction(expected, pair)
                pairs[(pair, direction)] += 1
                if direction == "reversed":
                    self.add("error", "relationship_reversed", src, value=self.ref(dst["key"]),
                             detail=f"{rel_type} goes {pair[0]} -> {pair[1]}, the data "
                                    f"model says {expected[0]} -> {expected[1]}")
                elif direction == "wrong_entities":
                    self.add("error", "relationship_wrong_entities", src,
                             value=self.ref(dst["key"]),
                             detail=f"{rel_type} connects {pair[0]} -> {pair[1]}, the data "
                                    f"model says {expected[0]} -> {expected[1]}")
            for (s, d, c) in [(k[0], k[1], v) for k, v in
                              Counter((r["src"], r["dst"]) for r in rows).items() if v > 1]:
                self.add("warning", "duplicate_relationship", self.nodes.get(s),
                         detail=f"{c} identical {rel_type} relationships to {self.ref(d)}")
            report[rel_type] = {
                "count": len(rows), "in_data_model": expected is not None,
                "data_model": f"{expected[0]} -> {expected[1]}" if expected else None,
                "seen": [{"direction": f"{s} -> {d}", "status": status, "count": c}
                         for ((s, d), status), c in pairs.most_common()],
            }
        return report

    @staticmethod
    def direction(expected, pair):
        if expected is None:
            return "not_in_data_model"
        if pair == expected:
            return "as_in_data_model"
        if pair == expected[::-1]:
            return "reversed"
        return "wrong_entities"

    # ---- 4. structure: the rules the risk scoring relies on ---------------
    def check_structure(self):
        stats = Counter()

        for bid in self.of("Bid"):
            orgs = sorted(set(self.inc[(bid["key"], "SUBMITS")]))
            tenders = sorted(set(self.out[(bid["key"], "SUBMITTED_FOR")]))
            if not orgs:
                self.add("error", "bid_without_organization", bid,
                         detail="no organization SUBMITS this bid")
            elif len(orgs) > 1:
                self.add("error", "bid_with_multiple_organizations", bid,
                         value=[self.ref(o) for o in orgs],
                         detail=f"submitted by {len(orgs)} organizations, the model allows one")
            if not tenders:
                self.add("error", "bid_without_tender", bid,
                         detail="this bid is not SUBMITTED_FOR any tender")
            elif len(tenders) > 1:
                self.add("error", "bid_for_multiple_tenders", bid,
                         value=[self.ref(t) for t in tenders],
                         detail=f"submitted for {len(tenders)} tenders")
            stats["bids_with_one_organization"] += len(orgs) == 1
            stats["bids_with_one_tender"] += len(tenders) == 1

        for tender in self.of("Tender"):
            k = tender["key"]
            issued_by = set(self.out[(k, "ISSUED_BY")])
            issues = set(self.inc[(k, "ISSUES")])
            entities = issued_by | issues
            if not entities:
                self.add("error", "tender_without_public_entity", tender,
                         detail="no ISSUED_BY / ISSUES relationship")
            elif len(entities) > 1:
                self.add("error", "tender_with_multiple_public_entities", tender,
                         value=[self.ref(e) for e in sorted(entities)],
                         detail=f"issued by {len(entities)} public entities, the model allows one")
            if issued_by != issues:
                self.add("warning", "issues_and_issued_by_disagree", tender,
                         detail=f"ISSUED_BY -> {sorted(self.ref(e) for e in issued_by)}, "
                                f"ISSUES <- {sorted(self.ref(e) for e in issues)}")
            if not self.out[(k, "REQUIRES")]:
                self.add("warning", "tender_without_item", tender,
                         detail="no REQUIRES relationship to an item")

            bids = sorted(set(self.inc[(k, "SUBMITTED_FOR")]))
            flags = {b: self.value(self.nodes[b], "successful_bid") for b in bids}
            winners = [b for b, v in flags.items() if to_bool(v)]   # only an explicit true/yes/1
            if not bids:
                self.add("error", "tender_without_bids", tender,
                         detail="no bid is SUBMITTED_FOR this tender, so it has no winner")
            elif not winners:
                self.add("error", "tender_without_winning_bid", tender,
                         value={self.ref(b): v for b, v in flags.items()},
                         detail=f"none of its {len(bids)} bids has successful_bid = true")
            elif len(winners) > 1:
                self.add("error", "tender_with_multiple_winning_bids", tender,
                         value=[self.ref(b) for b in winners],
                         detail=f"{len(winners)} of its bids have successful_bid = true")

            wb = self.value(tender, "winning_bid")
            if wb is not None and bids:
                ids = {self.nodes[b]["id"] for b in bids}
                if str(wb) not in ids:
                    elsewhere = any(n["id"] == str(wb) for n in self.of("Bid"))
                    self.add("error", "winning_bid_not_among_tender_bids", tender,
                             "winning_bid", wb,
                             "names a bid submitted for ANOTHER tender" if elsewhere
                             else "names no bid in the graph")
                elif len(winners) == 1 and self.nodes[winners[0]]["id"] != str(wb):
                    self.add("error", "winning_bid_disagrees_with_successful_bid", tender,
                             "winning_bid", wb,
                             f"successful_bid = true is on {self.ref(winners[0])}")
            stats["tenders_with_one_public_entity"] += len(entities) == 1
            stats["tenders_with_exactly_one_winner"] += len(winners) == 1

        for org in self.of("Organization"):
            per_tender = defaultdict(list)
            for bid in self.out[(org["key"], "SUBMITS")]:
                for t in self.out[(bid, "SUBMITTED_FOR")]:
                    per_tender[t].append(self.ref(bid))
            for t, bids in per_tender.items():
                if len(set(bids)) > 1:
                    self.add("warning", "organization_multiple_bids_same_tender", org,
                             value=sorted(set(bids)),
                             detail=f"{len(set(bids))} bids for tender {self.ref(t)}")

        for person in self.of("ResponsiblePerson"):
            if not self.out[(person["key"], "RESPONSIBLE_OF")]:
                self.add("warning", "responsible_person_without_public_entity", person,
                         detail="no RESPONSIBLE_OF relationship")
        return dict(stats)

    def run(self):
        entities = self.check_attributes()
        self.check_ids()
        relationships = self.check_relationships()
        structure = self.check_structure()
        self.issues.sort(key=lambda i: (SEVERITY_ORDER[i["severity"]], i["check"],
                                        str(i["entity"]), str(i["id"])))
        return {
            "pilot": self.pilot,
            "nodes": len(self.nodes), "relationships": len(self.rels),
            "issues_by_severity": dict(Counter(i["severity"] for i in self.issues)),
            "issues_by_check": {f"{s}: {c}": n for (s, c), n in Counter(
                (i["severity"], i["check"]) for i in self.issues).most_common()},
            "structure": structure,
            "labels_not_in_data_model": self.unknown_labels,
            "entities": entities, "relationship_types": relationships,
        }


# --------------------------------------------------------------------------- #
# Readable comparison (Markdown), one column per pilot
# --------------------------------------------------------------------------- #

MARK = {"ok": "", "warning": "⚠ ", "error": "✗ "}
RULES = [
    ("Each bid is submitted by exactly one organization", "bids_with_one_organization", "Bid"),
    ("Each bid is submitted for exactly one tender", "bids_with_one_tender", "Bid"),
    ("Each tender is issued by exactly one public entity",
     "tenders_with_one_public_entity", "Tender"),
    ("Each tender has exactly one winning bid among its bids",
     "tenders_with_exactly_one_winner", "Tender"),
]


def _table(header, rows):
    out = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    out += ["| " + " | ".join(str(c).replace("|", "\\|").replace("\n", " ") for c in r) + " |"
            for r in rows]
    return "\n".join(out) + "\n"


def _show(v, width=50):
    s = str(v)
    return f"`{s[:width]}…`" if len(s) > width else f"`{s}`"


def _attribute_cell(f, count):
    if count == 0:
        return "–"
    p = f["present"]
    presence = (f"{p}/{count}" if p == count else
                f"**missing on all** (0/{count})" if p == 0 else
                f"**missing on {count - p}** ({p}/{count})")
    formats = ", ".join(f"{MARK[f['format_status'][fmt]]}{fmt} {n}"
                        for fmt, n in f["formats"].items())
    return presence + (f"<br>{formats}" if formats else "")


def comparison_markdown(summaries):
    pilots = [s["pilot"] for s in summaries]
    by = {s["pilot"]: s for s in summaries}
    md = [f"# Pilot graphs vs the CEDAR data model\n",
          f"Data model: `{DATA_MODEL_PDF}` (6 entities, 39 attributes, 7 relationships).  ",
          f"Generated by `audit_graph.py` on {datetime.now():%Y-%m-%d %H:%M}.  ",
          "Pilots: " + ", ".join(f"**{p}** ({by[p]['nodes']} nodes, "
                                 f"{by[p]['relationships']} relationships)" for p in pilots),
          "",
          "Attribute names are matched ignoring case and underscores (`ID_Tender` = "
          "`id_tender`). The loader's display properties (`id`, `title`, `color`, "
          "`name`, `type` in lowercase) are not part of the model and are ignored.  ",
          "Marks: ✗ = breaks the data model, ⚠ = readable but not stored as the model "
          "says (e.g. a number stored as text).\n"]

    # 1. entities
    md.append("## 1. Entities\n")
    rows = []
    for entity in DATA_MODEL:
        cells = []
        for p in pilots:
            e = by[p]["entities"][entity]
            labels = ", ".join(f"`{l}`" for l in e["labels_seen"])
            cells.append(f"{e['count']} ({labels})" if e["count"] else "**absent**")
        rows.append([entity] + cells)
    for p in pilots:
        for label, count in by[p]["labels_not_in_data_model"].items():
            rows.append([f"✗ `{label}` (not in the model)"]
                        + [count if q == p else "" for q in pilots])
    md.append(_table(["Entity"] + [f"{p}: nodes (label)" for p in pilots], rows))

    # 2. attributes
    md.append("## 2. Attributes of each entity\n")
    md.append("Present on how many nodes, and in which formats.\n")
    for entity, spec in DATA_MODEL.items():
        md.append(f"### {entity}\n")
        rows = []
        for attr, kind in spec["attributes"].items():
            req = " (required)" if by[pilots[0]]["entities"][entity]["attributes"][attr][
                "required"] else ""
            rows.append([f"`{attr}`{req}", kind] + [
                _attribute_cell(by[p]["entities"][entity]["attributes"][attr],
                                by[p]["entities"][entity]["count"]) for p in pilots])
        md.append(_table(["Attribute", "Model type"] + pilots, rows))
        extra = sorted({k for p in pilots
                        for k in by[p]["entities"][entity]["properties_not_in_data_model"]})
        if extra:
            md.append(f"Properties on {entity} nodes that are **not in the data model**:\n")
            md.append(_table(["Property"] + pilots, [[f"`{k}`"] + [
                (f"{by[p]['entities'][entity]['properties_not_in_data_model'][k]}"
                 f"/{by[p]['entities'][entity]['count']}"
                 if k in by[p]["entities"][entity]["properties_not_in_data_model"] else "–")
                for p in pilots] for k in extra]))

    # 3. relationships
    md.append("## 3. Relationships and their direction\n")
    rows = []
    for rel in sorted({r for p in pilots for r in by[p]["relationship_types"]}):
        model = next((by[p]["relationship_types"][rel]["data_model"] for p in pilots
                      if rel in by[p]["relationship_types"]), None)
        cells = []
        for p in pilots:
            r = by[p]["relationship_types"].get(rel)
            if not r or not r["count"]:
                cells.append("absent")
                continue
            mark = {"as_in_data_model": "✓", "reversed": "✗ reversed",
                    "wrong_entities": "✗ wrong entities", "not_in_data_model": "✗ not in model"}
            cells.append("<br>".join(f"{mark[x['status']]} {x['direction']}: {x['count']}"
                                     for x in r["seen"]))
        rows.append([f"`{rel}`", model or "**not in the model**"] + cells)
    md.append(_table(["Relationship", "Data model"] + pilots, rows))

    # 4. booleans
    md.append("## 4. Boolean attributes: values actually stored\n")
    md.append("The model says boolean (`true` / `false`). Every distinct value found, "
              "with its stored type:\n")
    for entity, spec in DATA_MODEL.items():
        for attr, kind in spec["attributes"].items():
            if kind != "bool":
                continue
            md.append(f"### {entity}.{attr}\n")
            counts, status = defaultdict(dict), {}
            for p in pilots:
                f = by[p]["entities"][entity]["attributes"][attr]
                for v in f["values"]:
                    key = (_show(v["value"]), v["type"])
                    counts[key][p] = v["count"]
                    status[key] = f"{MARK[v['status']]}{v['status']} ({v['format']})"
                if f["missing"]:
                    counts[("*(missing)*", "–")][p] = f["missing"]
                    status[("*(missing)*", "–")] = "✗ missing"
            md.append(_table(["Value", "Stored as", "Status"] + pilots, [
                [v, t, status[(v, t)]] + [counts[(v, t)].get(p, "–") for p in pilots]
                for (v, t) in counts]))

    # 5. other format deviations
    md.append("## 5. Other values not stored as the model says\n")
    rows = []
    for entity, spec in DATA_MODEL.items():
        for attr, kind in spec["attributes"].items():
            if kind == "bool":
                continue
            for p in pilots:
                f = by[p]["entities"][entity]["attributes"][attr]
                for fmt, n in f["formats"].items():
                    if f["format_status"][fmt] != "ok":
                        rows.append([f"{entity}.`{attr}`", kind, p,
                                     f"{MARK[f['format_status'][fmt]]}{fmt}", n,
                                     ", ".join(_show(x, 30) for x in f["examples"][fmt])])
    md.append(_table(["Attribute", "Model type", "Pilot", "Stored as", "Nodes", "Examples"],
                     rows) if rows else "None.\n")

    # 6. rules
    md.append("## 6. Structural rules\n")
    md.append(_table(["Rule"] + pilots, [[rule] + [
        f"{by[p]['structure'].get(key, 0)}/{by[p]['entities'][entity]['count']}"
        for p in pilots] for rule, key, entity in RULES]))

    # 7. all checks
    md.append("## 7. Number of issues per check\n")
    md.append("Each one is listed with its node in `<pilot>/inconsistencies.csv`.\n")
    keys = []
    for p in pilots:
        keys += [k for k in by[p]["issues_by_check"] if k not in keys]
    keys.sort(key=lambda k: (SEVERITY_ORDER[k.split(": ")[0]], k))
    md.append(_table(["Severity", "Check"] + pilots, [
        k.split(": ") + [by[p]["issues_by_check"].get(k, "–") for p in pilots] for k in keys]))
    return "\n".join(md)


# --------------------------------------------------------------------------- #
# Saving
# --------------------------------------------------------------------------- #

CSV_COLUMNS = ["severity", "check", "entity", "id", "field", "value", "detail", "neo4j_id"]


def save(pilot, summary, issues, out_dir):
    folder = Path(out_dir) / pilot
    folder.mkdir(parents=True, exist_ok=True)
    dump = lambda obj: json.dumps(obj, indent=1, ensure_ascii=False, default=str)
    (folder / "summary.json").write_text(dump(summary), encoding="utf-8")
    (folder / "data_model_comparison.md").write_text(comparison_markdown([summary]),
                                                     encoding="utf-8")
    (folder / "inconsistencies.json").write_text(dump(issues), encoding="utf-8")
    with open(folder / "inconsistencies.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        w.writeheader()
        for i in issues:
            w.writerow({k: json.dumps(v, ensure_ascii=False, default=str)
                        if isinstance(v, (list, dict)) else v for k, v in i.items()})
    return folder


def audit_pilot(pilot, out_dir):
    nodes, rels = load_graph(pilot)
    audit = Audit(pilot, nodes, rels)
    summary = audit.run()
    folder = save(pilot, summary, audit.issues, out_dir)

    sev = summary["issues_by_severity"]
    log.info("[audit]   %s: %d nodes, %d relationships -> %d errors, %d warnings, %d info",
             pilot, summary["nodes"], summary["relationships"],
             sev.get("error", 0), sev.get("warning", 0), sev.get("info", 0))
    for check, n in summary["issues_by_check"].items():
        if not check.startswith("info"):
            log.info("[audit]   %s:   %5d  %s", pilot, n, check)
    log.info("[audit]   %s: saved to %s/", pilot, folder)
    return summary


def main():
    load_dotenv()
    setup_logging()
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("pilots", nargs="*", help="e.g. it sl (default: PILOTS in .env)")
    ap.add_argument("--out", default="audits",
                    help="folder for the reports, one sub-folder per pilot (default: audits/)")
    args = ap.parse_args()

    failed, summaries = [], []
    for pilot in resolve_pilots(args.pilots):
        try:
            summaries.append(audit_pilot(pilot, args.out))
        except Exception as e:
            failed.append(pilot)
            log.exception("[audit]   %s: audit failed: %s", pilot, describe_error(e))
    if summaries:
        path = Path(args.out) / "data_model_comparison.md"
        path.write_text(comparison_markdown(summaries), encoding="utf-8")
        log.info("[audit]   pilots side by side: %s", path)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
