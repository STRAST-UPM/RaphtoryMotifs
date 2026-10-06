"""Audit each pilot's Neo4j graph (it / ua / sl) against the CEDAR data model,
and save every inconsistency: fields that are sometimes a boolean, sometimes
"Yes"/"No", sometimes 0/1, sometimes missing; prices written with a dot or a
comma as the decimal separator; unparseable dates; relationships that don't
connect the labels they're supposed to; duplicate or missing graph ids; and
how many nodes are affected by each.

IMPORTANT: DATA_MODEL and RELATIONSHIP_MODEL below are OUR OWN reconstruction
of the schema, built from the pilot schema dumps and sample nodes seen so far
in this project -- not an authoritative spec from CEDAR. If you have the real
data-model document, correct the two dicts below; everything else runs off
them unchanged.

    python audit_data_model.py                 # it, ua, sl (or PILOTS in .env)
    python audit_data_model.py it sl
    python audit_data_model.py --source graphs  # read <pilot>_raw.json if load_pilots.py
                                                 # already saved it, else the live endpoint
    python audit_data_model.py --out audits

Writes, per pilot, in <out>/:
    <pilot>_data_model_report.json   counts and value-format breakdown per field
    <pilot>_inconsistencies.json     one entry per individual problem found
"""

import argparse
import json
import math
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

from dotenv import load_dotenv

from cedar_risk.unify import norm, to_float, parse_time
from cedar_risk.utils import neo4j_store
from cedar_risk.utils.neo4j_store import pilot_endpoint, pilot_setting, resolve_pilots

# --------------------------------------------------------------------------- #
# Reconstructed data model
# --------------------------------------------------------------------------- #
# label -> {"required": {property: kind}, "optional": {property: kind}}
# kind is one of: "id", "bool", "number", "date", "text"
DATA_MODEL = {
    "tender": {
        "required": {"ID_Tender": "id"},
        "optional": {
            "Time_Publication": "date", "Time_Creation": "date",
            "Time_Open": "date", "Time_Deadline": "date",
            "Winning_Bid": "text", "Awarded_Price": "number",
            "Responsible_Signee_Name": "text",
        },
    },
    "bid": {
        "required": {"ID_Bid": "id", "Successful_Bid": "bool"},
        "optional": {
            "Contract_Price": "number", "Amount": "number",
            "Responsible_Signee_Name": "text", "Time_Submission": "date",
        },
    },
    "organization": {
        "required": {"ID_Organization": "id"},
        "optional": {"Name": "text"},
    },
    "public_entity": {
        "required": {"ID_Public_Entity": "id"},
        "optional": {"Name": "text"},
    },
    "responsible_person": {
        "required": {},
        "optional": {"First_Name": "text", "Last_Name": "text"},
    },
    "item": {
        "required": {},
        "optional": {"ID_Good_Service": "id"},
    },
}

# relationship type -> (expected source label, expected target label)
RELATIONSHIP_MODEL = {
    "SUBMITS": ("organization", "bid"),
    "SUBMITTED_FOR": ("bid", "tender"),
    "ISSUES": ("public_entity", "tender"),
    "ISSUED_BY": ("tender", "public_entity"),
    "REQUIRES": ("tender", "item"),
    "RESPONSIBLE_OF": ("responsible_person", "public_entity"),
}


# --------------------------------------------------------------------------- #
# Loading (prefer the raw dump load_pilots.py already saved; else fetch live)
# --------------------------------------------------------------------------- #

def load_rows(pilot, source_dir):
    raw_path = Path(source_dir) / f"{pilot}_raw.json"
    if raw_path.exists():
        print(f"[{pilot}] reading {raw_path}")
        data = json.loads(raw_path.read_text())
        return data["nodes"], data["rels"]
    endpoint = pilot_endpoint(pilot)
    api_key = pilot_setting("GRAPH_API_KEY", pilot)
    if not api_key:
        raise RuntimeError(f"no {raw_path} and no GRAPH_API_KEY_{pilot.upper()} to fetch from {endpoint}")
    print(f"[{pilot}] no {raw_path}; reading {endpoint}")
    return neo4j_store.read_from_neo4j(endpoint, api_key)


# --------------------------------------------------------------------------- #
# Value classifiers -- bucket every literal representation actually seen
# --------------------------------------------------------------------------- #

def classify_bool(v):
    """Buckets every way "yes/no" has been seen written. "unrecognized" is the
    one that matters most: the rest of the pipeline's to_bool() silently reads
    anything in that bucket as False, with no record that it wasn't really a
    no -- this is where that gets surfaced instead of hidden."""
    if isinstance(v, bool):
        return "python_bool"
    if isinstance(v, (int, float)):
        return "numeric_0_1" if v in (0, 1) else "numeric_other"
    s = str(v).strip()
    if s == "":
        return "empty_string"
    sl = s.lower()
    if sl in {"true", "false"}:
        return "text_true_false"
    if sl in {"yes", "no"}:
        return "text_yes_no"
    if sl in {"y", "n"}:
        return "text_y_n"
    if sl in {"0", "1"}:
        return "text_0_1"
    return "unrecognized"


def classify_number(v):
    if isinstance(v, bool):
        return "unrecognized"
    if isinstance(v, (int, float)):
        return "numeric" if math.isfinite(v) else "unrecognized"
    s = str(v).strip()
    if s == "":
        return "empty_string"
    if re.fullmatch(r"-?\d+", s):
        return "integer_string"
    if re.fullmatch(r"-?\d+\.\d+", s):
        return "dot_decimal_string"
    if re.fullmatch(r"-?\d+,\d+", s):
        return "comma_decimal_string"
    if re.fullmatch(r"-?\d{1,3}(\.\d{3})+", s):
        return "dot_thousands_string"
    if re.fullmatch(r"-?\d{1,3}(,\d{3})+", s):
        return "comma_thousands_string"
    if re.fullmatch(r"-?\d{1,3}(\.\d{3})+,\d+", s):
        return "dot_thousands_comma_decimal_string"
    if re.fullmatch(r"-?\d{1,3}(,\d{3})+\.\d+", s):
        return "comma_thousands_dot_decimal_string"
    return "parseable_other" if to_float(v) is not None else "unparseable"


def classify_date(v):
    if isinstance(v, str):
        s = v.strip()
        if s == "":
            return "empty_string"
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.*", s):
            return "iso_datetime"
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
            return "iso_date"
        if re.fullmatch(r"\d{2}/\d{2}/\d{4}.*", s):
            return "dd_mm_yyyy"
    elif hasattr(v, "year"):
        return "datetime_object"
    return "parseable_other" if parse_time(v) is not None else "unparseable"


def classify_text(v):
    return "empty_string" if str(v).strip() == "" else "text"


CLASSIFIERS = {"bool": classify_bool, "number": classify_number,
               "date": classify_date, "text": classify_text, "id": classify_text}

# classifications that count as a genuine problem, not just "a different but
# valid way of writing the same thing"
BAD = {"bool": {"unrecognized"}, "number": {"unparseable"},
       "date": {"unparseable"}, "text": {"empty_string"}, "id": {"empty_string"}}


# --------------------------------------------------------------------------- #
# Auditing
# --------------------------------------------------------------------------- #

DATA_MODEL_BY_NORM = {norm(label): (label, spec) for label, spec in DATA_MODEL.items()}


def audit_label(label_norm, label_display, pilot, nodes, problems):
    canonical, spec = DATA_MODEL_BY_NORM.get(label_norm, (label_display, {"required": {}, "optional": {}}))
    fields = {**spec["required"], **spec["optional"]}
    count = len(nodes)

    fields_report = {}
    for canon_name, kind in fields.items():
        required = canon_name in spec["required"]
        formats = Counter()
        present = 0
        spellings = set()
        for n in nodes:
            matched_key = next((k for k in n["props"] if norm(k) == norm(canon_name)), None)
            val = n["props"][matched_key] if matched_key is not None else None
            if matched_key is not None:
                spellings.add(matched_key)
            if val is None:
                formats["missing"] += 1
                if required:
                    problems.append({"pilot": pilot, "label": canonical, "id": n["id"],
                                     "field": canon_name, "issue": "missing_required_field"})
                continue
            present += 1
            cls = CLASSIFIERS[kind](val)
            formats[cls] += 1
            if cls in BAD[kind]:
                problems.append({"pilot": pilot, "label": canonical, "id": n["id"],
                                 "field": canon_name, "issue": cls, "value": val})
        fields_report[canon_name] = {
            "kind": kind, "required": required, "present": present,
            "missing": count - present, "value_formats": dict(formats.most_common()),
            "spellings_seen": sorted(spellings) if len(spellings) > 1 else None,
        }

    known_norms = {norm(k) for k in fields}
    unexpected = Counter(k for n in nodes for k in n["props"] if norm(k) not in known_norms)

    bad_ids = {p["id"] for p in problems if p["label"] == canonical}
    return {
        "label": canonical,
        "known_in_data_model": label_norm in DATA_MODEL_BY_NORM,
        "count": count,
        "fields": fields_report,
        "unexpected_properties": dict(unexpected.most_common()),
        "nodes_with_an_inconsistency": len(bad_ids),
    }


def audit_relationships(rels, label_of, pilot, problems):
    by_type = defaultdict(list)
    for r in rels:
        by_type[r["rel"]].append(r)

    report = {}
    for rel_type in sorted(set(by_type) | set(RELATIONSHIP_MODEL)):
        rows = by_type.get(rel_type, [])
        expected = RELATIONSHIP_MODEL.get(rel_type)
        src_mismatch = dst_mismatch = dangling_src = dangling_dst = 0
        for r in rows:
            src_label, dst_label = label_of.get(r["src"]), label_of.get(r["dst"])
            if src_label is None:
                dangling_src += 1
                problems.append({"pilot": pilot, "label": None, "id": r["src"], "field": None,
                                 "issue": f"{rel_type}: source node {r['src']!r} not found"})
            elif expected and norm(src_label) != norm(expected[0]):
                src_mismatch += 1
                problems.append({"pilot": pilot, "label": src_label, "id": r["src"], "field": None,
                                 "issue": f"{rel_type}: expected source label "
                                          f"'{expected[0]}', got '{src_label}'"})
            if dst_label is None:
                dangling_dst += 1
                problems.append({"pilot": pilot, "label": None, "id": r["dst"], "field": None,
                                 "issue": f"{rel_type}: target node {r['dst']!r} not found"})
            elif expected and norm(dst_label) != norm(expected[1]):
                dst_mismatch += 1
                problems.append({"pilot": pilot, "label": dst_label, "id": r["dst"], "field": None,
                                 "issue": f"{rel_type}: expected target label "
                                          f"'{expected[1]}', got '{dst_label}'"})
        report[rel_type] = {
            "count": len(rows), "known_in_data_model": expected is not None,
            "expected_src_label": expected[0] if expected else None,
            "expected_dst_label": expected[1] if expected else None,
            "src_label_mismatches": src_mismatch, "dst_label_mismatches": dst_mismatch,
            "dangling_src": dangling_src, "dangling_dst": dangling_dst,
        }
    return report


def audit_ids(nodes, pilot, problems):
    id_count = Counter(n["id"] for n in nodes if n.get("id") is not None)
    missing = sum(1 for n in nodes if n.get("id") is None)
    duplicates = {i: c for i, c in id_count.items() if c > 1}
    for i, c in duplicates.items():
        problems.append({"pilot": pilot, "label": None, "id": i, "field": "id",
                         "issue": f"duplicate graph id, used by {c} nodes"})
    if missing:
        problems.append({"pilot": pilot, "label": None, "id": None, "field": "id",
                         "issue": f"{missing} nodes have no graph id at all (unusable)"})
    return {"nodes_missing_graph_id": missing, "duplicate_graph_ids": len(duplicates),
            "duplicate_examples": dict(list(duplicates.items())[:5])}


def run_pilot(pilot, source_dir, out_dir):
    nodes, rels = load_rows(pilot, source_dir)

    by_label = defaultdict(list)
    label_of = {}
    label_spellings = defaultdict(set)
    for n in nodes:
        if n.get("id") is None:
            continue
        label_of[n["id"]] = n["label"]
        label_norm = norm(n["label"] or "")
        by_label[label_norm].append({"id": n["id"], "props": n.get("props") or {}})
        label_spellings[label_norm].add(n["label"] or "")

    problems = []
    labels_report = {}
    for label_norm in sorted(set(by_label) | set(DATA_MODEL_BY_NORM)):
        display = DATA_MODEL_BY_NORM.get(label_norm, (None, None))[0] \
            or sorted(label_spellings.get(label_norm, {label_norm}))[0]
        rep = audit_label(label_norm, display, pilot, by_label.get(label_norm, []), problems)
        labels_report[rep["label"]] = rep
    rel_report = audit_relationships(rels, label_of, pilot, problems)
    id_report = audit_ids(nodes, pilot, problems)

    total_nodes = sum(v["count"] for v in labels_report.values())
    nodes_with_issue = len({p["id"] for p in problems if p.get("id") and p.get("label")})

    report = {
        "pilot": pilot, "total_nodes": total_nodes, "total_relationships": len(rels),
        "nodes_with_an_inconsistency": nodes_with_issue, "total_issues_logged": len(problems),
        "labels": labels_report, "relationships": rel_report, "ids": id_report,
    }

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / f"{pilot}_data_model_report.json"
    problems_path = out_dir / f"{pilot}_inconsistencies.json"
    report_path.write_text(json.dumps(report, indent=1, ensure_ascii=False, default=str))
    problems_path.write_text(json.dumps(problems, indent=1, ensure_ascii=False, default=str))

    print(f"\n{'=' * 78}\nPILOT {pilot.upper()}\n{'=' * 78}")
    print(f"  {total_nodes} nodes, {len(rels)} relationships   "
          f"{nodes_with_issue} nodes don't fully respect the data model   "
          f"{len(problems)} individual issues logged")
    for label, rep in labels_report.items():
        flag = "   (UNKNOWN LABEL -- not in DATA_MODEL)" if not rep["known_in_data_model"] else ""
        print(f"\n  {label:<20} {rep['count']:>6} nodes, "
              f"{rep['nodes_with_an_inconsistency']} with an issue{flag}")
        for field, f in rep["fields"].items():
            formats = ", ".join(f"{k}:{v}" for k, v in f["value_formats"].items())
            req = "required" if f["required"] else "optional"
            spell = f"  spellings: {f['spellings_seen']}" if f["spellings_seen"] else ""
            print(f"      {field:<24} [{f['kind']}, {req}]  present {f['present']}/{rep['count']}"
                  f"   {formats}{spell}")
        if rep["unexpected_properties"]:
            print(f"      (not in the model) {dict(list(rep['unexpected_properties'].items())[:6])}")
    print("\n  relationships:")
    for rel_type, r in rel_report.items():
        flag = "" if r["known_in_data_model"] else "  (not in RELATIONSHIP_MODEL)"
        issues = (f"  mismatches: src {r['src_label_mismatches']}, dst {r['dst_label_mismatches']}; "
                  f"dangling: src {r['dangling_src']}, dst {r['dangling_dst']}"
                  if (r["src_label_mismatches"] or r["dst_label_mismatches"]
                      or r["dangling_src"] or r["dangling_dst"]) else "")
        print(f"    {rel_type:<20} {r['count']:>6}{flag}{issues}")
    if id_report["nodes_missing_graph_id"] or id_report["duplicate_graph_ids"]:
        print(f"\n  ids: {id_report['nodes_missing_graph_id']} nodes with no graph id, "
              f"{id_report['duplicate_graph_ids']} duplicate graph ids")
    print(f"\n  saved {report_path}\n  saved {problems_path}")
    return report


if __name__ == "__main__":
    load_dotenv()
    ap = argparse.ArgumentParser()
    ap.add_argument("pilots", nargs="*", help="it ua sl (default: PILOTS in .env, else all)")
    ap.add_argument("--source", default="graphs",
                     help="folder to read <pilot>_raw.json from (default: graphs/); "
                          "falls back to the live endpoint if not found")
    ap.add_argument("--out", default="audits", help="where to save the reports (default: audits/)")
    args = ap.parse_args()

    failed = []
    for pilot in resolve_pilots(args.pilots):
        try:
            run_pilot(pilot, args.source, args.out)
        except Exception as exc:
            failed.append(pilot)
            print(f"[{pilot}] FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
    sys.exit(1 if failed else 0)