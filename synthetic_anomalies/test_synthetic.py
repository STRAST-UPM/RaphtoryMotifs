"""CEDAR risk scoring on the SYNTHETIC anomaly graph -- main.py without Kafka.

Same analysis as for the pilots (main.run: Neo4j -> Raphtory -> motifs ->
organization / public entity / tender scores), on the local graph seeded with
the planted scenarios (synthetic_anomalies/). The request is built here
instead of arriving on Kafka, and the response -- same template as main.py's
(messages.build_message) -- is only saved, never published.

    docker compose -f synthetic_anomalies/docker-compose.yml up -d   # Neo4j + gateway + synthetic graph
    python synthetic_anomalies/test_synthetic.py      # every tender, organization, public entity
    python synthetic_anomalies/test_synthetic.py --tender T3 T40 --organization Org_1 Org_5
    python synthetic_anomalies/test_synthetic.py --public-entity all
    python synthetic_anomalies/test_synthetic.py --out outputs

Writes, like main.py:
    <out>/synthetic_latest.json                     last response
    <out>/synthetic/<timestamp>_<requestID>.json    one file per run
and <out>/synthetic/ground_truth.json, the planted scenarios to compare against.

.env:
    SYNTHETIC_GRAPH_ENDPOINT=http://localhost:8000/neo4j/query   (default)
    SYNTHETIC_GRAPH_API_KEY=<key of the local gateway>
"""

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))     # main.py, cedar_risk

from dotenv import load_dotenv

from data_generation import generate                    # same folder as this script
from main import run, save_message
from cedar_risk.utils.messages import (
    ALL, INDICATOR_ID, InvalidRequest, answer, build_message, build_request, describe_plan,
    now_ms, parse_request, request_plan)
from cedar_risk.utils.logger import get_logger, setup_logging, describe_error

log = get_logger("synthetic")

PILOT = "synthetic"                 # file names: outputs/synthetic_latest.json, ...
DEFAULT_ENDPOINT = "http://localhost:8000/neo4j/query"


def requested_entities(args):
    """entities[] of the request; nothing named -> all three types, entityId 'all'
    (same rule as kafka/send_request.py)."""
    named = [("tender", args.tender),
             ("organization", args.organization),
             ("publicEntity", args.public_entity)]
    entities = [{"entityType": etype, "entityId": eid}
                for etype, ids in named for eid in (ids or [])]
    return entities or [{"entityType": etype, "entityId": ALL} for etype, _ in named]


def save_ground_truth(out_dir, seed):
    """The scenarios planted by data_generation, next to the responses."""
    _, scenarios = generate(seed)
    path = Path(out_dir) / PILOT / "ground_truth.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(scenarios, indent=1, ensure_ascii=False, default=str),
                    encoding="utf-8")
    return path


def log_top(plan, indicators, scores, n=5):
    """The highest risks per requested node type, to eyeball against the
    ground truth (one indicator_id covers every type, so group by type)."""
    for node_type in plan:
        ids = {str(k) for k in scores.get(node_type) or {}}
        items = sorted((i for i in indicators if i["entityID"] in ids),
                       key=lambda i: -(i["risk_level"] or 0))
        top = ", ".join(f"{i['entityID']}={i['risk_level']:.2f}" for i in items[:n])
        log.info("[top]     %-13s %s", node_type, top)


def main():
    load_dotenv()
    setup_logging(log_file='./logs/synthetic.log')
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--tender", nargs="+", metavar="ID", help='tender IDs, or "all"')
    ap.add_argument("--organization", nargs="+", metavar="ID",
                    help='organization IDs, or "all"')
    ap.add_argument("--public-entity", nargs="+", metavar="ID",
                    help='public entity IDs, or "all"')
    ap.add_argument("--indicators", nargs="+", metavar="INDICATOR_ID", default=[INDICATOR_ID],
                    help=f"indicator_id strings (default: {INDICATOR_ID}, this tool's)")
    ap.add_argument("--seed", type=int, default=42,
                    help="seed the graph was generated with, for ground_truth.json")
    ap.add_argument("--out", default="outputs",
                    help="folder for the JSON responses (default: outputs/)")
    args = ap.parse_args()

    endpoint = os.getenv("SYNTHETIC_GRAPH_ENDPOINT") or DEFAULT_ENDPOINT
    api_key = os.getenv("SYNTHETIC_GRAPH_API_KEY")
    if not api_key:
        sys.exit("Set SYNTHETIC_GRAPH_API_KEY (the local gateway's key) in .env")

    request = build_request(requested_entities(args), indicators=args.indicators,
                            kg_version="synthetic", issuer="test_synthetic")
    try:
        parse_request(request)
    except InvalidRequest as e:
        sys.exit(f"invalid request: {e}")
    plan = request_plan(request)
    if plan is None:
        sys.exit("none of this tool's indicators was requested")
    rid = request["requestID"]
    log.info("[request] %s %s: %s", PILOT, rid, describe_plan(plan))

    start_ms = now_ms()
    scores, failure = None, None
    try:
        log.info("[analysis] %s: starting analysis -> %s", PILOT, endpoint)
        org, ent, ten = run(endpoint, api_key)
        scores = {"Organization": org, "PublicEntity": ent, "Tender": ten}
    except Exception as e:
        log.exception("[analysis] %s: analysis failed: %s", PILOT, describe_error(e))
        failure = f"The analysis could not run: {describe_error(e)}"

    indicators, entities = answer(plan, scores, failure)
    if not indicators:
        log.warning("[request] %s: nothing produced -> no response saved", rid)
        sys.exit(1)
    msg = build_message(indicators, entities, kg_version=request["kg"]["versionId"],
                        start_ms=start_ms, request_id=rid)
    save_message(PILOT, msg, args.out)
    log.info("[save]    ground truth -> %s", save_ground_truth(args.out, args.seed))
    if failure is None:
        log_top(plan, indicators, scores)
    sys.exit(1 if failure else 0)


if __name__ == "__main__":
    main()
