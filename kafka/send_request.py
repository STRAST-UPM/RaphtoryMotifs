"""Send an analysis request (CEDAR Kafka contract) to each pilot's request
topic, then wait for this tool's response on the pilot's results topic.

main.py must be running (in another terminal) to answer.

    python send_request.py                    -> every pilot in PILOTS: all tenders,
                                                 organizations and public entities
    python send_request.py --pilots it        -> only the Italian graph
    python send_request.py --pilots sl --tender B0835BC998 C1234ABCD5
    python send_request.py --pilots it --organization all --tender B0835BC998
    python send_request.py --everything       -> one {"entityType": "all", "entityId": "all"}
    python send_request.py --indicators UPM_RAPHTORY_TENDER_RISK
    python send_request.py --wait 0           -> send only, don't wait for the response
    python send_request.py --print            -> also print the request and the full response

"all" as an ID means every entity of that type (our extension to the contract).
Topics and broker come from the same .env as main.py:
    KAFKA_BOOTSTRAP_SERVERS + the SASL_SSL credentials, KAFKA_REQUEST_TOPICS,
    KAFKA_RESULTS_TOPICS, PILOTS
    KG_VERSION_IT / KG_VERSION_SL (or KG_VERSION) -> kg.versionId, unless --kg-version
"""

import argparse
import json
import sys
import time
from collections import Counter

from dotenv import load_dotenv

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))


from cedar_risk.utils.kafka_client import (
    ALL, TOOL_NAME, build_request, kafka_settings, make_producer, pilot_topic,
    publish, tail_consumer)
from cedar_risk.utils.neo4j_store import pilot_setting, resolve_pilots


def requested_entities(args):
    """entities[] of the request; nothing named -> all three types, entityId 'all'."""
    if args.everything:
        return [{"entityType": ALL, "entityId": ALL}]
    named = [("tender", args.tender),
             ("organization", args.organization),
             ("publicEntity", args.public_entity)]
    entities = [{"entityType": etype, "entityId": eid}
                for etype, ids in named for eid in (ids or [])]
    return entities or [{"entityType": etype, "entityId": ALL} for etype, _ in named]


def report(pilot, msg, show_all, top=5):
    """One-line summary of a response, then its highest risks."""
    analysis = msg["analysis"]
    indicators = analysis["indicators"]
    took = (analysis["endTime"] - analysis["startTime"]) / 1000
    per_id = Counter(i["indicator_id"] for i in indicators)
    failed = sum(1 for i in indicators
                 if any(a.get("value") == "Analysis Failed" for a in i["additional_info"]))

    print(f"\n[result]  {pilot}: response {msg['responseID']} after {took:.1f}s, "
          f"{len(indicators)} indicators ("
          + ", ".join(f"{n} {k}" for k, n in per_id.items())
          + (f"; {failed} Analysis Failed" if failed else "") + ")")
    for i in sorted(indicators, key=lambda i: -i["risk_level"])[:top]:
        print(f"          {i['risk_level']:.3f}  {i['indicator_id']:<32} {i['entityID']}")
    if show_all:
        print(json.dumps(msg, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    load_dotenv()

    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pilots", nargs="+", metavar="PILOT",
                    help="pilots to ask (default: PILOTS in .env, else every pilot)")
    ap.add_argument("--tender", nargs="+", metavar="ID", help='tender IDs, or "all"')
    ap.add_argument("--organization", nargs="+", metavar="ID",
                    help='organization IDs, or "all"')
    ap.add_argument("--public-entity", nargs="+", metavar="ID",
                    help='public entity IDs, or "all"')
    ap.add_argument("--everything", action="store_true",
                    help='a single {"entityType": "all", "entityId": "all"} entry')
    ap.add_argument("--indicators", nargs="+", metavar="INDICATOR_ID",
                    help="indicator_id strings (default: none = all indicators)")
    ap.add_argument("--kg-version", help="kg.versionId (default: KG_VERSION_<PILOT> "
                                         "or KG_VERSION in .env, else 'latest')")
    ap.add_argument("--issuer", default="ui", help="issuer field (default: ui)")
    ap.add_argument("--wait", type=float, default=300,
                    help="seconds to wait for the responses, 0 = don't wait (default: 300)")
    ap.add_argument("--print", action="store_true", dest="show_all",
                    help="also print the request and the full response JSON")
    args = ap.parse_args()

    pilots = resolve_pilots(args.pilots)
    settings = kafka_settings()
    entities = requested_entities(args)

    # listen on the results topics BEFORE sending, so no response can be missed
    consumer = None
    if args.wait > 0:
        consumer = tail_consumer([pilot_topic(p, "results") for p in pilots], **settings)

    producer = make_producer(**settings)
    pending = {}                                    # requestID -> pilot
    for pilot in pilots:
        request = build_request(
            entities,
            indicators=args.indicators,
            kg_version=args.kg_version or pilot_setting("KG_VERSION", pilot) or "latest",
            issuer=args.issuer,
        )
        topic = pilot_topic(pilot, "request")
        publish(producer, topic, request)
        pending[request["requestID"]] = pilot
        print(f"[sent]    {pilot}: request {request['requestID']} -> {topic}  "
              f"({len(entities)} entities)")
        if args.show_all:
            print(json.dumps(request, ensure_ascii=False, indent=1))
    producer.close()

    if consumer is None:
        sys.exit(0)

    print(f"[wait]    up to {args.wait:.0f}s for {TOOL_NAME} on "
          + ", ".join(pilot_topic(p, "results") for p in pilots))
    deadline = time.monotonic() + args.wait
    try:
        while pending and time.monotonic() < deadline:
            for records in consumer.poll(timeout_ms=1000).values():
                for record in records:
                    try:
                        msg = json.loads(record.value)
                    except (ValueError, TypeError):
                        continue
                    # other tools may answer the same requestID: keep only ours
                    if (isinstance(msg, dict) and msg.get("requestID") in pending
                            and msg.get("toolName") == TOOL_NAME):
                        report(pending.pop(msg["requestID"]), msg, args.show_all)
    finally:
        consumer.close()

    if pending:
        print(f"\n[timeout] no response after {args.wait:.0f}s for: "
              + ", ".join(f"{p} ({rid})" for rid, p in pending.items())
              + "  -- is main.py running and are the topics right?", file=sys.stderr)
        sys.exit(1)