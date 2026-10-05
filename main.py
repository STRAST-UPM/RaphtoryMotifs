"""CEDAR risk-scoring tool -- request / response flow of the CEDAR Kafka contract.

    request on <pilot>_analyse  (from the UI, or from send_request.py)
      -> scores of that pilot's graph: Neo4j -> Raphtory -> motifs ->
         organization / public entity / tender anomaly scores
         (computed once, then recomputed only when the Neo4j graph changes)
      -> the requested entities and indicators are picked out
      -> ONE response, saved as JSON, then published on <pilot>_results_available

Request extension (see kafka_client.py): entityId "all" = every entity of
that entityType; {"entityType": "all", "entityId": "all"} = the whole graph.

Which pilots to serve, in order of precedence:
    1. --pilots on the command line             python main.py --pilots it sl
    2. PILOTS in .env (space/comma separated)   PILOTS=it,sl
    3. every pilot

.env:
    PILOTS=it,sl
    KAFKA_BOOTSTRAP=localhost:9092
    KAFKA_REQUEST_TOPICS=it_analyse,sl_analyse
    KAFKA_RESULTS_TOPICS=it_results_available,sl_results_available
    KAFKA_GROUP_ID=upm-raphtory          this tool's own consumer group (topics are shared)
    KAFKA_USERNAME / KAFKA_PASSWORD      only for a SASL broker, leave unset locally
    GRAPH_API_KEY_IT / GRAPH_API_KEY_SL  (or plain GRAPH_API_KEY)
    GRAPH_ENDPOINT=https://kg-{pilot}.cedar-heu-project.eu/neo4j/query

Run:  python main.py                       -> serve requests for every pilot in PILOTS
      DRY_RUN=1 python main.py             -> answer and save, do not publish
      python main.py --out outputs         -> where the JSON copies go (default: outputs/)
      python send_request.py               -> (other terminal) ask for everything

Each response is saved BEFORE it is published (on disk even if Kafka is down):
    <out>/<pilot>_latest.json                    last response of that pilot
    <out>/<pilot>/<timestamp>_<requestID>.json   one file per response
"""


import argparse
import json
import os
import re
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv


from cedar_risk.utils import neo4j_store
from cedar_risk import temporal_graph, motifs
from cedar_risk.scoring import organization_scores, entity_scores, tender_scores
from cedar_risk.utils.kafka_client import (
    InvalidRequest, parse_request, request_plan, describe_plan, answer,
    build_message, make_producer, make_consumer, publish, kafka_settings,
    pilot_topic, now_ms)
from cedar_risk.utils.config import DELAY
from cedar_risk.utils.neo4j_store import pilot_endpoint, pilot_setting, resolve_pilots


def run(endpoint, api_key):
    """Read the graph from Neo4j, extract motifs, and return the anomaly output
    for organizations, public entities and tenders (three {id: dict} maps)."""
    nodes, rels = neo4j_store.read_from_neo4j(endpoint=endpoint, api_key=api_key)
    print(f"[storage] {len(nodes)} nodes, {len(rels)} relationships from Neo4j")

    g = temporal_graph.build_temporal_graph(nodes, rels)
    T = temporal_graph.tender_index(g)
    print(f"[graph]   {g.count_nodes()} nodes, {g.count_edges()} edges, "
          f"tenders indexed={len(T)}")
    if not T:
        print("[graph]   no tenders in this graph -> nothing to score")
        return {}, {}, {}

    trip = motifs.org_motifs(T)
    cliques = motifs.max_cliques(motifs.suspicious_groups(trip))
    quad = motifs.entity_group_motifs(T)

    ids = temporal_graph.node_ids(g)      # every node gets a score, even with no connection
    org_anom = organization_scores(T, trip, cliques, all_orgs=ids["organization"])
    entity_anom = entity_scores(T, quad, all_entities=ids["public_entity"])
    tender_anom = tender_scores(T, org_anom, entity_anom)   # inherits from the two above
    print(f"[scores]  {len(org_anom)} organizations, {len(entity_anom)} entities, "
          f"{len(tender_anom)} tenders")
    return org_anom, entity_anom, tender_anom


def save_message(pilot, message, out_dir):
    """Save the exact response about to be published:
      <out_dir>/<pilot>_latest.json                    overwritten -- last response
      <out_dir>/<pilot>/<timestamp>_<requestID>.json   one file per response
    """
    out_dir = Path(out_dir)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    rid = re.sub(r"[^A-Za-z0-9._-]", "_", message["requestID"])[:80]   # safe file name
    text = json.dumps(message, ensure_ascii=False, indent=1, default=str)

    latest = out_dir / f"{pilot}_latest.json"
    history = out_dir / pilot / f"{ts}_{rid}.json"
    for path in (latest, history):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    print(f"[save]    {len(message['analysis']['indicators'])} indicators -> {history}")
    return history


class PilotContext:
    """Endpoint, API key, topics, output folder, change-listener and the
    cached scores of one pilot."""

    def __init__(self, pilot, out_dir):
        self.pilot = pilot
        self.out_dir = out_dir
        self.endpoint = pilot_endpoint(pilot)
        self.api_key = pilot_setting("GRAPH_API_KEY", pilot)
        assert self.endpoint and self.api_key, (
                "Set GRAPH_ENDPOINT_<PILOT> and GRAPH_API_KEY_<PILOT> "
                "(or GRAPH_ENDPOINT / GRAPH_API_KEY) in your .env.")
        self.request_topic = pilot_topic(pilot, "request")
        self.results_topic = pilot_topic(pilot, "results")
        self.listener = neo4j_store.Neo4jListener(endpoint=self.endpoint, api_key=self.api_key)
        self.scores = None      # {node_type: {node_id: score}} of the current graph
        self.dirty = True       # no scores yet, graph changed, or last run failed

    def refresh(self):
        """Scores of the current graph: computed the first time, then again
        only when the Neo4j graph has changed (or the last run failed)."""
        if not self.dirty and self.listener.has_changed():
            print(f"Graph change detected ({self.pilot}) " + "#" * 50)
            self.dirty = True
        if self.dirty:
            print(f"\n[pilot]   {self.pilot} -> {self.endpoint}")
            org, ent, ten = run(self.endpoint, self.api_key)
            self.scores = {"Organization": org, "PublicEntity": ent, "Tender": ten}
            self.dirty = False
        return self.scores

    def safe_refresh(self):
        """refresh() between requests; a failure is logged and retried later."""
        try:
            self.refresh()
        except Exception:
            traceback.print_exc()
            print(f"[pilot]   {self.pilot}: analysis failed, retrying in {DELAY}s",
                  file=sys.stderr)

    def close(self):
        self.listener.close()


def handle_request(ctx, record, producer):
    """Answer one request message with one response (CEDAR Kafka contract)."""
    try:
        request = parse_request(record.value)
    except InvalidRequest as e:
        print(f"[request] {record.topic}@{record.offset}: invalid request skipped ({e})",
              file=sys.stderr)
        return None

    rid = request["requestID"]
    plan = request_plan(request)
    if plan is None:
        print(f"[request] {rid}: none of this tool's indicators requested -> skipped")
        return None
    print(f"\n[request] {ctx.pilot} {rid} from {request['issuer']!r}: {describe_plan(plan)}")

    # contract: startTime = when the request was published (Kafka record timestamp)
    start_ms = record.timestamp if (record.timestamp or 0) > 0 else now_ms()

    failure = None
    try:
        scores = ctx.refresh()
    except Exception as e:
        traceback.print_exc()
        scores = ctx.scores
        if scores is not None:
            print(f"[request] {rid}: analysis failed, answering from the last scores",
                  file=sys.stderr)
        else:
            failure = f"The analysis could not run: {type(e).__name__}: {e}"

    indicators, entities = answer(plan, scores, failure)
    if not indicators:
        # contract: no response when nothing was produced
        print(f"[request] {rid}: nothing produced -> no response")
        return None

    msg = build_message(indicators, entities, kg_version=request["kg"]["versionId"],
                        start_ms=start_ms, request_id=rid)
    for i in indicators:
        print(f"  {i['indicator_id']:<32} {i['entityID']:<14} risk={i['risk_level']}")
    save_message(ctx.pilot, msg, ctx.out_dir)

    if producer is None:
        print(f"[dry-run] response {msg['responseID']} saved, not published "
              f"to {ctx.results_topic}")
        return msg
    publish(producer, ctx.results_topic, msg)
    print(f"[kafka]   response {msg['responseID']} ({len(indicators)} indicators) "
          f"-> {ctx.results_topic}")
    return msg


if __name__ == "__main__":
    load_dotenv()

    ap = argparse.ArgumentParser()
    ap.add_argument("--pilots", nargs="+", metavar="PILOT",
                     help="pilots to serve, e.g. --pilots it sl (default: PILOTS in "
                          ".env, else every pilot)")
    ap.add_argument("--out", default="outputs",
                     help="folder to save each response (as JSON) before it is "
                          "published to Kafka (default: outputs/)")
    args = ap.parse_args()

    requested = resolve_pilots(args.pilots)
    contexts = []
    for pilot in requested:
        if not pilot_setting("GRAPH_API_KEY", pilot):
            print(f"[pilot]   {pilot}: skipped, no GRAPH_API_KEY_{pilot.upper()} "
                  f"(or GRAPH_API_KEY) in .env", file=sys.stderr)
            continue
        contexts.append(PilotContext(pilot, out_dir=args.out))
    assert contexts, (
        f"No pilot had an API key set. Requested: {requested}. "
        f"Set GRAPH_API_KEY_<PILOT> (or GRAPH_API_KEY) in your .env.")

    by_topic = {c.request_topic: c for c in contexts}
    for c in contexts:
        print(f"[pilots]  {c.pilot}: {c.request_topic} -> {c.results_topic}")
    print(f"[pilots]  saving to: {args.out}/")

    settings = kafka_settings()
    dry_run = os.getenv("DRY_RUN", "").lower() in {"1", "true", "yes"}
    producer = None if dry_run else make_producer(**settings)
    consumer = make_consumer(
        list(by_topic),
        group_id=os.getenv("KAFKA_GROUP_ID", "upm-raphtory"),
        auto_offset_reset=os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest"),
        **settings,
    )
    print(f"[kafka]   waiting for requests on {', '.join(by_topic)}")

    try:
        while True:
            batch = consumer.poll(timeout_ms=int(DELAY * 1000))
            for tp, records in batch.items():
                ctx = by_topic[tp.topic]
                for record in records:
                    try:
                        handle_request(ctx, record, producer)
                    except Exception:
                        traceback.print_exc()
                        print(f"[request] {tp.topic}@{record.offset}: failed, "
                              f"the saved copy (if any) is in {args.out}/", file=sys.stderr)
                    consumer.commit()       # answered (or skipped): don't redo it on restart

            # between requests: keep each pilot's scores in step with its graph,
            # so a request is answered from up-to-date scores without waiting
            for ctx in contexts:
                ctx.safe_refresh()
    except KeyboardInterrupt:
        print("\n[kafka]   stopping")
    finally:
        consumer.close()
        for ctx in contexts:
            ctx.close()
        if producer is not None:
            producer.close()