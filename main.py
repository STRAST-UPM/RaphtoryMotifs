"""CEDAR risk-scoring tool -- request / response flow of the CEDAR Kafka contract.

    request on <pilot>_analyse  (from the UI, or from send_request.py)
      -> scores of that pilot's graph: Neo4j -> Raphtory -> motifs ->
         organization / public entity / tender anomaly scores
         (run on every request, on the graph as it is at that moment --
          nothing runs without a request)
      -> the requested entities and indicators are picked out
      -> ONE response, saved as JSON, then published on <pilot>_results_available

Request extension (see messages.py): entityId "all" = every entity of
that entityType; {"entityType": "all", "entityId": "all"} = the whole graph.

Which pilots to serve, in order of precedence:
    1. --pilots on the command line             python main.py --pilots it sl
    2. PILOTS in .env (space/comma separated)   PILOTS=it,sl
    3. every pilot

.env:
    PILOTS=it,sl
    KAFKA_BOOTSTRAP_SERVERS=localhost:8004   local replica (production: kafka.cedar-heu-project.eu:8004)
    KAFKA_REQUEST_TOPICS=it_analyse,sl_analyse
    KAFKA_RESULTS_TOPICS=it_results_available,sl_results_available
    KAFKA_GROUP_ID=upm-raphtory          this tool's own consumer group (topics are shared)
    KAFKA_CREDENTIALS_DIR, KAFKA_KEY_PASSWORD, KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD
                                         SASL_SSL credentials, as in the production
                                         template (see .env.example, kafka_client.py)
    GRAPH_API_KEY_IT / GRAPH_API_KEY_SL  (or plain GRAPH_API_KEY)
    GRAPH_ENDPOINT=https://kg-{pilot}.cedar-heu-project.eu/neo4j/query

Run:  python main.py                       -> serve requests for every pilot in PILOTS
      DRY_RUN=1 python main.py             -> answer (and save), do not publish
      python main.py --out outputs         -> save the responses there (overrides OUTPUT_DIR)
      python send_request.py               -> (other terminal) ask for everything

Saving the responses on disk is optional: with OUTPUT_DIR in .env (or --out),
each response is saved BEFORE it is published (so it is kept even if Kafka is
down or refuses it); without, it is only published.
    <out>/<pilot>_latest.json                    last response of that pilot
    <out>/<pilot>/<timestamp>_<requestID>.json   one file per response
"""

import argparse
import json
import os
import re
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
from kafka.errors import MessageSizeTooLargeError


from cedar_risk.utils import neo4j_store
from cedar_risk import temporal_graph, motifs
from cedar_risk.scoring import organization_scores, entity_scores, tender_scores
from cedar_risk.utils.kafka_client import (
    make_producer, make_consumer, publish, kafka_settings, pilot_topic)
from cedar_risk.utils.messages import (
    InvalidRequest, parse_request, request_plan, describe_plan, answer,
    build_message, now_ms)

from cedar_risk.utils.config import DELAY
from cedar_risk.utils.logger import get_logger, setup_logging, describe_error
from cedar_risk.utils.neo4j_store import pilot_endpoint, pilot_setting, resolve_pilots

log = get_logger(__name__)


def run(endpoint, api_key):
    """Read the graph from Neo4j, extract motifs, and return the anomaly output
    for organizations, public entities and tenders (three {id: dict} maps)."""
    log.info("[analysis] reading the graph from Neo4j (%s)", endpoint)
    try:
        nodes, rels = neo4j_store.read_from_neo4j(endpoint=endpoint, api_key=api_key)
    except Exception as e:
        log.exception("[analysis] reading the graph from Neo4j failed (%s): %s",
                      endpoint, describe_error(e))
        raise
    log.info("[storage] %d nodes, %d relationships from Neo4j", len(nodes), len(rels))

    log.info("[analysis] building the temporal graph")
    try:
        g = temporal_graph.build_temporal_graph(nodes, rels)
        T = temporal_graph.tender_index(g)
    except Exception as e:
        log.exception("[analysis] building the temporal graph failed: %s", describe_error(e))
        raise
    log.info("[graph]   %d nodes, %d edges, tenders indexed=%d",
             g.count_nodes(), g.count_edges(), len(T))
    if not T:
        log.warning("[graph]   no tenders in this graph -> nothing to score")
        return {}, {}, {}

    log.info("[analysis] extracting motifs")
    try:
        trip = motifs.org_motifs(T)
        cliques = motifs.max_cliques(motifs.suspicious_groups(trip))
        quad = motifs.entity_group_motifs(T)
    except Exception as e:
        log.exception("[analysis] motif extraction failed: %s", describe_error(e))
        raise
    log.info("[motifs]  %d organization motifs, %d suspicious cliques, %d entity motifs",
             len(trip), len(cliques), len(quad))

    log.info("[analysis] scoring organizations, public entities and tenders")
    try:
        ids = temporal_graph.node_ids(g)      # every node gets a score, even with no connection
        org_anom = organization_scores(T, trip, cliques, all_orgs=ids["organization"])
        entity_anom = entity_scores(T, quad, all_entities=ids["public_entity"])
        tender_anom = tender_scores(T, org_anom, entity_anom)   # inherits from the two above
    except Exception as e:
        log.exception("[analysis] scoring failed: %s", describe_error(e))
        raise
    log.info("[scores]  %d organizations, %d entities, %d tenders",
             len(org_anom), len(entity_anom), len(tender_anom))
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

    log.info("[save]    %d indicators -> %s",
             len(message["analysis"]["indicators"]), history)
    return history


class PilotContext:
    """Endpoint, API key, topics, output folder and the last scores of one pilot."""

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
        self.scores = None      # {node_type: {node_id: score}} of the last successful run

    def analyse(self):
        """Scores of the graph as it is now: the analysis runs on every request.
        The result is kept only as a fallback for a later failed run."""
        log.info("[analysis] %s: starting analysis -> %s", self.pilot, self.endpoint)
        org, ent, ten = run(self.endpoint, self.api_key)
        self.scores = {"Organization": org, "PublicEntity": ent, "Tender": ten}
        log.info("[analysis] %s: analysis finished", self.pilot)
        return self.scores


def handle_request(ctx, record, producer):
    """Answer one request message with one response (CEDAR Kafka contract)."""
    try:
        request = parse_request(record.value)
    except InvalidRequest as e:
        log.warning("[request] %s@%s: invalid request skipped (%s)",
                    record.topic, record.offset, e)
        return None

    rid = request["requestID"]
    plan = request_plan(request)
    if plan is None:
        log.info("[request] %s: none of this tool's indicators requested -> skipped", rid)
        return None
    log.info("[request] %s %s from %r: %s",
             ctx.pilot, rid, request["issuer"], describe_plan(plan))

    # contract: startTime = when the request was published (Kafka record timestamp)
    start_ms = record.timestamp if (record.timestamp or 0) > 0 else now_ms()

    failure = None
    try:
        scores = ctx.analyse()
    except Exception as e:
        scores = ctx.scores
        if scores is not None:
            log.exception("[request] %s: analysis failed (%s), answering from the "
                          "last scores", rid, describe_error(e))
        else:
            log.exception("[request] %s: analysis failed (%s) and no previous scores "
                          "exist", rid, describe_error(e))
            failure = f"The analysis could not run: {type(e).__name__}: {e}"

    indicators, entities = answer(plan, scores, failure)
    if not indicators:
        # contract: no response when nothing was produced
        log.info("[request] %s: nothing produced -> no response", rid)
        return None

    msg = build_message(indicators, entities, kg_version=request["kg"]["versionId"],
                        start_ms=start_ms, request_id=rid)
    for i in indicators:
        log.debug("  %-32s %-14s risk=%s", i["indicator_id"], i["entityID"], i["risk_level"])
    if ctx.out_dir:
        save_message(ctx.pilot, msg, ctx.out_dir)

    if producer is None:
        log.info("[dry-run] response %s %s, not published to %s", msg["responseID"],
                 "saved" if ctx.out_dir else "not saved", ctx.results_topic)
        return msg
    try:
        publish(producer, ctx.results_topic, msg)
    except MessageSizeTooLargeError as e:
        # over Kafka's message size limit (the producer's, or the broker's)
        size = len(json.dumps(msg, ensure_ascii=False, default=str).encode("utf-8"))
        log.exception("[kafka]   %s: response %s (%d indicators, %.2f MB before gzip) "
                      "is TOO LARGE for Kafka -- not delivered to %s; %s: %s",
                      rid, msg["responseID"], len(indicators), size / 1e6,
                      ctx.results_topic, _saved_copy(ctx.out_dir), describe_error(e))
        return None
    except Exception as e:
        log.exception("[kafka]   %s: publishing response %s to %s failed: %s",
                      rid, msg["responseID"], ctx.results_topic, describe_error(e))
        raise
    log.info("[kafka]   response %s (%d indicators) -> %s",
             msg["responseID"], len(indicators), ctx.results_topic)
    return msg


def _saved_copy(out_dir):
    """Where a response that could not be delivered can still be found."""
    return (f"the saved copy (if any) is in {out_dir}/" if out_dir
            else "not saved on disk either (OUTPUT_DIR is not set)")


def _stop(signum, frame):
    raise KeyboardInterrupt


def main():
    """Serve the CEDAR Kafka requests of the configured pilots (see the module docstring)."""
    load_dotenv()
    setup_logging()
    # `docker stop` sends SIGTERM: stop like Ctrl-C, so the consumer leaves its
    # group and the producer flushes (else the next start waits for the group)
    signal.signal(signal.SIGTERM, _stop)

    ap = argparse.ArgumentParser()
    ap.add_argument("--pilots", nargs="+", metavar="PILOT",
                     help="pilots to serve, e.g. --pilots it sl (default: PILOTS in "
                          ".env, else every pilot)")
    ap.add_argument("--out",
                     help="folder to save each response (as JSON) before it is "
                          "published (default: OUTPUT_DIR in .env, else not saved)")
    args = ap.parse_args()
    out_dir = args.out or os.getenv("OUTPUT_DIR") or None

    requested = resolve_pilots(args.pilots)
    contexts = []
    for pilot in requested:
        if not pilot_setting("GRAPH_API_KEY", pilot):
            log.warning("[pilot]   %s: skipped, no GRAPH_API_KEY_%s (or GRAPH_API_KEY) in .env",
                        pilot, pilot.upper())
            continue
        contexts.append(PilotContext(pilot, out_dir=out_dir))
    assert contexts, (
        f"No pilot had an API key set. Requested: {requested}. "
        f"Set GRAPH_API_KEY_<PILOT> (or GRAPH_API_KEY) in your .env.")

    by_topic = {c.request_topic: c for c in contexts}
    for c in contexts:
        log.info("[pilots]  %s: %s -> %s", c.pilot, c.request_topic, c.results_topic)
    if out_dir:
        log.info("[pilots]  responses saved to: %s/", out_dir)
    else:
        log.info("[pilots]  responses not saved on disk (OUTPUT_DIR not set): only published")

    settings = kafka_settings()
    dry_run = os.getenv("DRY_RUN", "").lower() in {"1", "true", "yes"}
    if dry_run and not out_dir:
        log.warning("[pilots]  DRY_RUN without OUTPUT_DIR: responses are neither "
                    "published nor saved")
    try:
        producer = None if dry_run else make_producer(**settings)
        consumer = make_consumer(
            list(by_topic),
            group_id=os.getenv("KAFKA_GROUP_ID", "upm-raphtory"),
            auto_offset_reset=os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest"),
            **settings,
        )
    except Exception as e:
        log.exception("[kafka]   could not connect to Kafka (KAFKA_BOOTSTRAP_SERVERS=%s): %s",
                      settings["bootstrap"], describe_error(e))
        sys.exit(1)                 # already logged with its traceback
    log.info("[kafka]   waiting for requests on %s", ", ".join(by_topic))

    try:
        while True:
            batch = consumer.poll(timeout_ms=int(DELAY * 1000))
            for tp, records in batch.items():
                ctx = by_topic[tp.topic]
                for record in records:
                    try:
                        handle_request(ctx, record, producer)
                    except Exception as e:
                        log.exception("[request] %s@%s: failed (%s), %s", tp.topic,
                                      record.offset, describe_error(e), _saved_copy(out_dir))
                    consumer.commit()       # answered (or skipped): don't redo it on restart
            # no request -> nothing to do: the graph is only read when a request arrives
    except KeyboardInterrupt:
        log.info("[kafka]   stopping")
    except Exception as e:
        log.exception("[kafka]   main loop crashed: %s", describe_error(e))
        sys.exit(1)                 # already logged with its traceback
    finally:
        consumer.close()
        if producer is not None:
            producer.close()


if __name__ == "__main__":
    main()
