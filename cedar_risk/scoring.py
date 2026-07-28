from collections import defaultdict , Counter
from operator import itemgetter

import pandas as pd

from .utils.config import THRESHOLDS
from .utils.score_utils import involvement
from .indicators import group_checks , entity_checks 

from .descriptions import org_description , tender_description , entity_description


REPEAT = THRESHOLDS["REPEAT"]
SUCCESS = THRESHOLDS["SUCCESS"]



ORG_CHECKS = ["repetition", "success_rate", "fake_competition",
              "winner_rotation", "person_consistency"]

ENTITY_CHECKS = ["repetition", "success_rate", "targeting_rate",
                 "official_consistency", "restricted_competition"]



## Cache the score of each organization group to avoid recomputing it
def _checks(g, T, cache):
    if g not in cache:
        cache[g] = group_checks(g, T)
    return cache[g]
 
 
def organization_scores(T, trip, cliques):
    """
    Compute the anomaly score for every organization.

    An organization may belong to one or more detected group.
    Each candidate group is evaluated and the highest
    resulting anomaly score is assigned to the organization.
    
    """

    ## Participants of each tender
    parts = {tid: set(t["participants"]) for tid, t in T.items()} 

    ## Cache previously computed group scores
    cache = {}

    ## Extract the organizations forming each detected clique
    cores = [tuple(c["orgs"]) for c in cliques]


    ## Measure each organization's participation inside every detected clique
    clique_participation = {}
    for g in cores :
        members = set(g)
        ## A clique is considered present in a tender when at least three of
        ## its organizations participate.
        need = min(3, len(members)) 
        hits = Counter()
        for p in parts.values():
            if len(members & p) >= need:
                hits.update(members & p)

        clique_participation[g] = {
            "hits": hits,
            "max_hits": max(hits.values(), default=0),
            }

    ## Collect every 3-organizations motifs each organization belongs to
    by_org = defaultdict(list)
    for tr in trip:
        for o in tr:
            by_org[o].append(tr)

    ## Calculate number of  bids submitted by each organization 
    bids_in = Counter(o for t in T.values() for o in t["participants"])

    result = {}
    for o in sorted(bids_in):
        cands = []

        ## Score the organization through every clique it belongs to
        for clique, participation in clique_participation.items():
            if o not in clique:
                continue
            inv = involvement(participation["hits"][o],participation["max_hits"])
            ch = _checks(clique, T, cache)
            cands.append((ch["overall"] * inv, ch, clique, inv))
   

        ## Score the organization through every triplet it belongs to
        for tr in by_org.get(o, ()):
            ch = _checks(tr, T, cache)
            cands.append((ch["overall"], ch, tr, 1.0))


        ## Keep the strongest anomaly signal among all candidate groups
        if cands:
            overall, ch, group, inv = max(cands, key=itemgetter(0)) ## give the organization the highest anomaly score
        else:
            overall, ch, group, inv = 0.0, {}, None, 0.0

 
        overall = round(float(overall), 3)
        names = list(group) if group else None
        comps = {check: round(float(ch.get(check, 0.0)), 3) for check in ORG_CHECKS}
        comps["involvement"] = round(float(inv), 3)
        result[o] = {"node_type": "Organization", "node_id": o, "overall": overall,
                     "description": org_description(overall, ch, names),
                     "group": names,
                     "components": comps,
                     "bids_submitted": bids_in[o]}
    return result


def entity_scores(T, quad):
    """
    Compute an anomaly score for every public entity.

    For each public entity, the method evaluates the organization groups that
    repeatedly compete in its tenders. Each group is assessed for potential
    favoritism, and the entity inherits the highest anomaly score observed
    among these groups.
    """

    ## Group all detected organization motifs by public entity
    by_entity = defaultdict(list)
    for (entity, group), count in quad.items():
        by_entity[entity].append((group, count))

    ## Count the number of tenders published by each entity
    tenders_by_entity = defaultdict(int)
    for tender in T.values():
        tenders_by_entity[tender["entity"]] += 1

    result = {}

    ## Evaluate every public entity independently
    for entity in sorted({t["entity"] for t in T.values()}):

        ## Keep only the most frequently recurring organization groups
        ## to reduce unnecessary scoring.
        candidate_groups = sorted(
            by_entity.get(entity, []),
            key=lambda x: -x[1]
        )[:3]

        best_score = None
        best_group = None

        ## Score every candidate organization group
        for group, _ in candidate_groups:

            checks = entity_checks(entity, group, T)

            if best_score is None or checks["overall"] > best_score["overall"]:
                best_score = checks
                best_group = group

        ## Entity has no suspicious organization groups
        if best_score is None:
            best_score = {check: 0.0 for check in ENTITY_CHECKS}
            best_score["overall"] = 0.0

        overall = round(float(best_score["overall"]), 3)
        group_names = list(best_group) if best_group else None

        components = {
            check: round(float(best_score.get(check, 0.0)), 3)
            for check in ENTITY_CHECKS
        }

        result[entity] = {
            "node_type": "PublicEntity",
            "node_id": entity,
            "overall": overall,
            "description": entity_description(
                overall,
                best_score,
                group_names,
            ),
            "group": group_names,
            "components": components,
            "tenders_issued": tenders_by_entity[entity],
        }

    return result





def tender_scores(T, org_anom, entity_anom):
    """
    Compute an anomaly score for every tender.

    The tender itself is not analysed directly. Instead, its risk is inherited
    from the anomaly scores already assigned to the participating organizations
    and the issuing public entity.

    Closed tender
    -------------
    The final score is based on:
        - the anomaly score of the winning organization;
        - the anomaly score of the issuing public entity.

    Open tender
    -----------
    Since no winner exists yet, the score estimates the likelihood that a
    suspicious organization group will win.

    """

    result = {}

    for tender_id, tender in T.items():

        participants = tender["participants"]
        entity = tender["entity"]

        winner = tender.get("winner")
        winner = winner if winner in org_anom else None

        open_stage = winner is None



        # Tender still in progress
        if open_stage:

            expected_collusion = 0.0
            coll_group = None
            fav_group = None

            ## Evaluate every suspicious organization group represented
            ## among the current bidders.
            for organization in participants:

                info = org_anom.get(organization)

                if not info:
                    continue

                group = info.get("group")

                if not group:
                    continue

                ## Estimate the probability that this group wins the tender.
                participating_members = set(group) & set(participants)
                probability = ( len(participating_members) / len(participants))
                expected_risk = probability * info["overall"]

                ## Keep the strongest expected collusion signal.
                if expected_risk > expected_collusion:
                    expected_collusion = expected_risk
                    coll_group = group

            collusion = expected_collusion

            ## Favoritism cannot yet be assessed because
            ## the tender has not been awarded.
            favoritism = 0.0

        # Tender already awarded
        else:
            ## The tender inherits its collusion risk from the winning organization.
            winner_info = org_anom.get(winner, {})
            collusion = winner_info.get("overall", 0.0)
            coll_group = winner_info.get("group")

            ## The tender inherits its favoritism risk from the public entity that issued the tender.
            entity_info = entity_anom.get(entity, {})
            favoritism = entity_info.get("overall", 0.0)
            fav_group = entity_info.get("group")


        ## The tender inherits the strongest anomaly signal.
        overall = round(max(collusion, favoritism), 3)

        result[tender_id] = {
            "node_type": "Tender",
            "node_id": tender_id,
            "overall": overall,
            "description": tender_description(
                overall,
                collusion,
                favoritism,
                coll_group,
                fav_group,
                entity,
                winner,
                open_stage,
            ),
            "components": {
                "collusion": round(collusion, 3),
                "favoritism": round(favoritism, 3),
            },
            "explanation": {
                "participants": participants,
                "winning_organization": winner,
                "public_entity": entity,
                "bidding_stage": open_stage,
            },
        }

    return result


