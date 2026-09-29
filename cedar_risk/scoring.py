from collections import defaultdict , Counter
from operator import itemgetter

import pandas as pd

from .utils.config import THRESHOLDS
from .utils.score_utils import involvement, sim, GROUP_SIZE
from .indicators import group_checks , entity_checks 
from .motifs import org_motifs, entity_group_motifs

from .descriptions import org_description , tender_description , entity_description


REPEAT = THRESHOLDS["REPEAT"]
SUCCESS = THRESHOLDS["SUCCESS"]



ORG_CHECKS = ["repetition", "group_success_rate", "fake_competition",
              "winner_rotation", "person_consistency"]

ENTITY_CHECKS = ["repetition", "success_rate", "targeting_rate",
                 "official_consistency", "restricted_competition"]


# --------------------------------------------------------------------------- #
# How every node gets a score
# --------------------------------------------------------------------------- #
# The original scenarios (collusion between a group of GROUP_SIZE = 3
# organizations, favoritism between an entity and such a group) are kept
# exactly as they are and decide whenever the node has evidence for them.
#
# When the data has too few connections for that (tenders with only 1-2
# bidders), the node is scored with the SAME indicators on a smaller group --
# first a pair of organizations, then a single organization -- and the result
# is discounted by how far that group is from the scenario:
#
#       closeness(k) = sim(k, GROUP_SIZE) = min(k / 3, 1)^2
#           group of 3 -> 1.00   (original scenario, no discount)
#           group of 2 -> 0.44
#           single     -> 0.11
#
# so a node always gets a score, and a weaker structure can never look more
# suspicious than the original scenario it approximates.
# --------------------------------------------------------------------------- #

def _basis(k):
    return "single organization" if k == 1 else f"group of {k}"


## Cache the score of each organization group to avoid recomputing it
def _checks(g, T, cache):
    if g not in cache:
        cache[g] = group_checks(g, T)
    return cache[g]
 
 
def organization_scores(T, trip, cliques, all_orgs=()):
    """
    Compute the anomaly score for every organization.

    An organization may belong to one or more detected group.
    Each candidate group is evaluated and the highest
    resulting anomaly score is assigned to the organization.

    Organizations with no triplet evidence are scored on pairs of co-bidders,
    then on their own bidding record (discounted by closeness to the
    original scenario). `all_orgs` adds organizations that never bid
    (score 0), so that every organization in the graph gets a score.
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
        need = min(GROUP_SIZE, len(members)) 
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

    ## The organization's own success rate (its wins / its own appearances) --
    ## not to be confused with `group_success_rate` in the components below,
    ## which is the success rate of a detected GROUP the organization belongs to.
    wins_by_org = Counter(t["winner"] for t in T.values() if t.get("winner"))

    ## Fallback groups (pairs, then the organization alone), largest first
    fallback = {}
    for k in range(GROUP_SIZE - 1, 0, -1):
        by_k = defaultdict(list)
        if k == 1:
            for o in bids_in:
                by_k[o].append((o,))
        else:
            for grp in org_motifs(T, size=k):
                for o in grp:
                    by_k[o].append(grp)
        fallback[k] = by_k

    result = {}
    for o in sorted(set(bids_in) | set(all_orgs)):
        cands = []              # (score, checks, group, involvement, closeness)
        basis = _basis(GROUP_SIZE)

        ## Score the organization through every clique it belongs to
        for clique, participation in clique_participation.items():
            if o not in clique:
                continue
            inv = involvement(participation["hits"][o],participation["max_hits"])
            ch = _checks(clique, T, cache)
            cands.append((ch["overall"] * inv, ch, clique, inv, 1.0))
   

        ## Score the organization through every triplet it belongs to
        for tr in by_org.get(o, ()):
            ch = _checks(tr, T, cache)
            cands.append((ch["overall"], ch, tr, 1.0, 1.0))


        ## No triplet evidence: fall back to smaller groups, discounted
        if not cands:
            for k, by_k in fallback.items():
                closeness = sim(k, GROUP_SIZE)
                for grp in by_k.get(o, ()):
                    ch = _checks(grp, T, cache)
                    cands.append((ch["overall"] * closeness, ch, grp, 1.0, closeness))
                if cands:
                    basis = _basis(k)
                    break


        ## Keep the strongest anomaly signal among all candidate groups
        if cands:
            overall, ch, group, inv, closeness = max(cands, key=itemgetter(0)) ## give the organization the highest anomaly score
        else:
            overall, ch, group, inv, closeness = 0.0, {}, None, 0.0, 0.0
            basis = "no bids"

 
        overall = round(float(overall), 3)
        names = list(group) if group else None
        comps = {check: round(float(ch.get(check, 0.0)), 3) for check in ORG_CHECKS}
        comps["involvement"] = round(float(inv), 3)
        comps["closeness"] = round(float(closeness), 3)
        success_rate = round(wins_by_org[o] / bids_in[o], 3) if bids_in[o] else 0.0
        result[o] = {"node_type": "Organization", "node_id": o, "overall": overall,
                     "description": org_description(overall, ch, names),
                     "group": names,
                     "components": comps,
                     "basis": basis,
                     "success_rate": success_rate,
                     "bids_submitted": bids_in[o]}
    return result


def entity_scores(T, quad, all_entities=()):
    """
    Compute an anomaly score for every public entity.

    For each public entity, the method evaluates the organization groups that
    repeatedly compete in its tenders. Each group is assessed for potential
    favoritism, and the entity inherits the highest anomaly score observed
    among these groups.

    An entity with no triplet evidence is scored on pairs, then on single
    winning organizations (discounted by closeness to the original
    scenario). `all_entities` adds entities that issued no tender (score 0).
    Tenders without an issuing entity are ignored here.
    """

    ## Entity + organization-group motifs, original size first, then smaller
    quads = {GROUP_SIZE: quad}
    for k in range(GROUP_SIZE - 1, 0, -1):
        quads[k] = entity_group_motifs(T, size=k)

    by_entity_k = {}
    for k, q in quads.items():
        by_entity = defaultdict(list)
        for (entity, group), count in q.items():
            by_entity[entity].append((group, count))
        by_entity_k[k] = by_entity

    ## Count the number of tenders published by each entity
    tenders_by_entity = defaultdict(int)
    for tender in T.values():
        if tender["entity"] is not None:
            tenders_by_entity[tender["entity"]] += 1

    result = {}

    ## Evaluate every public entity independently
    for entity in sorted(set(tenders_by_entity) | set(all_entities)):

        best_score = None
        best_group = None
        closeness = 0.0
        basis = "no closed tenders"

        ## Original scenario first; smaller groups only if it has no evidence
        for k, by_entity in by_entity_k.items():

            ## Keep only the most frequently recurring organization groups
            ## to reduce unnecessary scoring.
            candidate_groups = sorted(
                by_entity.get(entity, []),
                key=lambda x: -x[1]
            )[:3]

            k_closeness = sim(k, GROUP_SIZE)

            ## Score every candidate organization group
            for group, _ in candidate_groups:

                checks = dict(entity_checks(entity, group, T))
                checks["overall"] = checks["overall"] * k_closeness

                if best_score is None or checks["overall"] > best_score["overall"]:
                    best_score = checks
                    best_group = group

            if best_score is not None:
                closeness, basis = k_closeness, _basis(k)
                break

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
        components["closeness"] = round(float(closeness), 3)

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
            "basis": basis,
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

    A tender with a missing entity, winner or bidders still gets a score from
    whatever is available (0 when nothing is).
    """

    result = {}

    for tender_id, tender in T.items():

        participants = tender["participants"]
        entity = tender["entity"]

        winner = tender.get("winner")
        winner = winner if winner in org_anom else None

        open_stage = winner is None

        coll_group = None
        fav_group = None


        # Tender still in progress
        if open_stage:

            expected_collusion = 0.0

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