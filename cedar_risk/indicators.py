import numpy as np
import pandas as pd

from  .utils.score_utils import (sim, combine, success_measurement, recurrence,
                                 group_need, reference_price, dominant_share,
                                 is_positive)
from .utils.config import THRESHOLDS


REPEAT = THRESHOLDS["REPEAT"]
BID_GAP = THRESHOLDS["BID_GAP"]

# Every indicator below tolerates incomplete data (missing amounts, prices,
# responsible persons, officials): a missing value is skipped, and an indicator
# with nothing to measure scores 0 instead of failing.


def group_success_rate(group, tenders):
    """
    Compute the proportion of tenders won by the organization group.

    Returns
    -------
    success_rate : float
        Fraction of tenders won by the group.
    winners : list
        Winning organizations, used by other scoring functions.
    """
    winners = [
        tender["winner"]
        for tender in tenders
        if tender["winner"] in group
    ]

    success_rate = len(winners) / len(tenders) if tenders else 0.0

    return success_rate, winners




def fake_competition_score(group, tenders):
    """
    
    Measure whether losing bids from the same group stay consistently
    above the winning bid with a noticable gap (fake competitions).

    Bids without a usable amount, and tenders without a usable reference
    price (awarded price, else the winning bid's own amount), are skipped.
    
    """
    bid_gaps = []

    for tender in tenders:
        reference = reference_price(tender)
        if reference is None:
            continue

        for bid in tender["bids"]:
            if (bid["org"] in group
                    and bid["org"] != tender["winner"]
                    and is_positive(bid.get("amount"))):
                bid_gaps.append((bid["amount"] - reference) / reference)

    average_gap = float(np.mean(bid_gaps)) if bid_gaps else 0.0

    return sim(average_gap, BID_GAP)




def winner_rotation_score(group, winners):
    """
    Measure how evenly victories are distributed across the organizations
    in the group.
    """
    if len(group) <= 1 or not winners:
        return 0.0

    distribution = pd.Series(winners).value_counts(normalize=True)

    entropy = -(distribution * np.log(distribution)).sum()

    return float(abs(entropy / np.log(len(group))))




def person_consistency_score(group, tenders):
    """
    Measure whether the same responsible person repeatedly submits bids
    for the organizations in the group (missing persons are ignored).
    """
    consistency = 0.0

    for org in group:

        people = [
            bid.get("person")
            for tender in tenders
            for bid in tender["bids"]
            if bid["org"] == org
        ]

        consistency = max(consistency, dominant_share(people))

    return consistency


#  Organization Group check for pottential collusive behaviors
def group_checks(group, T):
    """
    Evaluate the collusive behaviour of an organization group.

    The overall score combines:
      - Repetition
      - Success rate
      - Fake competition
      - Winner rotation
      - Responsible-person consistency

    A group is present in a tender when min(GROUP_SIZE, len(group)) of its
    members bid on it, so the same check also scores a pair or a single
    organization (used by the fallback scoring).
    """
    group = set(group)
    need = group_need(group)

    tenders = [
        tender
        for tender in T.values()
        if len(group & set(tender["participants"])) >= need
    ]

    appearances = len(tenders)

    checks = {
        "appearances": appearances,
        "repetition": sim(appearances, REPEAT),
        "group_success_rate": 0.0,
        "success_risk": 0.0,
        "fake_competition": 0.0,
        "winner_rotation": 0.0,
        "person_consistency": 0.0,
        "overall": 0.0,
    }

    if appearances == 0:
        return checks

    success_rate, winners = group_success_rate(group, tenders)

    fake_competition = fake_competition_score(group, tenders)

    winner_rotation = winner_rotation_score(group, winners)

    person_consistency = person_consistency_score(group, tenders)

    recurrence_score = recurrence(
        appearances,
        REPEAT,
        2 * REPEAT,
    )

    combined = combine([
        success_rate,
        fake_competition,
        winner_rotation,
        person_consistency,
        recurrence_score,
    ])

    ## Risk score of the success rate (threshold SUCCESS, discounted when few wins)
    success_risk = success_measurement(success_rate, len(winners))

    overall = (
        checks["repetition"]
        * success_risk
        * combined
    )

    checks.update({
        "group_success_rate": success_rate,
        "success_risk": success_risk,
        "fake_competition": fake_competition,
        "winner_rotation": winner_rotation,
        "person_consistency": person_consistency,
        "overall": overall,
    })

    return checks




#  Public Enities check for potential favorism to certain group of organizations  
def entity_checks(entity, G, T):
    G = set(G)
    need = group_need(G)

    out = {"appearances": 0, "repetition": 0.0, "success_rate": 0.0,
           "success_risk": 0.0, "targeting_rate": 0.0, "official_consistency": 0.0,
           "restricted_competition": 0.0, "overall": 0.0}
    if entity is None or not G:
        return out

    ent_t = [t for t in T.values() if t["entity"] == entity
             and len(G & set(t["participants"])) >= need]
    
    n = len(ent_t)
    out.update({"appearances": n, "repetition": sim(n, REPEAT)})
    if n == 0:
        return out

    ## Calculate winning rate 
    won = [t for t in ent_t if t["winner"] in G]
    sr = len(won) / n


    ## calculate targeting rate 
    group_total = sum(1 for t in T.values() if len(G & set(t["participants"])) >= need)
    targeting_rate = (n / group_total)  if group_total else 0.0
    

    ## Check Public entity Repsonsible person consistency (missing officials are ignored)
    official_consistency = dominant_share([t["official"] for t in won])


    
    distinct = set().union(*[t["participants"] for t in ent_t])
    restricted_competition = (len(G) / len(distinct)) if distinct else 0.0

    combined = combine([sr, targeting_rate, official_consistency,
                          restricted_competition, recurrence(n, REPEAT, 2*REPEAT)])

    ## Risk score of the success rate (threshold SUCCESS, discounted when few wins)
    success_risk = success_measurement(sr, len(won))

    overall = out["repetition"] * success_risk * combined

    out.update({"success_rate": sr, "success_risk": success_risk,
                "targeting_rate": targeting_rate,
                "official_consistency": official_consistency,
                "restricted_competition": restricted_competition, "overall": overall})
    return out