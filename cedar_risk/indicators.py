import numpy as np
import pandas as pd

from  .utils.score_utils import sim, combine, success_measurement, recurrence 
from .utils.config import THRESHOLDS


REPEAT = THRESHOLDS["REPEAT"]
BID_GAP = THRESHOLDS["BID_GAP"]



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
    
    """
    bid_gaps = [
        (bid["amount"] - tender["winner_bid"]) / tender["winner_bid"]
        for tender in tenders
        for bid in tender["bids"]
        if bid["org"] in group
        and bid["org"] != tender["winner"]
        and tender["winner_bid"]
    ]

    average_gap = np.mean(bid_gaps) if bid_gaps else 0.0

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
    for the organizations in the group.
    """
    consistency = 0.0

    for org in group:

        people = [
            bid["person"]
            for tender in tenders
            for bid in tender["bids"]
            if bid["org"] == org
        ]

        if not people:
            continue

        dominant_person = (
            pd.Series(people)
            .value_counts(normalize=True)
            .iloc[0]
        )

        consistency = max(consistency, float(dominant_person))

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
    """
    group = set(group)

    tenders = [
        tender
        for tender in T.values()
        if len(group & set(tender["participants"])) >= min(3, len(group))
    ]

    appearances = len(tenders)

    checks = {
        "appearances": appearances,
        "repetition": sim(appearances, REPEAT),
        "success_rate": 0.0,
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

    overall = (
        checks["repetition"]
        * success_measurement(success_rate, len(winners))
        * combined
    )

    checks.update({
        "success_rate": success_rate,
        "fake_competition": fake_competition,
        "winner_rotation": winner_rotation,
        "person_consistency": person_consistency,
        "overall": overall,
    })

    return checks




#  Public Enities check for potential favorism to certain group of organizations  
def entity_checks(entity, G, T):
    G = set(G)
    ent_t = [t for t in T.values() if t["entity"] == entity
             and len(G & set(t["participants"])) >= 3]
    
    n = len(ent_t)
    out = {"appearances": n, "repetition": sim(n, REPEAT), "success_rate": 0.0,
           "targeting_rate": 0.0, "official_consistency": 0.0,
           "restricted_competition": 0.0, "overall": 0.0}
    if n == 0:
        return out

    ## Calculate winning rate 
    won = [t for t in ent_t if t["winner"] in G]
    sr = len(won) / n


    ## calculate targeting rate 
    group_total = sum(1 for t in T.values() if len(G & set(t["participants"])) >= 3)
    targeting_rate = (n / group_total)  if group_total else 0.0
    

    ## Check Public entity Repsonsible person consistency
    if won:
        offs = pd.Series([t["official"] for t in won])
        official_consistency = float(offs.value_counts(normalize=True).iloc[0])
    else:
        official_consistency = 0.0


    
    distinct = set().union(*[t["participants"] for t in ent_t])
    restricted_competition = (len(G) / len(distinct)) if distinct else 0.0

    combined = combine([sr, targeting_rate, official_consistency,
                          restricted_competition, recurrence(n, REPEAT, 2*REPEAT)])

    overall = out["repetition"] * success_measurement(sr, len(won)) * combined

    out.update({"success_rate": sr, "targeting_rate": targeting_rate,
                "official_consistency": official_consistency,
                "restricted_competition": restricted_competition, "overall": overall})
    return out
