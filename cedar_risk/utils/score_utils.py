import math
from collections import Counter

from .config import THRESHOLDS


REPEAT = THRESHOLDS["REPEAT"]
SUCCESS = THRESHOLDS["SUCCESS"]
WINS_MIN = THRESHOLDS["WINS_MIN"]

# Size of the organization group of the original scenarios (triplets: three
# organizations bidding on the same tender). Smaller groups (pairs, then a single
# organization) are only used as fallbacks, discounted by how far they are from
# this scenario -- see scoring.py.
GROUP_SIZE = THRESHOLDS.get("GROUP_SIZE", 3)


def group_need(group):
    """A group is 'present' in a tender when at least this many of its members bid."""
    return min(GROUP_SIZE, len(group))


# --------------------------------------------------------------------------- #
# Tolerant helpers for incomplete data
# --------------------------------------------------------------------------- #

def is_positive(x):
    """True for a real, finite, strictly positive number (None / NaN / 0 / text -> False)."""
    return (
        isinstance(x, (int, float))
        and not isinstance(x, bool)
        and math.isfinite(x)
        and x > 0
    )


def reference_price(tender):
    """
    Price the losing bids are compared with: the awarded price, or -- when it is
    missing or not usable -- the amount of the winning bid itself. None if neither exists.
    """
    if is_positive(tender.get("winner_bid")):
        return tender["winner_bid"]
    for bid in tender.get("bids", []):
        if (bid.get("successful") and bid.get("org") == tender.get("winner")
                and is_positive(bid.get("amount"))):
            return bid["amount"]
    return None


def dominant_share(values):
    """Share of the most frequent value, ignoring missing / blank values (0.0 if none)."""
    values = [v for v in values if v is not None and v == v and str(v).strip() != ""]
    if not values:
        return 0.0
    return Counter(values).most_common(1)[0][1] / len(values)


# --------------------------------------------------------------------------- #
# Scoring functions (unchanged principle)
# --------------------------------------------------------------------------- #

def sim(x, target):
    """
    Similarity to a scenario threshold.

    Returns:
        min(x / target, 1)^2
    """
    if x is None or not math.isfinite(x):
        return 0.0
    return float(min(max(x, 0) / target, 1.0) ** 2)



def involvement(k, max_k=10, minimum=0.8):
    """
    Measure an organization's involvement inside a detected clique.
    """
    if not max_k or max_k <= 0:
        return minimum
    ratio = min(max(k, 0) / max_k, 1.0)
    return minimum + (1.0 - minimum) * ratio**2



def recurrence(x, threshold, span):
    """
    Measure how much a group's recurrence exceeds the minimum threshold.

    The score is zero below the threshold and increases smoothly until it
    reaches one.
    """
    return float(min(max(x - threshold, 0.0) / span, 1.0) ** 2)


def combine(values):
    """
    Combine multiple suspicious behaviours into a single score.

    Each additional suspicious behaviour increases the final score.

    Used to combine:

     For Organizations :
        - Success rate
        - Fake competition
        - Winner rotation
        - Responsible person consistency
        - Group recurrence

        
     For Public entities :
        - Success rate
        - High Targeting Rate 
        - Responsible person consistency
        - Restricted Competition
        - High Recuurence          
    """
    prod = 1.0

    for value in values:
        prod *= (1.0 - min(max(float(value), 0.0), 1.0))

    return 1.0 - prod



def success_measurement(sr, wins):
    """
    Measure how credible a group's success is.

    The score depends on:
        - the group's success rate;
        - the number of wins supporting that success rate.

    Groups with very few wins are discounted even if their success rate is high.
    """
    attain = (
        1.0
        if sr >= SUCCESS
        else (sr / SUCCESS) ** 2
    )

    return attain * sim(wins, WINS_MIN)