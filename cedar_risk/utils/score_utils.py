from .config import THRESHOLDS


REPEAT = THRESHOLDS["REPEAT"]
SUCCESS = THRESHOLDS["SUCCESS"]
WINS_MIN = THRESHOLDS["WINS_MIN"]


def sim(x, target):
    """
    Similarity to a scenario threshold.

    Returns:
        min(x / target, 1)^2
    """
    return float(min(max(x, 0) / target, 1.0) ** 2)



def involvement(k, max_k=10,minimum=0.8):
    """
    Measure an organization's involvement inside a detected clique.
    """

    assert k >= REPEAT, (
    f"Organization should have at least {REPEAT} participations to belong to a clique."
)
    ratio = k / max_k
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