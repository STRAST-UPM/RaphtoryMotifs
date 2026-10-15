"""Scenario thresholds and storage settings.

Every threshold is a statement about the anomaly SCENARIOS of the CEDAR
methodology.
"""

THRESHOLDS = {
    "REPEAT": 3,        # motif recurring >= 3 => suspicious group (extraction)
    "SUCCESS": 0.75,    # success rate >= 75% 
    "BID_GAP": 0.25,    # avg gap winner vs others > 25% => fake competition
    "WINS_MIN": 3,      # rates/patterns over fewer than 3 wins are not evidence
    "SUSPICIOUS": 0.6,
}

DELAY = 1  # seconds main.py waits on Kafka per poll before polling again