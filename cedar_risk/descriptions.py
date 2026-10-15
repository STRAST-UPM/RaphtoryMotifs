from .utils.config import  THRESHOLDS

SUSPICIOUS = THRESHOLDS["SUSPICIOUS"]

def suspicious_behaviors(behaviors):
    """
    Return the suspicious behaviours whose score exceeds the reporting threshold.
    """
    return [
        description
        for score, description in behaviors
        if score >= SUSPICIOUS
    ]


# --------------------------------------------------------------------------- #
# Organization description
# --------------------------------------------------------------------------- #

def org_description(overall, ch, names):
    """Generate the description for an organization anomaly."""

    if overall < SUSPICIOUS:
        return None

    group = ", ".join(names) if names else "an organization group"

    reasons = [
        f"high joint success rate ({ch.get('group_success_rate', 0.0):.0%})",
        *suspicious_behaviors([
            (ch.get("fake_competition", 0.0), "inflated cover bids"),
            (ch.get("winner_rotation", 0.0), "winner rotation"),
            (ch.get("person_consistency", 0.0), "Consistent responsible person"),
        ]),
    ]

    if names and len(names) == 1:
        subject = f"suspicious bidding pattern of {group}"
    else:
        subject = f"potential collusion between {group}"

    return f"{subject} ({'; '.join(reasons)})"


# --------------------------------------------------------------------------- #
# Public entity description
# --------------------------------------------------------------------------- #

def entity_description(overall, ch, names):
    """Generate the description for a public entity anomaly."""

    if overall < SUSPICIOUS:
        return None

    group = ", ".join(names) if names else "an organization group"

    reasons = [
        f"high group success in this entity's tenders ({ch.get('success_rate', 0.0):.0%})",
        *suspicious_behaviors([
            (ch.get("targeting_rate", 0.0), "group repeatedly targets this entity"),
            (ch.get("official_consistency", 0.0), "same Entity responsible person awards the tenders"),
            (ch.get("restricted_competition", 0.0), "restricted competition"),
        ]),
    ]

    return (
        f"potential favoritism between this public entity and {group} "
        f"({'; '.join(reasons)})"
    )


# --------------------------------------------------------------------------- #
# Tender description
# --------------------------------------------------------------------------- #

def tender_description(
    overall,
    collusion,
    favoritism,
    coll_group,
    fav_group,
    entity,
    open_stage,
):
    """Generate the description for a tender anomaly."""

    if overall < SUSPICIOUS:
        return None

    messages = []

    if collusion >= SUSPICIOUS and coll_group:
        group = ", ".join(coll_group)

        if open_stage:
            messages.append(
                f"potential collusion involving {group} among the bidders"
            )
        else:
            messages.append(
                f"winning organization belongs to the suspicious group ({group})"
            )

    if favoritism >= SUSPICIOUS and fav_group:
        group = ", ".join(fav_group)

        if open_stage:
            messages.append(
                f"{entity} shows potential favoritism toward {group}"
            )
        else:
            messages.append(
                f"winning organization belongs to a group favored by {entity}"
            )

    if not messages:
        return "potential tender anomaly"

    description = "; ".join(messages)

    if open_stage:
        return f"open tender; {description}"

    return description