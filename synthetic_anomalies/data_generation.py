"""Synthetic procurement market with the planted anomaly scenarios and the
degradation ladder. Every tender has at least 3 bids."""
import random
from datetime import datetime, timedelta

import numpy as np
import pandas as pd


def generate(seed=42):
    """Returns (df, scenarios): one staging row per bid, plus the planted
    ground truth for validation and labelling."""
    random.seed(seed)
    np.random.seed(seed)


    random.seed(42)
    np.random.seed(42)

    ORGS = [f"Org_{i}" for i in range(20)]            # regular market
    SMALL_ORGS = [f"Org_{i}" for i in range(20, 24)]  # 1-2 bids only
    CONTACT_ORG = "Org_24"                            # bids once, inside a cartel tender
    ENTITIES = [f"Entity_{i}" for i in range(6)]
    SMALL_ENTITY = "Entity_6"                         # publishes only 2 tenders
    ORG_PERSONS = [f"Person_{i:02d}" for i in range(50)]  # 50-69 assigned to ladder orgs below
    OFFICIALS = [f"Official_{i}" for i in range(10)]

    pool = ORG_PERSONS.copy()
    random.shuffle(pool)
    responsible_people = {org: [pool.pop(), pool.pop()]
                          for org in ORGS + SMALL_ORGS + [CONTACT_ORG]}
    entity_officials = {e: random.sample(OFFICIALS, 2) for e in ENTITIES + [SMALL_ENTITY]}

    rows = []
    tender_seq = 0
    BASE = datetime(2025, 1, 1, 8, 0)


    def add_tender(entity, participants, winner, base_price, day,
                   spread_minutes, price_fn, people_fn, official=None):
        nonlocal tender_seq
        tid = f"T{tender_seq}"
        tender_seq += 1
        t0 = BASE + timedelta(days=day, minutes=random.randint(0, 300))
        off = official or random.choice(entity_officials[entity])
        for org in participants:
            rows.append({
                "TenderID": tid,
                "PublicEntity": entity,
                "EntityOfficial": off,
                "Organization": org,
                "BidAmount": round(price_fn(org, winner, base_price), 2),
                "SubmissionTime": t0 + timedelta(minutes=random.uniform(0, spread_minutes)),
                "ResponsiblePerson": people_fn(org),
                "Winner": winner,
            })
        return tid


    # --- background: honest competition, persons rotate ---
    for k in range(35):
        entity = random.choice(ENTITIES)
        participants = random.sample(ORGS, random.randint(3, 6))
        winner = random.choice(participants)
        base_price = random.randint(100_000, 300_000)

        def price(org, win, bp):
            return bp * (random.uniform(0.96, 1.02) if org == win
                         else random.uniform(0.98, 1.15))

        add_tender(entity, participants, winner, base_price,
                   day=random.randint(0, 180), spread_minutes=48 * 60,
                   price_fn=price,
                   people_fn=lambda o: random.choice(responsible_people[o]))

    # --- scenario A: full ring, SAME persons every time ---
    RING_A = ("Org_1", "Org_5", "Org_9")
    FAV_GROUP = ("Org_2", "Org_6", "Org_11")   # defined early: scenario pools stay disjoint
    ring_a_people = {o: responsible_people[o][0] for o in RING_A}
    for i in range(5):
        entity = random.choice(ENTITIES[:3])
        participants = list(RING_A)
        if i < 2:
            participants.append("Org_18")   # touches the cartel twice
        elif i == 2:
            participants.append(CONTACT_ORG)  # single bid, inside a cartel tender
        winner = RING_A[i % 3]
        base_price = random.randint(150_000, 300_000)

        def price(org, win, bp):
            if org == win:
                return bp * random.uniform(0.95, 0.99)
            if org in RING_A:
                return bp * random.uniform(1.25, 1.45)
            return bp * random.uniform(0.99, 1.10)

        add_tender(entity, participants, winner, base_price,
                   day=random.randint(0, 180), spread_minutes=8,
                   price_fn=price,
                   people_fn=lambda o: ring_a_people.get(o, random.choice(responsible_people[o])))

    # --- scenario B: partial ring, persons rotate (org-level pattern only) ---
    RING_B = ("Org_3", "Org_5", "Org_7")
    for i in range(3):
        entity = random.choice(ENTITIES)
        winner = "Org_3" if random.random() < 0.7 else random.choice(RING_B)
        base_price = random.randint(120_000, 250_000)

        def price(org, win, bp):
            if org == win:
                return bp * random.uniform(0.96, 1.0)
            if org in RING_B:
                return bp * random.uniform(1.12, 1.28)
            return bp * random.uniform(0.98, 1.10)

        add_tender(entity, list(RING_B), winner, base_price,
                   day=random.randint(0, 180), spread_minutes=45,
                   price_fn=price,
                   people_fn=lambda o: random.choice(responsible_people[o]))

    # --- scenario C: favoritism, SAME official + SAME org persons ---
    fav_people = {o: responsible_people[o][0] for o in FAV_GROUP}
    FAV_OFFICIAL = entity_officials["Entity_0"][0]
    for i in range(5):
        # Org_16 co-appears WITHOUT completing the core triplet: the case
        # only motif-based clique expansion can absorb
        participants = list(FAV_GROUP)
        if i < 3:
            participants.append("Org_16")
        others = [o for o in ORGS if o not in FAV_GROUP and o not in RING_A
                  and o not in ("Org_16", "Org_18")]
        participants += random.sample(others, 1)
        winner = "Org_2" if random.random() < 0.75 else random.choice(FAV_GROUP)
        base_price = random.randint(180_000, 350_000)

        def price(org, win, bp):
            return bp * (random.uniform(0.95, 0.99) if org == win
                         else random.uniform(1.0, 1.12))

        add_tender("Entity_0", participants, winner, base_price,
                   day=random.randint(0, 180), spread_minutes=24 * 60,
                   price_fn=price,
                   people_fn=lambda o: fav_people.get(o, random.choice(responsible_people[o])),
                   official=FAV_OFFICIAL)

    # --- scenario D: coincidental repeats, persons rotate ---
    COINC = ("Org_13", "Org_14", "Org_15")
    for i in range(2):
        entity = random.choice(ENTITIES)
        pool = [o for o in ORGS if o not in COINC and o not in RING_A
                and o not in FAV_GROUP
                and o not in ("Org_16", "Org_18")]
        participants = list(COINC) + random.sample(pool, 2)
        winner = random.choice(participants)
        base_price = random.randint(100_000, 250_000)

        def price(org, win, bp):
            return bp * (random.uniform(0.96, 1.01) if org == win
                         else random.uniform(0.99, 1.12))

        add_tender(entity, participants, winner, base_price,
                   day=random.randint(0, 180), spread_minutes=72 * 60,
                   price_fn=price,
                   people_fn=lambda o: random.choice(responsible_people[o]))

    # --- scenario F: near-cartel — repeats 4x but only partly matches ---
    NEAR_GROUP = ("Org_10", "Org_12", "Org_14")
    for i in range(4):
        participants = list(NEAR_GROUP)
        if i % 2:
            participants.append(random.choice(
                [o for o in ORGS if o not in NEAR_GROUP and o not in RING_A
                 and o not in FAV_GROUP]))
        winner = (NEAR_GROUP[0], NEAR_GROUP[0], NEAR_GROUP[1])[i] if i < 3 \
            else random.choice([o for o in participants if o not in NEAR_GROUP]
                               or [participants[0]])
        base_price = random.randint(120_000, 250_000)

        def price(org, win, bp):
            if org == win:
                return bp * random.uniform(0.96, 1.0)
            if org in NEAR_GROUP:
                return bp * random.uniform(1.10, 1.18)   # mild padding only
            return bp * random.uniform(0.99, 1.08)

        add_tender(random.choice(ENTITIES), participants, winner, base_price,
                   day=random.randint(0, 180), spread_minutes=6 * 60,
                   price_fn=price,
                   people_fn=lambda o: random.choice(responsible_people[o]))

    # --- new joiners: organizations with exactly 1, 2 and 3 honest bids ---
    NEW_ORGS = ["Org_25", "Org_26", "Org_27"]        # 3, 2, 1 bids respectively
    for org, n_bids in zip(NEW_ORGS, (3, 2, 1)):
        responsible_people[org] = [f"Person_{45 + NEW_ORGS.index(org)}"]
        for _ in range(n_bids):
            participants = [org] + random.sample(
                [o for o in ORGS if o not in RING_A and o not in FAV_GROUP], 3)
            winner = random.choice(participants)
            base_price = random.randint(100_000, 220_000)

            def price(o_, win, bp):
                return bp * (random.uniform(0.96, 1.01) if o_ == win
                             else random.uniform(0.99, 1.10))

            add_tender(random.choice(ENTITIES), participants, winner, base_price,
                       day=random.randint(0, 180), spread_minutes=72 * 60,
                       price_fn=price,
                       people_fn=lambda o_: random.choice(responsible_people[o_]))

    # --- new public entity with a single tender ---
    NEW_ENTITY = "Entity_7"
    entity_officials[NEW_ENTITY] = random.sample(OFFICIALS, 1)
    participants = random.sample([o for o in ORGS if o not in RING_A
                                  and o not in FAV_GROUP], 4)
    winner = random.choice(participants)
    bp0 = random.randint(150_000, 300_000)
    add_tender(NEW_ENTITY, participants, winner, bp0,
               day=random.randint(0, 180), spread_minutes=72 * 60,
               price_fn=lambda o, w, bp: bp * (random.uniform(0.96, 1.01) if o == w
                                               else random.uniform(0.99, 1.10)),
               people_fn=lambda o: random.choice(responsible_people[o]))

    # --- degradation ladder: groups at different distances from the scenarios ---
    # L1: a triplet that appears exactly ONCE, suspicious-looking in that tender
    G_ONCE = ("Org_32", "Org_33", "Org_34")
    # L2: repeats 4x but keeps LOSING to outsiders — repetition without success
    G_LOSE = ("Org_28", "Org_29", "Org_30")
    # L3: halfway — repeats 5x, wins 3 of 5 (60% < 75%), heavy padding, fixed people
    G_HALF = ("Org_35", "Org_36", "Org_37")
    for i, org in enumerate(G_ONCE + G_LOSE + G_HALF):
        responsible_people[org] = [f"Person_{50 + 2 * i}", f"Person_{51 + 2 * i}"]

    winner = G_ONCE[0]
    add_tender(random.choice(ENTITIES), list(G_ONCE), winner,
               random.randint(150_000, 250_000),
               day=random.randint(0, 180), spread_minutes=15,
               price_fn=lambda o, w, bp: bp * (random.uniform(0.95, 0.99) if o == w
                                               else random.uniform(1.28, 1.38)),
               people_fn=lambda o: responsible_people[o][0])

    for i in range(4):
        outsiders = random.sample([o for o in ORGS if o not in RING_A
                                   and o not in FAV_GROUP], 2)
        participants = list(G_LOSE) + outsiders
        winner = outsiders[0] if i < 3 else G_LOSE[0]
        add_tender(random.choice(ENTITIES), participants, winner,
                   random.randint(100_000, 220_000),
                   day=random.randint(0, 180), spread_minutes=24 * 60,
                   price_fn=lambda o, w, bp: bp * (random.uniform(0.96, 1.0) if o == w
                                                   else random.uniform(1.08, 1.15)),
                   people_fn=lambda o: random.choice(responsible_people[o]))

    for i in range(5):
        participants = list(G_HALF)
        if i >= 3:
            participants.append(random.choice(
                [o for o in ORGS if o not in RING_A and o not in FAV_GROUP]))
        winner = G_HALF[i % 3] if i < 3 else participants[-1]
        add_tender(random.choice(ENTITIES), participants, winner,
                   random.randint(120_000, 240_000),
                   day=random.randint(0, 180), spread_minutes=90,
                   price_fn=lambda o, w, bp: bp * (random.uniform(0.95, 0.99) if o == w
                                                   else (random.uniform(1.26, 1.34)
                                                         if o in G_HALF
                                                         else random.uniform(0.99, 1.08))),
                   people_fn=lambda o: responsible_people[o][0] if o in G_HALF
                   else random.choice(responsible_people[o]))

    # --- small participants: organizations with only 1-2 bids ---
    for i in range(3):
        smalls = [SMALL_ORGS[(2 * i) % 4], SMALL_ORGS[(2 * i + 1) % 4]]
        regs = random.sample([o for o in ORGS if o not in RING_A
                              and o not in FAV_GROUP], 2)
        participants = smalls + regs
        winner = random.choice(participants)
        base_price = random.randint(100_000, 250_000)

        def price(org, win, bp):
            return bp * (random.uniform(0.96, 1.01) if org == win
                         else random.uniform(0.99, 1.12))

        add_tender(random.choice(ENTITIES), participants, winner, base_price,
                   day=random.randint(0, 180), spread_minutes=72 * 60,
                   price_fn=price,
                   people_fn=lambda o: random.choice(responsible_people[o]))

    # --- small entity: publishes only 2 honest tenders ---
    for i in range(2):
        participants = random.sample([o for o in ORGS if o not in RING_A
                                      and o not in FAV_GROUP], 4)
        winner = random.choice(participants)
        base_price = random.randint(100_000, 250_000)

        def price(org, win, bp):
            return bp * (random.uniform(0.96, 1.01) if org == win
                         else random.uniform(0.99, 1.12))

        add_tender(SMALL_ENTITY, participants, winner, base_price,
                   day=random.randint(0, 180), spread_minutes=72 * 60,
                   price_fn=price,
                   people_fn=lambda o: random.choice(responsible_people[o]))

    # --- entity favoritism ladder: a gradient of captured public entities ---
    # Built ONLY from new orgs / new officials, appended after every other
    # scenario, so no existing organization score changes. Each entity gets a
    # 3-org ring: ring-won tenders are ring-only, ring-lost tenders add one
    # throwaway winner, and a few losing "away" tenders in Entity_8 pull the
    # ring's targeting rate below 1. n/wins set the band; official/away tune the
    # position inside it. Ring exclusivity (targeting=1) saturates the noisy-OR,
    # so the 0.6-0.8 band is reached with MEDIUM success (wins>=3, rate<0.75)
    # rather than full success, which would otherwise jump straight to ~1.0.
    LADDER = {
        "Entity_1": dict(ring=("Org_50", "Org_51", "Org_52"), n=5, wins=3, away=2, official="two"),
        "Entity_2": dict(ring=("Org_53", "Org_54", "Org_55"), n=6, wins=3, away=2, official="vary"),
        "Entity_3": dict(ring=("Org_56", "Org_57", "Org_58"), n=4, wins=0, away=0, official="none"),
        "Entity_4": dict(ring=("Org_59", "Org_60", "Org_61"), n=2, wins=1, away=4, official="vary"),
        "Entity_5": dict(ring=("Org_62", "Org_63", "Org_64"), n=3, wins=2, away=3, official="two"),
        "Entity_6": dict(ring=("Org_65", "Org_66", "Org_67"), n=4, wins=2, away=6, official="same"),
        "Entity_7": dict(ring=("Org_68", "Org_69", "Org_70"), n=6, wins=4, away=3, official="two"),
    }
    LADDER_SINK = "Entity_8"
    ladder_fill = 80
    ladder_person = 200

    def next_filler():
        nonlocal ladder_fill
        ladder_fill += 1
        return f"Org_{ladder_fill}"

    def next_person():
        nonlocal ladder_person
        ladder_person += 1
        return f"Person_{ladder_person}"

    def loser_price(o, w, bp):
        return bp * (random.uniform(0.95, 0.99) if o == w
                     else random.uniform(1.05, 1.20))

    def cover_price(o, w, bp):
        return bp * (random.uniform(0.95, 0.99) if o == w
                     else random.uniform(1.15, 1.30))

    for entity, cfg in LADDER.items():
        ring = cfg["ring"]
        ring_person = {o: next_person() for o in ring}
        if cfg["official"] == "same":
            offs = [f"{entity}_off0"] * cfg["wins"]
        elif cfg["official"] == "two":
            offs = [f"{entity}_off{i % 2}" for i in range(cfg["wins"])]
        elif cfg["official"] == "vary":
            offs = [f"{entity}_off{i}" for i in range(cfg["wins"])]
        else:
            offs = []

        won = 0
        for i in range(cfg["n"]):
            base_price = random.randint(120_000, 300_000)
            day = random.randint(0, 180)
            if i < cfg["wins"]:
                winner = ring[won % 3]
                people = dict(ring_person)
                add_tender(entity, list(ring), winner, base_price, day, 10,
                           cover_price, lambda o: people[o], official=offs[won])
                won += 1
            else:
                fo = next_filler()
                people = dict(ring_person, **{fo: next_person()})
                add_tender(entity, list(ring) + [fo], fo, base_price, day, 60,
                           loser_price, lambda o: people[o],
                           official=f"{entity}_open{i}")

        for j in range(cfg["away"]):
            base_price = random.randint(120_000, 300_000)
            day = random.randint(0, 180)
            fo = next_filler()
            people = dict(ring_person, **{fo: next_person()})
            add_tender(LADDER_SINK, list(ring) + [fo], fo, base_price, day, 60,
                       loser_price, lambda o: people[o],
                       official=f"{LADDER_SINK}_off{j % 3}")

    # move the 35 background honest tenders (T0..T34) onto dedicated market
    # entities so the ladder entities carry only their designed signal. The
    # entity label never enters organization scoring, so org scores are unchanged.
    MARKET = ("Entity_10", "Entity_11", "Entity_12")
    for r in rows:
        tnum = int(r["TenderID"][1:])
        if tnum < 35:
            r["PublicEntity"] = MARKET[tnum % 3]

    df = pd.DataFrame(rows)
    df["BidID"] = df["TenderID"] + "_" + df["Organization"]


    scenarios = {
        "ring": RING_A, "ring3": RING_B,
        "favoritism": FAV_GROUP, "fav_absorbed": "Org_16",
        "fav_official": FAV_OFFICIAL, "near": NEAR_GROUP,
        "once": G_ONCE, "lose": G_LOSE, "half": G_HALF,
        "coincidence": COINC, "small": tuple(SMALL_ORGS),
        "new": tuple(NEW_ORGS), "contact": CONTACT_ORG,
        "peripheral": "Org_18", "small_entity": SMALL_ENTITY,
        "new_entity": NEW_ENTITY,
        "entity_ladder": {e: cfg["ring"] for e, cfg in LADDER.items()},
        "entity_ladder_sink": LADDER_SINK,
        "market_entities": MARKET,
    }
    return df, scenarios