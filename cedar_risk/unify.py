"""Unified values for the risk-score module.

The three pilots store the same things differently (Successful_Bid is a
boolean in IT, "Yes"/"[HANDWRITTEN]" in SL, 1/0 in UA; prices are strings in
IT and floats elsewhere). The used properties are converted IN PLACE: they keep
their own name in the node, only the value gets a fixed type. No copies, no
extra properties. Everything else is left exactly as stored.

    bid     Successful_Bid           bool   always present; missing / unknown -> False
            Contract_Price           float  dropped if unreadable
            Responsible_Signee_Name  str
    tender  Winning_Bid              str
            Awarded_Price            float  dropped if unreadable
            Responsible_Signee_Name  str

The name is the one found in the node (UA writes Contract_price, SuccessfulBid,
...). Only a Successful_Bid that is missing altogether is created, as
`Successful_Bid = False`.
"""

import math
from datetime import datetime, timezone

TRUE_VALUES = {"true", "yes", "y", "1", "1.0"}


def norm(s):
    """Property / label name key: ignores case and underscores."""
    return str(s).replace("_", "").lower()


def pick(props, *names):
    """First non-None property among `names`, ignoring case and underscores."""
    keyed = {norm(k): v for k, v in props.items()}
    for n in names:
        v = keyed.get(norm(n))
        if v is not None:
            return v
    return None


def to_bool(v):
    """Successful-bid flag -> bool. Only an explicit yes/true/1 is True;
    missing, empty, 0, "No", "[HANDWRITTEN]", anything else -> False."""
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v == 1
    return str(v).strip().lower() in TRUE_VALUES


def to_float(v):
    """Best-effort number, None if unreadable.
    '81967.21' -> 81967.21 ; '0,0' -> 0.0 ; '1.234,56' -> 1234.56 ;
    '6.931.500.427' -> 6931500427.0 ; '1,234' -> 1.234 (decimal comma)"""
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v) if math.isfinite(v) else None
    s = str(v).strip().replace(" ", "")
    if not s:
        return None
    if "," in s and "." in s:
        dec = "," if s.rfind(",") > s.rfind(".") else "."
        thou = "." if dec == "," else ","
        s = s.replace(thou, "").replace(dec, ".")
    elif "," in s:
        s = s.replace(",", ".")
    elif s.count(".") > 1:
        s = s.replace(".", "")
    try:
        x = float(s)
    except ValueError:
        return None
    return x if math.isfinite(x) else None


def parse_time(v):
    """ISO date/datetime or dd/mm/yyyy -> naive UTC datetime, else None."""
    if v is None:
        return None
    if isinstance(v, datetime):
        dt = v
    else:
        s = str(v).strip()
        if not s:
            return None
        dt = None
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            for fmt in ("%d/%m/%Y", "%d/%m/%Y %H:%M:%S"):
                try:
                    dt = datetime.strptime(s, fmt)
                    break
                except ValueError:
                    pass
        if dt is None:
            return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _text(v):
    return str(v)


# label -> [(property name, converter, name to create if missing or None)]
FIELDS = {
    "bid": [("successful_bid", to_bool, "Successful_Bid"),
            ("contract_price", to_float, None),
            ("responsible_signee_name", _text, None)],
    "tender": [("winning_bid", _text, None),
               ("awarded_price", to_float, None),
               ("responsible_signee_name", _text, None)],
}


def unify_props(label, props):
    """Copy of `props` (None values removed) with the used properties converted
    in place to their fixed type. Unreadable numbers are dropped; a bid with no
    success flag gets Successful_Bid = False."""
    out = {k: v for k, v in (props or {}).items() if v is not None}
    for field, conv, create_as in FIELDS.get(norm(label or ""), []):
        keys = [k for k in out if norm(k) == norm(field)]
        if not keys and create_as:
            out[create_as] = conv(None)
        for k in keys:
            v = conv(out[k])
            if v is None:
                del out[k]
            else:
                out[k] = v
    return out