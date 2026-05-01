import structlog
log = structlog.get_logger()

SIGNAL_WEIGHTS = {
    "sheriff_sale":       55,
    "tax_delinquent":     40,
    "pre_foreclosure":    45,
    "probate":            35,
    "code_violation":     25,
    "eviction":           20,
    "absentee":           10,
    "absentee+tax":       15,
    "absentee+violation": 10,
    "probate+absentee":   12,
    "multi_signal":        8,
}


def score_to_priority(score):
    if score >= 70:
        return "HOT"
    elif score >= 50:
        return "WARM"
    elif score >= 30:
        return "COOL"
    else:
        return "COLD"


def compute_score(record: dict) -> tuple:
    """Return (score, priority) for a record based on its source_type."""
    source_type = record.get("source_type", "")
    signals = record.get("signals", [source_type] if source_type else [])
    score = 0
    best_signal = None
    best_weight = 0
    for sig in signals:
        w = SIGNAL_WEIGHTS.get(sig, 0)
        if w > best_weight:
            best_weight = w
            best_signal = sig
    if len(signals) > 1:
        score = best_weight + SIGNAL_WEIGHTS.get("multi_signal", 0)
    else:
        score = best_weight
    priority = score_to_priority(score)
    return score, priority


def score_lead(record: dict) -> dict:
    signals = record.get("signals", [])
    score = 0
    category = "unknown"
    best_weight = 0
    best_signal = None
    for sig in signals:
        w = SIGNAL_WEIGHTS.get(sig, 0)
        if w > best_weight:
            best_weight = w
            best_signal = sig
    if len(signals) > 1:
        score = best_weight + SIGNAL_WEIGHTS.get("multi_signal", 0)
        category = "multi_signal"
    elif best_signal:
        score = best_weight
        category = best_signal
    else:
        score = 0
        category = "unknown"
    priority = score_to_priority(score)
    return {
        **record,
        "motivation_score": score,
        "motivation_category": category,
        "priority": priority,
    }
