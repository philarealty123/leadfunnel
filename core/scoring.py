import structlog
log = structlog.get_logger()

SIGNAL_WEIGHTS = {
    "sheriff_sale":      55,
        "tax_delinquent":    40,
            "pre_foreclosure":   45,
                "probate":           35,
                    "code_violation":    25,
                        "eviction":          20,
                            "absentee":          10,
                                "absentee+tax":      15,
                                    "absentee+violation":10,
                                        "probate+absentee":  12,
                                            "multi_signal":       8,
                                            }


                                            def score_to_priority(score):
                                                if score >= 70:
                                                        return "HOT"
                                                            elif score >= 50:
                                                                    return "HIGH"
                                                                        elif score >= 30:
                                                                                return "MEDIUM"
                                                                                    else:
                                                                                            return "LOW"


                                                                                            def compute_score(record, existing_signals=None):
                                                                                                signals = set()
                                                                                                    score = 0

                                                                                                        source_type = record.get("source_type", "")
                                                                                                            if source_type in SIGNAL_WEIGHTS:
                                                                                                                    signals.add(source_type)
                                                                                                                            score += SIGNAL_WEIGHTS[source_type]

                                                                                                                                if existing_signals:
                                                                                                                                        for s in existing_signals:
                                                                                                                                                    if s in SIGNAL_WEIGHTS and s not in signals:
                                                                                                                                                                    signals.add(s)
                                                                                                                                                                                    score += SIGNAL_WEIGHTS[s]

                                                                                                                                                                                        if "absentee" in signals and "tax_delinquent" in signals:
                                                                                                                                                                                                score += SIGNAL_WEIGHTS["absentee+tax"]
                                                                                                                                                                                                    if "absentee" in signals and "code_violation" in signals:
                                                                                                                                                                                                            score += SIGNAL_WEIGHTS["absentee+violation"]
                                                                                                                                                                                                                if "probate" in signals and ("absentee" in signals or "tax_delinquent" in signals):
                                                                                                                                                                                                                        score += SIGNAL_WEIGHTS["probate+absentee"]
                                                                                                                                                                                                                            if len(signals) >= 3:
                                                                                                                                                                                                                                    score += SIGNAL_WEIGHTS["multi_signal"]

                                                                                                                                                                                                                                        score = min(score, 100)
                                                                                                                                                                                                                                            return score, score_to_priority(score)