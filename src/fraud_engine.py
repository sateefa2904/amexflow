"""
fraud_engine.py
AmexFlow Fraud Signal Engine — detects fraud patterns post-validation.

Four detection layers:
  1. Velocity detection    — same cardholder, N+ txns within time window
  2. Geographic impossible — physically impossible travel between transactions
  3. Category deviation    — transaction deviates from cardholder's spend profile
  4. Spend outlier         — transaction amount is statistical outlier vs. history

Each signal contributes to a 0-100 FRAUD_RISK_SCORE.
High-risk transactions are flagged for review without being blocked —
mirroring production fraud systems that balance false-positive cost
against fraud loss.

Output: feature-enriched records ready for ML model consumption.
"""

import csv
import json
import math
import statistics
from collections import defaultdict
from datetime import datetime, timedelta

# Risk score weights per signal (must sum to 100)
SIGNAL_WEIGHTS = {
    "VELOCITY":           30,
    "GEO_IMPOSSIBLE":     35,
    "CATEGORY_DEVIATION": 20,
    "SPEND_OUTLIER":      15,
}

VELOCITY_WINDOW_SECONDS = 300   # 5-minute window
VELOCITY_THRESHOLD      = 3     # 3+ transactions = flag
GEO_IMPOSSIBLE_KM       = 3000  # > 3000km in < 2 hours
GEO_IMPOSSIBLE_HOURS    = 2
CATEGORY_DEVIATION_THRESHOLD = 0.15  # flag if < 15% of cardholder's history
SPEND_OUTLIER_Z         = 2.5   # z-score threshold

COUNTRY_COORDS = {
    "US": (37.09, -95.71), "CA": (56.13, -106.34),
    "GB": (55.37, -3.43),  "DE": (51.16, 10.45),
    "FR": (46.22, 2.21),   "AU": (-25.27, 133.77),
    "RU": (61.52, 105.31), "CN": (35.86, 104.19),
}

RISK_TIERS = {
    (0,  25):  "LOW",
    (25, 55):  "MEDIUM",
    (55, 80):  "HIGH",
    (80, 101): "CRITICAL",
}


def haversine_km(c1, c2):
    lat1, lon1 = map(math.radians, c1)
    lat2, lon2 = map(math.radians, c2)
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(dlon/2)**2
    return 6371 * 2 * math.asin(math.sqrt(a))


def parse_dt(ts):
    try:
        return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def get_risk_tier(score):
    for (lo, hi), tier in RISK_TIERS.items():
        if lo <= score < hi:
            return tier
    return "CRITICAL"


def build_cardholder_history(records):
    """
    Build per-cardholder spend and category history from clean records.
    Used for deviation detection.
    """
    history = defaultdict(lambda: {
        "amounts": [], "categories": defaultdict(int), "timestamps": [], "countries": []
    })
    for r in records:
        cid = r.get("cardholder_id", "")
        try:
            amt = float(r.get("amount_usd", 0))
        except (ValueError, TypeError):
            continue
        ts = parse_dt(r.get("timestamp", ""))
        history[cid]["amounts"].append(amt)
        history[cid]["categories"][r.get("merchant_category", "Unknown")] += 1
        if ts:
            history[cid]["timestamps"].append(ts)
        history[cid]["countries"].append(r.get("country_code", ""))
    return history


def detect_velocity(record, history):
    """
    Flag if cardholder has N+ transactions within VELOCITY_WINDOW_SECONDS.
    Returns (signal_fired: bool, details: str)
    """
    cid = record.get("cardholder_id")
    ts = parse_dt(record.get("timestamp", ""))
    if not ts or cid not in history:
        return False, ""
    timestamps = history[cid]["timestamps"]
    window_start = ts - timedelta(seconds=VELOCITY_WINDOW_SECONDS)
    txns_in_window = sum(1 for t in timestamps if window_start <= t <= ts)
    if txns_in_window >= VELOCITY_THRESHOLD:
        return True, f"{txns_in_window}_txns_in_{VELOCITY_WINDOW_SECONDS}s"
    return False, ""


def detect_geo_impossible(record, history):
    """
    Flag if cardholder's current country is physically impossible
    given their last transaction location and time elapsed.
    """
    cid = record.get("cardholder_id")
    current_country = record.get("country_code", "")
    current_ts = parse_dt(record.get("timestamp", ""))
    if not current_ts or cid not in history or not history[cid]["timestamps"]:
        return False, ""

    countries = history[cid]["countries"]
    timestamps = history[cid]["timestamps"]
    if not countries or not timestamps:
        return False, ""

    last_country = countries[-1]
    last_ts = timestamps[-1]
    if last_country == current_country:
        return False, ""

    hours_elapsed = (current_ts - last_ts).total_seconds() / 3600
    if hours_elapsed <= 0 or hours_elapsed > GEO_IMPOSSIBLE_HOURS:
        return False, ""

    if last_country in COUNTRY_COORDS and current_country in COUNTRY_COORDS:
        dist_km = haversine_km(COUNTRY_COORDS[last_country], COUNTRY_COORDS[current_country])
        speed_kmh = dist_km / hours_elapsed if hours_elapsed > 0 else 999999
        if dist_km > GEO_IMPOSSIBLE_KM and hours_elapsed < GEO_IMPOSSIBLE_HOURS:
            return True, f"{dist_km:.0f}km_in_{hours_elapsed:.1f}h ({speed_kmh:.0f}km/h)"
    return False, ""


def detect_category_deviation(record, history):
    """
    Flag if current merchant category is rare in cardholder's history.
    Category must appear in < CATEGORY_DEVIATION_THRESHOLD of their transactions.
    """
    cid = record.get("cardholder_id")
    current_cat = record.get("merchant_category", "")
    if cid not in history:
        return False, ""
    cat_counts = history[cid]["categories"]
    total = sum(cat_counts.values())
    if total < 5:
        return False, ""
    cat_freq = cat_counts.get(current_cat, 0) / total
    if cat_freq < CATEGORY_DEVIATION_THRESHOLD:
        return True, f"{current_cat}_freq={cat_freq:.2%}_of_history"
    return False, ""


def detect_spend_outlier(record, history):
    """
    Flag if transaction amount is a statistical outlier vs cardholder's history.
    Uses z-score — flags if |z| > SPEND_OUTLIER_Z.
    """
    cid = record.get("cardholder_id")
    try:
        amount = float(record.get("amount_usd", 0))
    except (ValueError, TypeError):
        return False, ""
    if cid not in history or len(history[cid]["amounts"]) < 5:
        return False, ""
    amounts = history[cid]["amounts"]
    mean = statistics.mean(amounts)
    std = statistics.stdev(amounts)
    if std == 0:
        return False, ""
    z = (amount - mean) / std
    if abs(z) > SPEND_OUTLIER_Z:
        return True, f"amount=${amount:.2f}_z={z:.2f}_vs_avg=${mean:.2f}"
    return False, ""


def score_transaction(signals_fired):
    """Compute 0-100 fraud risk score from fired signals."""
    return min(100, sum(SIGNAL_WEIGHTS.get(s, 0) for s in signals_fired))


def run_fraud_engine(clean_path="data/clean_transactions.csv",
                     output_path="data/fraud_scored_transactions.csv",
                     report_path="data/fraud_report.json"):

    print(f"\n[fraud_engine] Loading clean records from {clean_path}...")
    with open(clean_path, newline="") as f:
        records = list(csv.DictReader(f))
    print(f"[fraud_engine] {len(records)} records loaded")

    # Sort by cardholder + timestamp for accurate velocity/geo checks
    records.sort(key=lambda r: (r.get("cardholder_id", ""), r.get("timestamp", "")))

    # Build history incrementally — each record scored against history BEFORE it
    history = defaultdict(lambda: {
        "amounts": [], "categories": defaultdict(int),
        "timestamps": [], "countries": []
    })

    scored = []
    signal_counts = defaultdict(int)
    risk_tier_counts = defaultdict(int)

    for record in records:
        cid = record.get("cardholder_id", "")
        signals = []
        details = []

        # Run all 4 detectors
        fired, d = detect_velocity(record, history)
        if fired:
            signals.append("VELOCITY")
            details.append(f"VELOCITY:{d}")
            signal_counts["VELOCITY"] += 1

        fired, d = detect_geo_impossible(record, history)
        if fired:
            signals.append("GEO_IMPOSSIBLE")
            details.append(f"GEO:{d}")
            signal_counts["GEO_IMPOSSIBLE"] += 1

        fired, d = detect_category_deviation(record, history)
        if fired:
            signals.append("CATEGORY_DEVIATION")
            details.append(f"CAT:{d}")
            signal_counts["CATEGORY_DEVIATION"] += 1

        fired, d = detect_spend_outlier(record, history)
        if fired:
            signals.append("SPEND_OUTLIER")
            details.append(f"SPEND:{d}")
            signal_counts["SPEND_OUTLIER"] += 1

        # Score and tier
        score = score_transaction(signals)
        tier = get_risk_tier(score)
        risk_tier_counts[tier] += 1

        # Enrich record with fraud features (ML feature store columns)
        record["fraud_signals"]          = "|".join(signals) if signals else "NONE"
        record["fraud_signal_count"]     = len(signals)
        record["fraud_risk_score"]       = score
        record["fraud_risk_tier"]        = tier
        record["fraud_signal_details"]   = " | ".join(details) if details else ""
        record["is_velocity_flag"]       = 1 if "VELOCITY" in signals else 0
        record["is_geo_impossible_flag"] = 1 if "GEO_IMPOSSIBLE" in signals else 0
        record["is_category_flag"]       = 1 if "CATEGORY_DEVIATION" in signals else 0
        record["is_spend_outlier_flag"]  = 1 if "SPEND_OUTLIER" in signals else 0
        record["requires_review"]        = 1 if score >= 55 else 0

        scored.append(record)

        # Update history AFTER scoring (no lookahead bias)
        try:
            history[cid]["amounts"].append(float(record.get("amount_usd", 0)))
        except (ValueError, TypeError):
            pass
        ts = parse_dt(record.get("timestamp", ""))
        if ts:
            history[cid]["timestamps"].append(ts)
        history[cid]["categories"][record.get("merchant_category", "")] += 1
        history[cid]["countries"].append(record.get("country_code", ""))

    # Write output
    if scored:
        with open(output_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=scored[0].keys())
            writer.writeheader()
            writer.writerows(scored)

    flagged = sum(1 for r in scored if r["requires_review"])
    high_critical = risk_tier_counts["HIGH"] + risk_tier_counts["CRITICAL"]

    report = {
        "total_scored": len(scored),
        "flagged_for_review": flagged,
        "flag_rate_pct": round(flagged / len(scored) * 100, 2),
        "risk_tier_distribution": dict(risk_tier_counts),
        "signal_breakdown": dict(signal_counts),
        "high_critical_count": high_critical,
    }

    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)

    print(f"[fraud_engine] Scored {len(scored)} transactions")
    print(f"[fraud_engine] Flagged for review: {flagged} ({report['flag_rate_pct']}%)")
    print(f"[fraud_engine] Risk tiers: {dict(risk_tier_counts)}")
    print(f"[fraud_engine] Signal breakdown: {dict(signal_counts)}")
    return report


if __name__ == "__main__":
    run_fraud_engine()
