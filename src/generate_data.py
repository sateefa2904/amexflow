"""
generate_data.py
Generates synthetic financial transaction data with realistic fraud patterns.

Fraud scenarios injected (~12% of records):
  - Velocity fraud: multiple transactions in short window
  - Geographic impossibility: impossible travel between transactions
  - Category mismatch: cardholder suddenly uses unusual merchant category
  - High-value outliers: sudden large transactions from low-spend cardholders
"""

import csv
import random
import uuid
import math
from datetime import datetime, timedelta
import os

random.seed(42)

MERCHANTS = [
    "Amazon", "Walmart", "Target", "Starbucks", "Delta Airlines",
    "Marriott Hotels", "Apple Store", "Best Buy", "Whole Foods", "Uber",
    "Shell Gas", "CVS Pharmacy", "McDonald's", "Netflix", "Spotify"
]
MERCHANT_CATEGORY = {
    "Amazon": "Retail", "Walmart": "Retail", "Target": "Retail",
    "Starbucks": "Food & Beverage", "Delta Airlines": "Travel",
    "Marriott Hotels": "Travel", "Apple Store": "Technology",
    "Best Buy": "Technology", "Whole Foods": "Food & Beverage",
    "Uber": "Transportation", "Shell Gas": "Fuel",
    "CVS Pharmacy": "Healthcare", "McDonald's": "Food & Beverage",
    "Netflix": "Entertainment", "Spotify": "Entertainment"
}

random.seed(42)
CARDHOLDER_PROFILES = {
    f"CH-{i}": {
        "home_country": random.choice(["US", "US", "US", "CA", "GB"]),
        "typical_category": random.choice(list(set(MERCHANT_CATEGORY.values()))),
        "avg_spend": random.uniform(20, 500),
    }
    for i in range(10000, 10200)
}

CARD_TYPES = ["Gold", "Platinum", "Green", "Blue Cash"]
STATUSES = ["approved", "declined", "pending"]

COUNTRY_COORDS = {
    "US": (37.09, -95.71), "CA": (56.13, -106.34),
    "GB": (55.37, -3.43), "DE": (51.16, 10.45),
    "FR": (46.22, 2.21), "AU": (-25.27, 133.77),
    "RU": (61.52, 105.31), "CN": (35.86, 104.19),
}

def haversine_km(c1, c2):
    lat1, lon1 = map(math.radians, c1)
    lat2, lon2 = map(math.radians, c2)
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(dlon/2)**2
    return 6371 * 2 * math.asin(math.sqrt(a))


def generate_transactions(n=2000):
    records = []
    cardholder_ids = list(CARDHOLDER_PROFILES.keys())
    last_txn = {}

    random.seed(42)
    for i in range(n):
        cardholder_id = random.choice(cardholder_ids)
        profile = CARDHOLDER_PROFILES[cardholder_id]
        merchant = random.choice(MERCHANTS)
        category = MERCHANT_CATEGORY[merchant]
        card_type = random.choice(CARD_TYPES)
        status = random.choices(STATUSES, weights=[0.85, 0.10, 0.05])[0]
        base_dt = datetime.now() - timedelta(days=random.randint(0, 365))
        fraud_flags = []
        country = profile["home_country"]
        amount = round(random.uniform(5, profile["avg_spend"] * 2), 2)
        fraud_roll = random.random()

        if fraud_roll < 0.04 and cardholder_id in last_txn:
            base_dt = last_txn[cardholder_id]["timestamp"] + timedelta(seconds=random.randint(30, 180))
            fraud_flags.append("VELOCITY")

        elif fraud_roll < 0.07 and cardholder_id in last_txn:
            last_country = last_txn[cardholder_id]["country"]
            impossible = [c for c in COUNTRY_COORDS if c != last_country and
                         haversine_km(COUNTRY_COORDS[last_country], COUNTRY_COORDS[c]) > 3000]
            if impossible:
                country = random.choice(impossible)
                base_dt = last_txn[cardholder_id]["timestamp"] + timedelta(minutes=random.randint(45, 119))
                fraud_flags.append("GEO_IMPOSSIBLE")

        elif fraud_roll < 0.10:
            atypical = [m for m, c in MERCHANT_CATEGORY.items() if c != profile["typical_category"]]
            if atypical:
                merchant = random.choice(atypical)
                category = MERCHANT_CATEGORY[merchant]
                fraud_flags.append("CATEGORY_MISMATCH")

        elif fraud_roll < 0.12:
            amount = round(profile["avg_spend"] * random.uniform(10, 20), 2)
            fraud_flags.append("HIGH_VALUE_OUTLIER")

        dirty = random.random() < 0.05
        txn = {
            "transaction_id": str(uuid.uuid4()),
            "timestamp": base_dt.strftime("%Y-%m-%d %H:%M:%S") if not (dirty and random.random() < 0.3) else "",
            "card_type": card_type,
            "merchant_name": merchant if not (dirty and random.random() < 0.3) else None,
            "merchant_category": category,
            "amount_usd": amount if not (dirty and random.random() < 0.3) else -amount,
            "currency": "USD",
            "status": status,
            "country_code": country,
            "is_international": str(country != profile["home_country"]),
            "cardholder_id": cardholder_id,
            "cardholder_home_country": profile["home_country"],
            "cardholder_typical_category": profile["typical_category"],
            "cardholder_avg_spend": round(profile["avg_spend"], 2),
            "_injected_fraud_flags": "|".join(fraud_flags) if fraud_flags else "",
        }
        records.append(txn)
        last_txn[cardholder_id] = {"timestamp": base_dt, "country": country, "amount": amount}

    return records


def save_raw(records, path="data/raw_transactions.csv"):
    os.makedirs("data", exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)
    print(f"[generate] Saved {len(records)} records → {path}")
    injected = sum(1 for r in records if r["_injected_fraud_flags"])
    print(f"[generate] Fraud patterns injected in {injected} records ({injected/len(records)*100:.1f}%)")


if __name__ == "__main__":
    records = generate_transactions(2000)
    save_raw(records)
