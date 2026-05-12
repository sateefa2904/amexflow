"""
test_validate.py
Unit tests for the data quality validation framework.
Run with: python -m pytest tests/ -v
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../src"))

from validate import validate_record


def make_record(**overrides):
    base = {
        "transaction_id": "txn-001",
        "timestamp": "2024-06-15 10:30:00",
        "card_type": "Gold",
        "merchant_name": "Amazon",
        "merchant_category": "Retail",
        "amount_usd": "150.00",
        "currency": "USD",
        "status": "approved",
        "country_code": "US",
        "is_international": "False",
        "cardholder_id": "CH-12345",
    }
    base.update(overrides)
    return base


# ── Null checks ──────────────────────────────────────────
def test_valid_record_passes():
    record, errors, warnings = validate_record(make_record(), 150.0, 50.0, "test-run")
    assert errors == [], f"Expected no errors, got: {errors}"
    assert record["_is_valid"] is True


def test_missing_merchant_fails():
    record, errors, _ = validate_record(make_record(merchant_name=None), 150.0, 50.0, "test-run")
    assert any("NULL_FIELD:merchant_name" in e for e in errors)
    assert record["_is_valid"] is False


def test_missing_timestamp_fails():
    record, errors, _ = validate_record(make_record(timestamp=""), 150.0, 50.0, "test-run")
    assert any("NULL_FIELD:timestamp" in e for e in errors)


# ── Amount validation ─────────────────────────────────────
def test_negative_amount_fails():
    record, errors, _ = validate_record(make_record(amount_usd="-50.00"), 150.0, 50.0, "test-run")
    assert any("INVALID_AMOUNT" in e for e in errors)


def test_zero_amount_fails():
    record, errors, _ = validate_record(make_record(amount_usd="0"), 150.0, 50.0, "test-run")
    assert any("INVALID_AMOUNT" in e for e in errors)


def test_valid_amount_passes():
    record, errors, _ = validate_record(make_record(amount_usd="999.99"), 150.0, 50.0, "test-run")
    assert not any("AMOUNT" in e for e in errors)


# ── Status validation ─────────────────────────────────────
def test_invalid_status_fails():
    record, errors, _ = validate_record(make_record(status="refunded"), 150.0, 50.0, "test-run")
    assert any("INVALID_STATUS" in e for e in errors)


def test_valid_statuses_pass():
    for status in ["approved", "declined", "pending"]:
        record, errors, _ = validate_record(make_record(status=status), 150.0, 50.0, "test-run")
        assert not any("INVALID_STATUS" in e for e in errors), f"Failed for status: {status}"


# ── Anomaly detection ─────────────────────────────────────
def test_anomaly_flagged_as_warning():
    # Amount 10x std deviations above mean — should trigger anomaly warning
    record, errors, warnings = validate_record(
        make_record(amount_usd="5000.00"), 100.0, 10.0, "test-run"
    )
    assert any("AMOUNT_ANOMALY" in w for w in warnings)
    assert errors == []  # warning only, not an error


def test_normal_amount_no_anomaly():
    record, errors, warnings = validate_record(
        make_record(amount_usd="110.00"), 100.0, 20.0, "test-run"
    )
    assert not any("AMOUNT_ANOMALY" in w for w in warnings)


# ── Timestamp validation ──────────────────────────────────
def test_invalid_timestamp_fails():
    record, errors, _ = validate_record(
        make_record(timestamp="not-a-date"), 150.0, 50.0, "test-run"
    )
    assert any("INVALID_TIMESTAMP" in e for e in errors)


def test_valid_timestamp_passes():
    record, errors, _ = validate_record(
        make_record(timestamp="2024-01-15 09:45:00"), 150.0, 50.0, "test-run"
    )
    assert not any("TIMESTAMP" in e for e in errors)


# ── Lineage metadata ──────────────────────────────────────
def test_lineage_metadata_attached():
    record, _, _ = validate_record(make_record(), 150.0, 50.0, "run-xyz")
    assert record["_run_id"] == "run-xyz"
    assert "_validated_at" in record
    assert "_is_valid" in record
