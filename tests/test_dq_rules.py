"""
Tests for data quality rules.

Each rule is tested against both a clean and a dirty fixture so we know
the check is actually discriminating, not just returning success.
"""
from __future__ import annotations

from src.dq.rules import (
    AmlCoverageRule,
    AmlStatusEnumRule,
    EidFormatRule,
    ErQuarantineRule,
    FraudScoreRangeRule,
    ProfileDobPlausibleRule,
)


def _ctx(**overrides):
    base = {"profile": [], "aecb": [], "fraud": [], "aml": [],
            "er_links": [], "er_quarantine": []}
    base.update(overrides)
    return base


# --- EidFormatRule -----------------------------------------------------------

def test_eid_format_passes_clean():
    ctx = _ctx(profile=[{"emirates_id": "784-1990-1234567-1"}])
    result = EidFormatRule().check(ctx)
    assert result.failed == 0 and not result.breached


def test_eid_format_catches_missing():
    ctx = _ctx(profile=[{"emirates_id": None}, {"emirates_id": "784-1990-1234567-1"}])
    result = EidFormatRule().check(ctx)
    assert result.failed == 1 and result.breached


def test_eid_format_catches_malformed():
    ctx = _ctx(profile=[{"emirates_id": "not-an-eid"}])
    result = EidFormatRule().check(ctx)
    assert result.failed == 1 and result.breached


# --- FraudScoreRangeRule -----------------------------------------------------

def test_fraud_score_out_of_range():
    ctx = _ctx(fraud=[{"score": 2000}, {"score": 500}, {"score": -10}])
    result = FraudScoreRangeRule().check(ctx)
    assert result.failed == 2 and result.breached


def test_fraud_score_not_numeric():
    ctx = _ctx(fraud=[{"score": "high"}])
    result = FraudScoreRangeRule().check(ctx)
    assert result.failed == 1


# --- AmlStatusEnumRule -------------------------------------------------------

def test_aml_status_valid_values():
    ctx = _ctx(aml=[{"status": "CLEAR"}, {"status": "REVIEW"}, {"status": "HIT"}])
    result = AmlStatusEnumRule().check(ctx)
    assert result.failed == 0


def test_aml_status_rejects_unknown():
    ctx = _ctx(aml=[{"status": "PENDING"}])
    result = AmlStatusEnumRule().check(ctx)
    assert result.failed == 1 and result.breached


# --- ProfileDobPlausibleRule -------------------------------------------------

def test_dob_plausible_accepts_adult():
    ctx = _ctx(profile=[{"dob": "1990-05-15"}])
    result = ProfileDobPlausibleRule().check(ctx)
    assert result.failed == 0


def test_dob_rejects_minor():
    ctx = _ctx(profile=[{"dob": "2020-01-01"}])
    result = ProfileDobPlausibleRule().check(ctx)
    assert result.failed == 1


def test_dob_rejects_impossibly_old():
    ctx = _ctx(profile=[{"dob": "1850-01-01"}])
    result = ProfileDobPlausibleRule().check(ctx)
    assert result.failed == 1


# --- AmlCoverageRule (warn) --------------------------------------------------

def test_aml_coverage_under_threshold_ok():
    ctx = _ctx(
        profile=[{"emirates_id": f"e{i}"} for i in range(100)],
        aml=[{"emirates_id": f"e{i}"} for i in range(97)],  # 3% missing, threshold 5%
    )
    result = AmlCoverageRule().check(ctx)
    assert not result.breached


def test_aml_coverage_over_threshold_warns():
    ctx = _ctx(
        profile=[{"emirates_id": f"e{i}"} for i in range(100)],
        aml=[{"emirates_id": f"e{i}"} for i in range(80)],  # 20% missing
    )
    result = AmlCoverageRule().check(ctx)
    assert result.breached


# --- ErQuarantineRule (warn) -------------------------------------------------

def test_er_quarantine_ok_when_small():
    ctx = _ctx(
        er_links=[{"a": 1} for _ in range(100)],
        er_quarantine=[{"a": 1}],  # 1% threshold
    )
    result = ErQuarantineRule().check(ctx)
    assert not result.breached


def test_er_quarantine_breaches_when_large():
    ctx = _ctx(
        er_links=[{"a": 1} for _ in range(80)],
        er_quarantine=[{"a": 1} for _ in range(20)],  # 20%
    )
    result = ErQuarantineRule().check(ctx)
    assert result.breached
