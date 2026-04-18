"""
Tests for the audit snapshot chain.

Covers: hash stability, chain continuity, tamper detection, and replay
reproducibility. These are the properties an auditor will care about.
"""
from __future__ import annotations

import json
import sqlite3
import stat
from datetime import datetime, timezone
from pathlib import Path

import pytest

import src.audit.snapshot_writer as sw
import src.audit.replay as replay_mod
from src.audit import verify_chain, write_snapshot
from src.models import DecisionOutcome


def _vector(eid="784-1990-0000001-0"):
    return {
        "emirates_id": eid, "product": "personal_finance",
        "as_of_ts": datetime.now(timezone.utc).isoformat(),
        "full_name": "Test User", "dob": "1990-01-01",
        "phone": "+971500000000", "email": "test@example.ae",
        "emirate": "Dubai", "kyc_age_days": 100,
        "is_new_user": False, "had_overdue_before": False,
        "credit_score": 700, "score_range": "D",
        "payment_order_flag": False,
        "num_bounced_cheques": 0, "total_bounced_cheques_amount": 0.0,
        "num_court_cases": 0, "num_open_court_cases": 0,
        "total_claim_amount": 0.0, "gross_annual_income": 120000.0,
        "aecb_available": True,
        "fraud_score": 100, "fraud_decision": "PASS",
        "fraud_reason_codes": [], "fraud_available": True,
        "aml_status": "CLEAR", "aml_matched_lists": [],
        "aml_available": True, "aml_stale_seconds": 60,
    }


def _outcome(decision_id="dcn_test1", eid="784-1990-0000001-0"):
    return DecisionOutcome(
        decision_id=decision_id, emirates_id=eid,
        product="personal_finance",
        decision_ts=datetime.now(timezone.utc),
        outcome="APPROVE", reason_codes=["ALL_CHECKS_PASSED"],
        engine_version="v1.0.0", rule_pack_version="pf_v1.0.0",
    )


@pytest.fixture
def isolated_audit(tmp_path, monkeypatch):
    """Redirect audit DB and dir into a temp path so tests don't pollute real data."""
    audit_dir = tmp_path / "audit"
    audit_db = tmp_path / "audit.db"

    monkeypatch.setattr(sw, "AUDIT_DIR", audit_dir)
    monkeypatch.setattr(sw, "AUDIT_DB", audit_db)
    monkeypatch.setattr(replay_mod, "AUDIT_DIR", audit_dir)
    monkeypatch.setattr(replay_mod, "AUDIT_DB", audit_db)

    yield audit_dir


def test_single_snapshot_passes_chain_verification(isolated_audit):
    write_snapshot(_outcome(), _vector())
    issues = verify_chain("personal_finance")
    assert issues == []


def test_chain_continuity_across_multiple_snapshots(isolated_audit):
    for i in range(5):
        write_snapshot(_outcome(decision_id=f"dcn_t{i}"), _vector())
    assert verify_chain("personal_finance") == []


def test_chain_detects_tampered_outcome(isolated_audit):
    write_snapshot(_outcome(decision_id="dcn_a"), _vector())
    snap_body = write_snapshot(_outcome(decision_id="dcn_b"), _vector())

    # Tamper: flip the outcome on the second snapshot
    snap_path = Path(sw.AUDIT_DIR.parent) / next(
        sw.AUDIT_DIR.rglob("dcn_b.json")
    ).relative_to(sw.AUDIT_DIR.parent)
    snap_path.chmod(stat.S_IRUSR | stat.S_IWUSR)
    body = json.loads(snap_path.read_text())
    body["outcome"] = "DECLINE"
    snap_path.write_text(json.dumps(body, sort_keys=True, indent=2))

    issues = verify_chain("personal_finance")
    assert any(i.kind == "hash_mismatch" for i in issues)


def test_chain_detects_vector_tamper(isolated_audit):
    snap = write_snapshot(_outcome(decision_id="dcn_v"), _vector())

    fv_path = Path(sw.AUDIT_DIR.parent) / snap["feature_vector_path"]
    fv_path.chmod(stat.S_IRUSR | stat.S_IWUSR)
    body = json.loads(fv_path.read_text())
    body["credit_score"] = 850
    fv_path.write_text(json.dumps(body, sort_keys=True, indent=2))

    issues = verify_chain("personal_finance")
    assert any(i.kind == "vector_mismatch" for i in issues)


def test_replay_is_deterministic(isolated_audit):
    write_snapshot(_outcome(decision_id="dcn_r"), _vector())
    result = replay_mod.replay_decision("dcn_r")
    assert result.matches


def test_prev_hash_pointer_updates(isolated_audit):
    s1 = write_snapshot(_outcome(decision_id="dcn_1"), _vector())
    s2 = write_snapshot(_outcome(decision_id="dcn_2"), _vector())
    assert s2["prev_snapshot_sha256"] == s1["this_snapshot_sha256"]


def test_genesis_hash_is_zero(isolated_audit):
    s1 = write_snapshot(_outcome(decision_id="dcn_first"), _vector())
    assert s1["prev_snapshot_sha256"] == "0" * 64
