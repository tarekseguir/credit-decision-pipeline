"""
Tests for PII minimisation in snapshots.

Two invariants pinned by these tests:
  1. Fields listed in PII_FIELDS_TO_TOKENISE are never written as raw values
     to the feature vector on disk.
  2. Rule packs don't read any of those fields — so tokenisation doesn't
     affect replay correctness. If a future rule pack tries to read raw PII,
     this test forces us to reconsider whether that's appropriate.
"""
from __future__ import annotations

import ast
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

import src.audit.snapshot_writer as sw
import src.audit.replay as replay_mod
from src.audit import write_snapshot
from src.audit.snapshot_writer import PII_FIELDS_TO_TOKENISE
from src.models import DecisionOutcome


def _vector():
    return {
        "emirates_id": "784-1990-0000001-0", "product": "personal_finance",
        "as_of_ts": datetime.now(timezone.utc).isoformat(),
        "full_name": "Ahmed Al Mansoori", "dob": "1990-01-15",
        "phone": "+971501234567", "email": "ahmed@example.ae",
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


def _outcome():
    return DecisionOutcome(
        decision_id="dcn_pii_test",
        emirates_id="784-1990-0000001-0",
        product="personal_finance",
        decision_ts=datetime.now(timezone.utc),
        outcome="APPROVE", reason_codes=["ALL_CHECKS_PASSED"],
        engine_version="v1.0.0", rule_pack_version="pf_v1.1.0",
    )


@pytest.fixture
def isolated_audit(tmp_path, monkeypatch):
    audit_dir = tmp_path / "audit"
    audit_db = tmp_path / "audit.db"
    monkeypatch.setattr(sw, "AUDIT_DIR", audit_dir)
    monkeypatch.setattr(sw, "AUDIT_DB", audit_db)
    monkeypatch.setattr(replay_mod, "AUDIT_DIR", audit_dir)
    monkeypatch.setattr(replay_mod, "AUDIT_DB", audit_db)
    yield audit_dir


def test_stored_feature_vector_has_no_raw_pii(isolated_audit):
    """The feature vector file on disk must not contain raw values for any
    field listed in PII_FIELDS_TO_TOKENISE."""
    vector = _vector()
    snap = write_snapshot(_outcome(), vector)

    fv_path = Path(sw.AUDIT_DIR.parent) / snap["feature_vector_path"]
    stored = json.loads(fv_path.read_text())

    for field in PII_FIELDS_TO_TOKENISE:
        raw_value = vector[field]
        stored_value = stored[field]
        assert stored_value != raw_value, (
            f"Field {field!r} was stored as raw PII: {raw_value!r}"
        )
        assert stored_value.startswith("tok_"), (
            f"Field {field!r} must be tokenised (tok_* prefix); got {stored_value!r}"
        )


def test_tokenisation_is_deterministic(isolated_audit):
    """Same raw value produces the same token — lets an auditor confirm
    two snapshots reference the same identity without seeing the raw PII."""
    v1 = _vector()
    v2 = _vector()
    v2["credit_score"] = 650   # different risk input, same PII

    from src.audit.snapshot_writer import _tokenise_pii
    t1 = _tokenise_pii(v1)
    t2 = _tokenise_pii(v2)

    for field in PII_FIELDS_TO_TOKENISE:
        assert t1[field] == t2[field], (
            f"Identical {field!r} produced different tokens"
        )


def test_emirates_id_is_preserved(isolated_audit):
    """Regulator requires the Emirates ID on every decision record — it must
    NOT be tokenised."""
    vector = _vector()
    snap = write_snapshot(_outcome(), vector)

    fv_path = Path(sw.AUDIT_DIR.parent) / snap["feature_vector_path"]
    stored = json.loads(fv_path.read_text())

    assert stored["emirates_id"] == vector["emirates_id"]
    assert not stored["emirates_id"].startswith("tok_")


def test_rule_packs_dont_read_pii_fields():
    """Static-analysis invariant: no rule pack module references any
    PII_FIELDS_TO_TOKENISE field. If a new rule pack tries to read raw name
    or phone, this test forces a review — either tokenise differently or
    change the field list.

    Uses AST parsing to inspect the source of every rule_pack module.
    """
    from src.decision import rule_packs
    pack_dir = Path(rule_packs.__file__).parent

    offenders: list[str] = []
    for py in pack_dir.glob("*.py"):
        tree = ast.parse(py.read_text())
        for node in ast.walk(tree):
            # Look for v["field"] and v.get("field") style accesses
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                if node.value in PII_FIELDS_TO_TOKENISE:
                    offenders.append(f"{py.name}: {node.value!r}")

    assert not offenders, (
        "Rule pack modules reference PII fields — if this is intentional, "
        "update PII_FIELDS_TO_TOKENISE. Offenders: " + ", ".join(offenders)
    )
