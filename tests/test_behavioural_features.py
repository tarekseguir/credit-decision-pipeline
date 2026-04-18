"""
Tests for the behavioural features (is_new_user, had_overdue_before).

Covers:
  - The SQL query that derives them from users/applications/instalments
  - Their effect on the PF rule pack decisions
"""
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

from src.decision.rule_packs.personal_finance_v1 import PersonalFinancePack
from src.silver.parse_profile import USER_FEATURES_SQL


def _base_vector(**overrides) -> dict:
    v = {
        "emirates_id": "784-1990-0000001-0",
        "product": "personal_finance",
        "full_name": "Test User", "dob": "1990-01-01",
        "phone": "+971500000000", "email": "t@e.ae",
        "emirate": "Dubai", "kyc_age_days": 365,
        "is_new_user": False, "had_overdue_before": False,
        "credit_score": 720, "score_range": "D",
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
    v.update(overrides)
    return v


# --- Rule-pack integration ---------------------------------------------------

def test_pf_declines_customer_with_prior_overdue():
    pack = PersonalFinancePack()
    outcome, reasons = pack.evaluate(_base_vector(had_overdue_before=True))
    assert outcome == "DECLINE"
    assert "INTERNAL_OVERDUE_HISTORY" in reasons


def test_pf_refers_new_user_for_personal_finance():
    pack = PersonalFinancePack()
    outcome, reasons = pack.evaluate(_base_vector(is_new_user=True))
    assert outcome == "REFER"
    assert "NEW_USER_ON_PLATFORM" in reasons


def test_pf_approves_clean_existing_customer():
    pack = PersonalFinancePack()
    outcome, _ = pack.evaluate(_base_vector())
    assert outcome == "APPROVE"


# --- SQL feature derivation --------------------------------------------------

PROFILE_SCHEMA = """
CREATE TABLE users (
    internal_uuid TEXT PRIMARY KEY, emirates_id TEXT, first_name TEXT, last_name TEXT,
    full_name TEXT, dob TEXT, phone TEXT, email TEXT, emirate TEXT, kyc_verified_at TEXT
);
CREATE TABLE applications (
    application_id TEXT PRIMARY KEY, internal_uuid TEXT, product TEXT,
    amount REAL, decision TEXT, applied_at TEXT
);
CREATE TABLE instalments (
    instalment_id TEXT PRIMARY KEY, application_id TEXT, due_date TEXT,
    amount REAL, paid_at TEXT, status TEXT, overdue_days INTEGER
);
"""


def _build_profile_db(users, apps, instalments) -> Path:
    p = Path(tempfile.mkstemp(suffix=".db")[1])
    with sqlite3.connect(p) as conn:
        conn.executescript(PROFILE_SCHEMA)
        conn.executemany(
            "INSERT INTO users VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", users
        )
        conn.executemany(
            "INSERT INTO applications VALUES (?, ?, ?, ?, ?, ?)", apps
        )
        conn.executemany(
            "INSERT INTO instalments VALUES (?, ?, ?, ?, ?, ?, ?)", instalments
        )
    return p


def _run_features(db_path: Path) -> dict:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        return {
            row["emirates_id"]: dict(row)
            for row in conn.execute(USER_FEATURES_SQL)
        }


def test_feature_is_new_user_true_when_no_applications():
    u = ("u1", "784-1990-0000001-0", "A", "B", "A B", "1990-01-01",
         "+9715", "a@b", "Dubai", "2025-01-01")
    db = _build_profile_db([u], [], [])
    result = _run_features(db)
    assert result["784-1990-0000001-0"]["is_new_user"] == 1
    assert result["784-1990-0000001-0"]["had_overdue_before"] == 0


def test_feature_is_new_user_false_when_has_applications():
    u = ("u1", "784-1990-0000001-0", "A", "B", "A B", "1990-01-01",
         "+9715", "a@b", "Dubai", "2025-01-01")
    a = ("app1", "u1", "bnpl", 1000.0, "APPROVE", "2024-01-01")
    db = _build_profile_db([u], [a], [])
    result = _run_features(db)
    assert result["784-1990-0000001-0"]["is_new_user"] == 0


def test_feature_had_overdue_true_when_any_overdue_instalment():
    u = ("u1", "784-1990-0000001-0", "A", "B", "A B", "1990-01-01",
         "+9715", "a@b", "Dubai", "2025-01-01")
    a = ("app1", "u1", "bnpl", 1000.0, "APPROVE", "2024-01-01")
    ok = ("i1", "app1", "2024-02-01", 500.0, "2024-02-01", "PAID_ON_TIME", 0)
    bad = ("i2", "app1", "2024-03-01", 500.0, None, "OVERDUE", 45)
    db = _build_profile_db([u], [a], [ok, bad])
    result = _run_features(db)
    assert result["784-1990-0000001-0"]["had_overdue_before"] == 1


def test_feature_had_overdue_true_when_paid_late():
    u = ("u1", "784-1990-0000001-0", "A", "B", "A B", "1990-01-01",
         "+9715", "a@b", "Dubai", "2025-01-01")
    a = ("app1", "u1", "bnpl", 1000.0, "APPROVE", "2024-01-01")
    late = ("i1", "app1", "2024-02-01", 500.0, "2024-02-20", "PAID_LATE", 19)
    db = _build_profile_db([u], [a], [late])
    result = _run_features(db)
    assert result["784-1990-0000001-0"]["had_overdue_before"] == 1


def test_feature_had_overdue_false_when_all_on_time():
    u = ("u1", "784-1990-0000001-0", "A", "B", "A B", "1990-01-01",
         "+9715", "a@b", "Dubai", "2025-01-01")
    a = ("app1", "u1", "bnpl", 1000.0, "APPROVE", "2024-01-01")
    ok1 = ("i1", "app1", "2024-02-01", 500.0, "2024-02-01", "PAID_ON_TIME", 0)
    ok2 = ("i2", "app1", "2024-03-01", 500.0, "2024-03-01", "PAID_ON_TIME", 0)
    db = _build_profile_db([u], [a], [ok1, ok2])
    result = _run_features(db)
    assert result["784-1990-0000001-0"]["had_overdue_before"] == 0
