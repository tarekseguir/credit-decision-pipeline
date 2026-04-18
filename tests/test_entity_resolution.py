"""
Tests for the 4-tier entity resolver.

Each tier has at least one happy-path test plus one rejection test.
"""
from __future__ import annotations

from src.config import Source
from src.silver.entity_resolution import resolve_record


# --- Tier 1: Emirates ID exact match -----------------------------------------

def test_tier1_eid_exact_match(profile_index):
    link = resolve_record(
        source=Source.AECB, source_event_id="rpt_1",
        emirates_id="784-1990-1234567-1",
        phone=None, email=None, full_name=None, dob=None,
        index=profile_index,
    )
    assert link.match_tier == "eid_exact"
    assert link.confidence == 1.00
    assert link.emirates_id == "784-1990-1234567-1"


def test_tier1_new_customer_from_profile(profile_index):
    """Profile and AECB may introduce new Emirates IDs."""
    link = resolve_record(
        source=Source.PROFILE, source_event_id="p_new",
        emirates_id="784-2000-0000001-0",
        phone=None, email=None, full_name=None, dob=None,
        index=profile_index,
    )
    assert link.match_tier == "eid_exact"
    assert "new customer" in link.notes


def test_tier1_rejects_invalid_eid_format(profile_index):
    """Malformed EIDs must not match tier 1."""
    link = resolve_record(
        source=Source.FRAUD, source_event_id="frd_x",
        emirates_id="not-a-valid-eid",
        phone=None, email=None, full_name=None, dob=None,
        index=profile_index,
    )
    assert link.match_tier == "quarantine"


# --- Tier 2: phone + email ---------------------------------------------------

def test_tier2_phone_email_match(profile_index):
    link = resolve_record(
        source=Source.FRAUD, source_event_id="frd_1",
        emirates_id=None,
        phone="+971501234567", email="ahmed@example.ae",
        full_name=None, dob=None,
        index=profile_index,
    )
    assert link.match_tier == "phone_email_exact"
    assert link.confidence == 0.95
    assert link.emirates_id == "784-1990-1234567-1"
    assert "backfilled" in link.notes


def test_tier2_requires_both_phone_and_email(profile_index):
    """Phone alone or email alone must not match tier 2."""
    link = resolve_record(
        source=Source.FRAUD, source_event_id="frd_phone_only",
        emirates_id=None,
        phone="+971501234567", email=None,
        full_name=None, dob=None,
        index=profile_index,
    )
    assert link.match_tier == "quarantine"


# --- Tier 3: name + DOB fuzzy ------------------------------------------------

def test_tier3_name_dob_fuzzy_exact(profile_index):
    link = resolve_record(
        source=Source.AML, source_event_id="aml_1",
        emirates_id=None, phone=None, email=None,
        full_name="Ahmed Al Mansoori", dob="1990-01-15",
        index=profile_index,
    )
    assert link.match_tier == "name_dob_fuzzy"
    assert link.confidence >= 0.92
    assert link.emirates_id == "784-1990-1234567-1"


def test_tier3_name_dob_fuzzy_one_letter_swap(profile_index):
    """A single-letter typo still hits tier 3."""
    link = resolve_record(
        source=Source.AML, source_event_id="aml_typo",
        emirates_id=None, phone=None, email=None,
        full_name="Ahmad Al Mansoori", dob="1990-01-15",  # Ahmed → Ahmad
        index=profile_index,
    )
    assert link.match_tier == "name_dob_fuzzy"


def test_tier3_different_dob_rejects(profile_index):
    """Name can match but DOB mismatch must reject."""
    link = resolve_record(
        source=Source.AML, source_event_id="aml_wrong_dob",
        emirates_id=None, phone=None, email=None,
        full_name="Ahmed Al Mansoori", dob="1980-01-01",
        index=profile_index,
    )
    assert link.match_tier == "quarantine"


def test_tier3_wildly_different_name_rejects(profile_index):
    """Same DOB but completely different name should not match."""
    link = resolve_record(
        source=Source.AML, source_event_id="aml_wrong_name",
        emirates_id=None, phone=None, email=None,
        full_name="Completely Different Person", dob="1990-01-15",
        index=profile_index,
    )
    assert link.match_tier == "quarantine"


# --- Fraud/AML quarantine invariant ------------------------------------------

def test_fraud_cannot_mint_new_customer(profile_index):
    """Fraud source with unknown EID must NOT create a new canonical ID."""
    link = resolve_record(
        source=Source.FRAUD, source_event_id="frd_stranger",
        emirates_id="784-2099-9999999-9",  # valid format, not in index
        phone=None, email=None, full_name=None, dob=None,
        index=profile_index,
    )
    assert link.match_tier == "quarantine"


def test_aml_cannot_mint_new_customer(profile_index):
    link = resolve_record(
        source=Source.AML, source_event_id="aml_stranger",
        emirates_id="784-2099-8888888-8",
        phone=None, email=None, full_name=None, dob=None,
        index=profile_index,
    )
    assert link.match_tier == "quarantine"
