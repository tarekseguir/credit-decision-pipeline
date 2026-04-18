"""
Schemas used across layers.

Bronze envelopes wrap raw payloads with provenance metadata.
Silver models are the canonical, cleaned representations.
Gold assembles them into a single decision_input_vector.
"""
from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


# --- Bronze envelope ----------------------------------------------------------

class BronzeEnvelope(BaseModel):
    """Common envelope written for every bronze record, regardless of source."""
    model_config = ConfigDict(extra="forbid")

    source: str                          # aecb | fraud | aml | profile
    source_event_id: str                 # source-side unique id
    ingest_run_id: str                   # which pipeline run landed this
    received_at: datetime                # when we received it
    content_sha256: str                  # hash of raw bytes
    raw_path: str                        # pointer to raw file on disk
    payload_format: Literal["xml", "json", "sqlite"]


# --- Silver models ------------------------------------------------------------

class SilverProfile(BaseModel):
    """Internal customer profile — source of truth for customer identity.

    Includes two behavioural features derived from the users/applications/
    instalments tables in the profile database.
    """
    internal_uuid: str
    emirates_id: str                     # canonical key
    first_name: str
    last_name: str
    full_name: str
    dob: str                             # ISO date
    phone: str                           # E.164
    email: str
    emirate: str
    kyc_verified_at: datetime
    ingest_run_id: str

    # Behavioural features
    is_new_user: bool                    # no prior applications
    had_overdue_before: bool             # any past instalment OVERDUE or PAID_LATE


class SilverAECB(BaseModel):
    """Parsed AECB credit bureau report (CRIF NAE response).

    Fields are extracted from the matched subject and scoring sections.
    """
    report_id: str                       # CBContractId (unique per enquiry)
    cb_subject_id: Optional[str]         # AECB's internal subject ID
    report_date: str                     # Enquiry / message timestamp as ISO date
    emirates_id: Optional[str]           # From EnquiredSubject or MatchedSubject
    full_name: str
    dob: str                             # Normalised to ISO (YYYY-MM-DD)
    nationality: Optional[str] = None

    # Score
    credit_score: Optional[int]          # Score.Data.Index
    score_range: Optional[str] = None    # Score.Data.Range letter
    payment_order_flag: bool = False     # True if PaymentOrderFlag='Y' (bounced cheques)

    # Derived from the detailed sections
    num_bounced_cheques: int = 0
    total_bounced_cheques_amount: float = 0.0
    num_court_cases: int = 0
    num_open_court_cases: int = 0        # status != 90 (closed)
    total_claim_amount: float = 0.0

    # Employment
    gross_annual_income: Optional[float] = None

    ingest_run_id: str


class SilverFraud(BaseModel):
    """Fraud provider API response."""
    provider_ref: str
    emirates_id: str
    phone: str                           # may differ from profile
    email: str
    score: int                           # 0–1000
    decision: Literal["PASS", "REVIEW", "FAIL"]
    reason_codes: list[str]
    model_version: str
    scored_at: datetime
    ingest_run_id: str


class SilverAML(BaseModel):
    """AML / PEP screening callback."""
    callback_id: str
    emirates_id: str
    full_name: str                       # may have spelling variation
    dob: str
    status: Literal["CLEAR", "REVIEW", "HIT"]
    matched_lists: list[str]
    received_at: datetime
    ingest_run_id: str


# --- Entity resolution --------------------------------------------------------

class ERLink(BaseModel):
    """Records which source record resolves to which canonical customer."""
    source: str
    source_event_id: str
    emirates_id: str                     # the canonical key (possibly backfilled)
    match_tier: Literal[
        "eid_exact",
        "phone_email_exact",
        "name_dob_fuzzy",
        "quarantine",
    ]
    confidence: float
    resolved_at: datetime
    notes: Optional[str] = None


class ConflictLogEntry(BaseModel):
    """A disagreement between sources that the precedence rules resolved."""
    emirates_id: str
    field: str
    winning_source: str
    winning_value: str
    losing_source: str
    losing_value: str
    logged_at: datetime


# --- Gold: decision input vector ---------------------------------------------

class DecisionInputVector(BaseModel):
    """Flat, point-in-time feature vector handed to the decision engine."""
    emirates_id: str
    product: str
    as_of_ts: datetime

    # From profile
    full_name: str
    dob: str
    phone: str
    email: str
    emirate: str
    kyc_age_days: int
    is_new_user: bool
    had_overdue_before: bool

    # From AECB (CRIF NAE response)
    credit_score: Optional[int]
    score_range: Optional[str]
    payment_order_flag: bool = False
    num_bounced_cheques: int = 0
    total_bounced_cheques_amount: float = 0.0
    num_court_cases: int = 0
    num_open_court_cases: int = 0
    total_claim_amount: float = 0.0
    gross_annual_income: Optional[float] = None
    aecb_available: bool

    # From fraud
    fraud_score: Optional[int]
    fraud_decision: Optional[str]
    fraud_reason_codes: list[str] = Field(default_factory=list)
    fraud_available: bool

    # From AML
    aml_status: Optional[str]
    aml_matched_lists: list[str] = Field(default_factory=list)
    aml_available: bool
    aml_stale_seconds: Optional[int] = None      # how old was AML data at decision time


# --- Decision + snapshot ------------------------------------------------------

class DecisionOutcome(BaseModel):
    decision_id: str
    emirates_id: str
    product: str
    decision_ts: datetime
    outcome: Literal["APPROVE", "DECLINE", "REFER"]
    reason_codes: list[str]
    engine_version: str
    rule_pack_version: str
