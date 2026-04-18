"""
Decision rule packs.

Each pack is a small class with:
  - version (string, bumped on any change; shows up in the snapshot)
  - evaluate(vector) -> (outcome, reason_codes)

Rule packs are pure functions of the decision_input_vector. Any change to
logic requires a version bump so audit trails remain unambiguous.

The packs are intentionally simple for the demo; production would use
a dedicated rule engine (e.g. Durable Rules, DMN).
"""
from __future__ import annotations

from abc import ABC, abstractmethod


class RulePack(ABC):
    version: str = ""
    product: str = ""

    @abstractmethod
    def evaluate(self, v: dict) -> tuple[str, list[str]]:
        """Return (outcome, reason_codes). Outcome in {APPROVE, DECLINE, REFER}."""
        raise NotImplementedError


# --- Shared guards -----------------------------------------------------------

def _hard_declines(v: dict) -> list[str]:
    """Reasons that force DECLINE regardless of product."""
    reasons = []
    if v.get("aml_status") == "HIT":
        reasons.append("AML_HIT")
    if v.get("fraud_decision") == "FAIL":
        reasons.append("FRAUD_FAIL")
    # Real AECB signals: bounced cheques or open court cases = severe red flag
    if v.get("payment_order_flag") and v.get("num_bounced_cheques", 0) >= 3:
        reasons.append("MULTIPLE_BOUNCED_CHEQUES")
    if v.get("num_open_court_cases", 0) > 0:
        reasons.append("OPEN_COURT_CASE")
    return reasons


def _refer_signals(v: dict) -> list[str]:
    """Reasons that force REFER to manual review."""
    reasons = []
    if not v.get("aecb_available"):
        reasons.append("AECB_MISSING")
    if not v.get("aml_available"):
        reasons.append("AML_PENDING")
    if v.get("aml_status") == "REVIEW":
        reasons.append("AML_REVIEW")
    if v.get("fraud_decision") == "REVIEW":
        reasons.append("FRAUD_REVIEW")
    return reasons
