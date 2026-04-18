"""Credit Card Alternative rule pack — revolving credit, mid-tier thresholds."""
from __future__ import annotations

from src.decision.rule_packs import RulePack, _hard_declines, _refer_signals


class CreditCardAltPack(RulePack):
    version = "cca_v1.1.0"
    product = "credit_card_alt"

    MIN_CREDIT_SCORE = 600
    MIN_KYC_AGE_DAYS = 30
    MAX_FRAUD_SCORE = 500

    def evaluate(self, v: dict) -> tuple[str, list[str]]:
        reasons: list[str] = []
        reasons.extend(_hard_declines(v))
        if reasons:
            return "DECLINE", reasons

        if not v.get("aecb_available"):
            return "REFER", ["AECB_MISSING"]

        if v["credit_score"] is not None and v["credit_score"] < self.MIN_CREDIT_SCORE:
            reasons.append(f"CREDIT_SCORE_BELOW_{self.MIN_CREDIT_SCORE}")
            return "DECLINE", reasons

        if v.get("num_bounced_cheques", 0) >= 2:
            return "DECLINE", ["MULTIPLE_BOUNCED_CHEQUES"]

        if v.get("kyc_age_days", 0) < self.MIN_KYC_AGE_DAYS:
            return "REFER", ["KYC_TOO_NEW"]

        if v.get("fraud_score") is not None and v["fraud_score"] > self.MAX_FRAUD_SCORE:
            return "REFER", [f"FRAUD_SCORE_ABOVE_{self.MAX_FRAUD_SCORE}"]

        refer = _refer_signals(v)
        if refer:
            return "REFER", refer

        return "APPROVE", ["ALL_CHECKS_PASSED"]
