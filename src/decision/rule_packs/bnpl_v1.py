"""BNPL rule pack — short duration, lower value, more permissive."""
from __future__ import annotations

from src.decision.rule_packs import RulePack, _hard_declines, _refer_signals


class BnplPack(RulePack):
    version = "bnpl_v1.1.0"
    product = "bnpl"

    MIN_CREDIT_SCORE = 550
    MAX_FRAUD_SCORE = 600

    def evaluate(self, v: dict) -> tuple[str, list[str]]:
        reasons: list[str] = []
        reasons.extend(_hard_declines(v))
        if reasons:
            return "DECLINE", reasons

        # BNPL can proceed even without AECB for thin-file customers
        # (small ticket size → acceptable risk), but still needs AML.
        if not v.get("aml_available"):
            return "REFER", ["AML_PENDING"]

        if v.get("aml_status") == "REVIEW":
            return "REFER", ["AML_REVIEW"]

        if v.get("fraud_score") is not None and v["fraud_score"] > self.MAX_FRAUD_SCORE:
            return "REFER", [f"FRAUD_SCORE_ABOVE_{self.MAX_FRAUD_SCORE}"]

        # If AECB exists, enforce thresholds
        if v.get("aecb_available"):
            if v["credit_score"] is not None and v["credit_score"] < self.MIN_CREDIT_SCORE:
                reasons.append(f"CREDIT_SCORE_BELOW_{self.MIN_CREDIT_SCORE}")
                return "DECLINE", reasons
            if v.get("num_bounced_cheques", 0) >= 2:
                return "DECLINE", ["MULTIPLE_BOUNCED_CHEQUES"]

        if v.get("fraud_decision") == "REVIEW":
            return "REFER", ["FRAUD_REVIEW"]

        return "APPROVE", ["ALL_CHECKS_PASSED"]
