"""Personal Finance rule pack — strictest thresholds."""
from __future__ import annotations

from src.decision.rule_packs import RulePack, _hard_declines, _refer_signals


class PersonalFinancePack(RulePack):
    version = "pf_v1.1.0"       # uses AECB real signals + behavioural features
    product = "personal_finance"

    MIN_CREDIT_SCORE = 650
    MIN_KYC_AGE_DAYS = 60
    MAX_FRAUD_SCORE = 400
    MIN_ANNUAL_INCOME = 60000      # AED

    def evaluate(self, v: dict) -> tuple[str, list[str]]:
        reasons: list[str] = []
        reasons.extend(_hard_declines(v))
        if reasons:
            return "DECLINE", reasons

        # New policy: past overdue on our own platform is a hard decline.
        if v.get("had_overdue_before"):
            return "DECLINE", ["INTERNAL_OVERDUE_HISTORY"]

        if v.get("credit_score") is None or v["credit_score"] < self.MIN_CREDIT_SCORE:
            reasons.append(f"CREDIT_SCORE_BELOW_{self.MIN_CREDIT_SCORE}")
            return "DECLINE", reasons

        # Even one bounced cheque is a decline signal for personal finance
        if v.get("num_bounced_cheques", 0) >= 1:
            return "DECLINE", ["BOUNCED_CHEQUE_HISTORY"]

        if (v.get("gross_annual_income") is not None
                and v["gross_annual_income"] < self.MIN_ANNUAL_INCOME):
            return "DECLINE", [f"INCOME_BELOW_{self.MIN_ANNUAL_INCOME}"]

        if v.get("kyc_age_days", 0) < self.MIN_KYC_AGE_DAYS:
            return "REFER", ["KYC_TOO_NEW"]

        if v.get("is_new_user"):
            return "REFER", ["NEW_USER_ON_PLATFORM"]

        if v.get("fraud_score") is not None and v["fraud_score"] > self.MAX_FRAUD_SCORE:
            return "REFER", [f"FRAUD_SCORE_ABOVE_{self.MAX_FRAUD_SCORE}"]

        refer = _refer_signals(v)
        if refer:
            return "REFER", refer

        return "APPROVE", ["ALL_CHECKS_PASSED"]
