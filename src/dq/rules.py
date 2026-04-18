"""
Data quality rules — declarative framework.

Each rule is a subclass of Rule with a name, severity, and a check() method.
The runner executes every registered rule, writes results to gold.dq_result,
and the gate task blocks silver→gold promotion if any MUST_PASS rule fails.

Severity semantics:
  MUST_PASS  — zero fails tolerated; breach blocks gold promotion + PagerDuty.
  WARN       — per-rule threshold (see config.WARN_THRESHOLDS); breach → Slack.

Adding a new rule is a one-liner: subclass Rule, register it in RULES.
"""
from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from src.config import WARN_THRESHOLDS


MUST_PASS = "MUST_PASS"
WARN = "WARN"


# --- Result object ------------------------------------------------------------

@dataclass
class DQResult:
    rule_id: str
    severity: str
    source: str
    description: str
    checked: int
    failed: int
    threshold: float                   # max tolerated fail_ratio
    breached: bool
    sample_failures: list[dict] = field(default_factory=list)
    evaluated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def fail_ratio(self) -> float:
        return 0.0 if self.checked == 0 else self.failed / self.checked

    @property
    def status_text(self) -> str:
        if not self.breached:
            return "PASS"
        return "FAIL" if self.severity == MUST_PASS else "WARN"

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "severity": self.severity,
            "source": self.source,
            "description": self.description,
            "checked": self.checked,
            "failed": self.failed,
            "fail_ratio": round(self.fail_ratio, 4),
            "threshold": self.threshold,
            "breached": self.breached,
            "status": self.status_text,
            "sample_failures": self.sample_failures[:3],
            "evaluated_at": self.evaluated_at.isoformat(),
        }


# --- Base rule ----------------------------------------------------------------

class Rule(ABC):
    """Base class for all DQ rules."""

    rule_id: str = ""
    severity: str = MUST_PASS
    source: str = ""
    description: str = ""

    @property
    def threshold(self) -> float:
        """Default threshold: zero fails for MUST_PASS, per-rule for WARN."""
        if self.severity == MUST_PASS:
            return 0.0
        return WARN_THRESHOLDS.get(self.rule_id, 0.0)

    @abstractmethod
    def check(self, context: dict) -> DQResult:
        """Run the check and return a DQResult.

        `context` contains loaded silver tables keyed by source/table.
        """
        raise NotImplementedError

    def _result(self, records: list, failures: list) -> DQResult:
        checked = len(records)
        failed = len(failures)
        ratio = (failed / checked) if checked else 0.0
        return DQResult(
            rule_id=self.rule_id,
            severity=self.severity,
            source=self.source,
            description=self.description,
            checked=checked,
            failed=failed,
            threshold=self.threshold,
            breached=ratio > self.threshold,
            sample_failures=failures[:3],
        )


# --- Concrete rules -----------------------------------------------------------

EID_PATTERN = re.compile(r"^784-\d{4}-\d{7}-\d$")


class EidFormatRule(Rule):
    rule_id = "eid_format"
    severity = MUST_PASS
    source = "profile"
    description = "Emirates ID must match 784-YYYY-NNNNNNN-C after normalisation."

    def check(self, context):
        profiles = context["profile"]
        failures = [p for p in profiles if not EID_PATTERN.match(p.get("emirates_id") or "")]
        return self._result(profiles, failures)


class FraudScoreRangeRule(Rule):
    rule_id = "fraud_score_range"
    severity = MUST_PASS
    source = "fraud"
    description = "Fraud score must be in [0, 1000]."

    def check(self, context):
        fraud = context["fraud"]
        failures = [f for f in fraud
                    if not isinstance(f.get("score"), (int, float))
                    or not (0 <= f["score"] <= 1000)]
        return self._result(fraud, failures)


class AmlStatusEnumRule(Rule):
    rule_id = "aml_status_enum"
    severity = MUST_PASS
    source = "aml"
    description = "AML status must be one of CLEAR, REVIEW, HIT."

    def check(self, context):
        aml = context["aml"]
        valid = {"CLEAR", "REVIEW", "HIT"}
        failures = [a for a in aml if a.get("status") not in valid]
        return self._result(aml, failures)


class ProfileDobPlausibleRule(Rule):
    rule_id = "profile_dob_plausible"
    severity = MUST_PASS
    source = "profile"
    description = "DOB must be between 1900-01-01 and today − 18 years."

    def check(self, context):
        profiles = context["profile"]
        today = datetime.now(timezone.utc).date()
        cutoff = today.replace(year=today.year - 18)
        failures = []
        for p in profiles:
            try:
                d = datetime.fromisoformat(p["dob"]).date()
            except (KeyError, ValueError):
                failures.append(p)
                continue
            if not (datetime(1900, 1, 1).date() <= d <= cutoff):
                failures.append(p)
        return self._result(profiles, failures)


class AecbFreshnessRule(Rule):
    rule_id = "aecb_freshness_30d"
    severity = MUST_PASS
    source = "aecb"
    description = "Latest AECB report must be within 30 days."

    MAX_AGE_DAYS = 30

    def check(self, context):
        aecb = context["aecb"]
        if not aecb:
            return DQResult(
                rule_id=self.rule_id, severity=self.severity, source=self.source,
                description=self.description, checked=0, failed=1,
                threshold=0.0, breached=True, sample_failures=[{"reason": "no aecb data"}],
            )
        cutoff = (datetime.now(timezone.utc).date()
                  - timedelta(days=self.MAX_AGE_DAYS))
        failures = []
        for a in aecb:
            try:
                report_date = datetime.fromisoformat(a.get("report_date") or "").date()
            except ValueError:
                failures.append(a)
                continue
            if report_date < cutoff:
                failures.append(a)
        return self._result(aecb, failures)


class AmlCoverageRule(Rule):
    rule_id = "aml_coverage"
    severity = WARN
    source = "aml"
    description = "At most 5% of profile customers may be missing an AML callback."

    def check(self, context):
        profile_eids = {p["emirates_id"] for p in context["profile"]}
        aml_eids = {a["emirates_id"] for a in context["aml"]}
        missing = profile_eids - aml_eids
        return DQResult(
            rule_id=self.rule_id, severity=self.severity, source=self.source,
            description=self.description,
            checked=len(profile_eids), failed=len(missing),
            threshold=self.threshold,
            breached=(len(missing) / max(1, len(profile_eids))) > self.threshold,
            sample_failures=[{"emirates_id": e} for e in list(missing)[:3]],
        )


class FraudPhoneConflictRule(Rule):
    rule_id = "fraud_phone_conflict"
    severity = WARN
    source = "fraud"
    description = "Phone mismatch between fraud and profile should be rare."

    def check(self, context):
        profile_phone = {p["emirates_id"]: p["phone"] for p in context["profile"]}
        conflicts = []
        checked = 0
        for f in context["fraud"]:
            if f["emirates_id"] in profile_phone:
                checked += 1
                if f["phone"] != profile_phone[f["emirates_id"]]:
                    conflicts.append({
                        "emirates_id": f["emirates_id"],
                        "profile_phone": profile_phone[f["emirates_id"]],
                        "fraud_phone": f["phone"],
                    })
        return DQResult(
            rule_id=self.rule_id, severity=self.severity, source=self.source,
            description=self.description,
            checked=checked, failed=len(conflicts),
            threshold=self.threshold,
            breached=(len(conflicts) / max(1, checked)) > self.threshold,
            sample_failures=conflicts[:3],
        )


class ErQuarantineRule(Rule):
    rule_id = "er_quarantine_rate"
    severity = WARN
    source = "er"
    description = "At most 2% of records should land in entity resolution quarantine."

    def check(self, context):
        links = context["er_links"]
        quarantine = context["er_quarantine"]
        total = len(links) + len(quarantine)
        return DQResult(
            rule_id=self.rule_id, severity=self.severity, source=self.source,
            description=self.description,
            checked=total, failed=len(quarantine),
            threshold=self.threshold,
            breached=(len(quarantine) / max(1, total)) > self.threshold,
            sample_failures=quarantine[:3],
        )


# --- Registry -----------------------------------------------------------------

RULES: list[Rule] = [
    EidFormatRule(),
    FraudScoreRangeRule(),
    AmlStatusEnumRule(),
    ProfileDobPlausibleRule(),
    AecbFreshnessRule(),
    AmlCoverageRule(),
    FraudPhoneConflictRule(),
    ErQuarantineRule(),
]
