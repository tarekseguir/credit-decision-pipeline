"""
DQ runner: load silver tables, run every rule, persist results, report.
"""
from __future__ import annotations

import json
import sqlite3

from src.config import SERVING_DB, Source
from src.dq.rules import MUST_PASS, RULES, DQResult
from src.silver._io import read_silver_table, SILVER_DIR


DQ_SCHEMA = """
CREATE TABLE IF NOT EXISTS dq_result (
    run_id          TEXT NOT NULL,
    rule_id         TEXT NOT NULL,
    severity        TEXT NOT NULL,
    source          TEXT NOT NULL,
    description     TEXT,
    checked         INTEGER NOT NULL,
    failed          INTEGER NOT NULL,
    fail_ratio      REAL NOT NULL,
    threshold       REAL NOT NULL,
    breached        INTEGER NOT NULL,
    status          TEXT NOT NULL,
    sample_failures TEXT,
    evaluated_at    TEXT NOT NULL,
    PRIMARY KEY (run_id, rule_id)
);
"""


def _load_context() -> dict:
    """Load all silver data needed by the rules."""
    er_links = []
    er_quarantine = []

    links_path = SILVER_DIR / "er" / "links.ndjson"
    if links_path.exists():
        er_links = [json.loads(line) for line in links_path.read_text().splitlines() if line.strip()]

    quar_path = SILVER_DIR / "er" / "quarantine.ndjson"
    if quar_path.exists():
        er_quarantine = [json.loads(line) for line in quar_path.read_text().splitlines() if line.strip()]

    return {
        "profile": read_silver_table(Source.PROFILE, "customer"),
        "aecb":    read_silver_table(Source.AECB, "credit_report"),
        "fraud":   read_silver_table(Source.FRAUD, "score"),
        "aml":     read_silver_table(Source.AML, "screening"),
        "er_links": er_links,
        "er_quarantine": er_quarantine,
    }


def run_all_rules() -> list[DQResult]:
    context = _load_context()
    return [rule.check(context) for rule in RULES]


def persist_results(run_id: str, results: list[DQResult]) -> None:
    with sqlite3.connect(SERVING_DB) as conn:
        conn.executescript(DQ_SCHEMA)
        for r in results:
            d = r.to_dict()
            conn.execute(
                """INSERT OR REPLACE INTO dq_result
                   (run_id, rule_id, severity, source, description,
                    checked, failed, fail_ratio, threshold, breached,
                    status, sample_failures, evaluated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (run_id, d["rule_id"], d["severity"], d["source"], d["description"],
                 d["checked"], d["failed"], d["fail_ratio"], d["threshold"],
                 1 if d["breached"] else 0, d["status"],
                 json.dumps(d["sample_failures"], default=str), d["evaluated_at"]),
            )


def must_pass_breaches(results: list[DQResult]) -> list[DQResult]:
    return [r for r in results if r.severity == MUST_PASS and r.breached]


def print_scorecard(results: list[DQResult]) -> None:
    print()
    print(f"  {'Rule':<26} {'Severity':<10} {'Chkd':>5} {'Fail':>5} {'Ratio':>7} {'Status':<6}")
    print(f"  {'-'*26} {'-'*10} {'-'*5} {'-'*5} {'-'*7} {'-'*6}")
    for r in results:
        status_icon = "✅" if not r.breached else ("❌" if r.severity == MUST_PASS else "⚠️ ")
        print(f"  {r.rule_id:<26} {r.severity:<10} {r.checked:>5} "
              f"{r.failed:>5} {r.fail_ratio:>7.2%} {status_icon} {r.status_text}")


# --- Task functions -----------------------------------------------------------

def dq_scorecard_task(context: dict) -> None:
    """Run every DQ rule and persist results."""
    run_id = context["run_id"]
    results = run_all_rules()
    persist_results(run_id, results)

    context["dq_results"] = [r.to_dict() for r in results]
    context["dq_breaches"] = [r.to_dict() for r in must_pass_breaches(results)]

    warn_count = sum(1 for r in results if r.breached and r.severity != MUST_PASS)
    fail_count = len(must_pass_breaches(results))
    print(f"ran {len(results)} rules — {fail_count} fail, {warn_count} warn", end="")


def dq_gate_task(context: dict) -> None:
    """Fail the DAG if any MUST_PASS rule breached."""
    breaches = context.get("dq_breaches", [])
    if breaches:
        rule_ids = ", ".join(r["rule_id"] for r in breaches)
        raise RuntimeError(
            f"Gold promotion blocked — {len(breaches)} MUST_PASS rule(s) breached: {rule_ids}"
        )
    print("all must-pass rules clean — promotion allowed", end="")
