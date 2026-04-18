"""DAG tasks for decisioning and audit trail."""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import stat

from src.audit import verify_chain, write_snapshot
from src.config import AUDIT_DB, AUDIT_DIR, SERVING_DB
from src.decision import run_all_decisions


def _reset_audit_state() -> None:
    """Wipe snapshots and the audit DB so a fresh run starts from genesis.

    The chain uses an auto-increment seq for ordering, which is stable
    within one audit.db. Between runs, we wipe and re-build so the chain
    starts from GENESIS each time. In production (S3 Object Lock), runs
    would be separated by date partitions and the chain per-product-per-day,
    so reset wouldn't be needed.
    """
    def _remove_readonly(func, path, _exc):
        """shutil.rmtree error handler — make file writable and retry."""
        try:
            os.chmod(path, stat.S_IWRITE | stat.S_IREAD)
            func(path)
        except OSError:
            pass

    if AUDIT_DIR.exists():
        shutil.rmtree(AUDIT_DIR, onerror=_remove_readonly)
    if AUDIT_DB.exists():
        try:
            AUDIT_DB.unlink()
        except PermissionError:
            os.chmod(AUDIT_DB, stat.S_IWRITE | stat.S_IREAD)
            AUDIT_DB.unlink()


def decision_task(context: dict) -> None:
    """Run every decision, write snapshot, and persist to fact_decision."""
    _reset_audit_state()

    pairs = run_all_decisions()

    # Write snapshots (slow path — one file per decision)
    snapshots = []
    for outcome, vector in pairs:
        snap = write_snapshot(outcome, vector)
        snapshots.append(snap)

    # Bulk-insert fact_decision rows
    with sqlite3.connect(SERVING_DB) as conn:
        conn.execute("DELETE FROM fact_decision")
        conn.executemany(
            """INSERT INTO fact_decision (
                decision_id, emirates_id, product, decision_ts, outcome,
                reason_codes, engine_version, rule_pack_version,
                credit_score, fraud_score, aml_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [(
                o.decision_id, o.emirates_id, o.product,
                o.decision_ts.isoformat(), o.outcome,
                json.dumps(o.reason_codes),
                o.engine_version, o.rule_pack_version,
                v.get("credit_score"), v.get("fraud_score"), v.get("aml_status"),
            ) for o, v in pairs],
        )

    # Summary stats
    outcomes: dict[str, int] = {}
    for o, _ in pairs:
        outcomes[o.outcome] = outcomes.get(o.outcome, 0) + 1

    context["decisions_made"] = len(pairs)
    context["outcomes"] = outcomes
    print(f"decisions={len(pairs)} "
          f"approve={outcomes.get('APPROVE', 0)} "
          f"decline={outcomes.get('DECLINE', 0)} "
          f"refer={outcomes.get('REFER', 0)}", end="")


def chain_verify_task(context: dict) -> None:
    """Post-run integrity check on the audit chain."""
    issues = verify_chain()
    context["chain_issues"] = [
        {"decision_id": i.decision_id, "kind": i.kind, "detail": i.detail}
        for i in issues
    ]
    if issues:
        # Show the first few issues so the root cause is visible in the logs
        print(f"\n    First {min(3, len(issues))} issues:")
        for i in issues[:3]:
            print(f"      [{i.kind}] {i.decision_id}: {i.detail}")
        # Count by kind for a quick summary
        by_kind: dict[str, int] = {}
        for i in issues:
            by_kind[i.kind] = by_kind.get(i.kind, 0) + 1
        print(f"    Breakdown: {by_kind}")
        raise RuntimeError(f"audit chain integrity check failed: {len(issues)} issue(s)")
    print("audit chain intact", end="")
