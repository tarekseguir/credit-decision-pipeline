"""
Decision engine.

Reads decision_input_vector rows, dispatches to the correct rule pack per
product, and returns DecisionOutcome objects. Does NOT persist anything —
the snapshot writer is responsible for durability.
"""
from __future__ import annotations

import sqlite3
import ulid
from datetime import datetime, timezone
from typing import Iterator

from src.config import DECISION_ENGINE_VERSION, SERVING_DB, Product
from src.decision.rule_packs import RulePack
from src.decision.rule_packs.bnpl_v1 import BnplPack
from src.decision.rule_packs.credit_card_alt_v1 import CreditCardAltPack
from src.decision.rule_packs.personal_finance_v1 import PersonalFinancePack
from src.models import DecisionOutcome


PACKS: dict[str, RulePack] = {
    Product.PERSONAL_FINANCE: PersonalFinancePack(),
    Product.BNPL: BnplPack(),
    Product.CREDIT_CARD_ALT: CreditCardAltPack(),
}


def _row_to_vector(row: sqlite3.Row) -> dict:
    """Turn a sqlite row into the dict the rule packs expect."""
    import json
    d = dict(row)
    d["aecb_available"] = bool(d["aecb_available"])
    d["fraud_available"] = bool(d["fraud_available"])
    d["aml_available"] = bool(d["aml_available"])
    d["fraud_reason_codes"] = json.loads(d["fraud_reason_codes"] or "[]")
    d["aml_matched_lists"] = json.loads(d["aml_matched_lists"] or "[]")
    return d


def iter_vectors() -> Iterator[dict]:
    with sqlite3.connect(SERVING_DB) as conn:
        conn.row_factory = sqlite3.Row
        for row in conn.execute("SELECT * FROM decision_input_vector"):
            yield _row_to_vector(row)


def decide(vector: dict) -> DecisionOutcome:
    """Run one decision. Pure function of the vector + the rule pack."""
    pack = PACKS[vector["product"]]
    outcome, reasons = pack.evaluate(vector)

    return DecisionOutcome(
        decision_id=f"dcn_{ulid.new()}",
        emirates_id=vector["emirates_id"],
        product=vector["product"],
        decision_ts=datetime.now(timezone.utc),
        outcome=outcome,
        reason_codes=reasons,
        engine_version=DECISION_ENGINE_VERSION,
        rule_pack_version=pack.version,
    )


def run_all_decisions() -> list[tuple[DecisionOutcome, dict]]:
    """Evaluate every vector. Returns (outcome, vector) pairs so the
    snapshot writer can bundle both into the audit trail."""
    return [(decide(v), v) for v in iter_vectors()]
