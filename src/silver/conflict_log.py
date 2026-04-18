"""
Conflict log.

When the same logical field appears in multiple sources (e.g. name in profile
vs AECB), disagreements are recorded here for audit. The "winner" is applied
per the precedence rules from the architecture doc; the "loser" is logged
verbatim so an auditor can reconstruct what was ignored and why.

Precedence (from §3):

  emirates_id   → AECB wins over profile       (bureau is authoritative for govt IDs)
  phone         → profile wins over fraud      (customer updates profile directly)
  email         → profile wins over fraud
  full_name     → profile wins over AECB/AML   (KYC-verified)
  dob           → profile wins over AECB/AML

Output:
  silver/conflict/log.ndjson
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from src.config import SILVER_DIR, Source
from src.models import ConflictLogEntry
from src.silver._io import read_silver_table


# Precedence: for each field, list of (source, source_table) in priority order.
PRECEDENCE: dict[str, list[tuple[str, str]]] = {
    "phone":     [(Source.PROFILE, "customer"), (Source.FRAUD, "score")],
    "email":     [(Source.PROFILE, "customer"), (Source.FRAUD, "score")],
    "full_name": [(Source.PROFILE, "customer"), (Source.AECB, "credit_report"),
                  (Source.AML, "screening")],
    "dob":       [(Source.PROFILE, "customer"), (Source.AECB, "credit_report"),
                  (Source.AML, "screening")],
}


def _index_by_eid(records: list[dict]) -> dict[str, dict]:
    """Index silver records by emirates_id, keeping the first seen."""
    out: dict[str, dict] = {}
    for r in records:
        eid = r.get("emirates_id")
        if eid and eid not in out:
            out[eid] = r
    return out


def _normalise_for_compare(field: str, value: str) -> str:
    """Normalise a value before comparison so case/whitespace diffs aren't flagged."""
    if field in ("full_name",):
        return value.strip().upper()
    if field in ("email",):
        return value.strip().lower()
    if field in ("phone",):
        return value.strip()
    return str(value).strip()


def detect_conflicts() -> list[dict]:
    """Compare sources field-by-field per Emirates ID and log disagreements."""
    source_indices: dict[str, dict[str, dict]] = {
        Source.PROFILE: _index_by_eid(read_silver_table(Source.PROFILE, "customer")),
        Source.AECB:    _index_by_eid(read_silver_table(Source.AECB, "credit_report")),
        Source.FRAUD:   _index_by_eid(read_silver_table(Source.FRAUD, "score")),
        Source.AML:     _index_by_eid(read_silver_table(Source.AML, "screening")),
    }

    now = datetime.now(timezone.utc)
    entries: list[ConflictLogEntry] = []

    for eid in source_indices[Source.PROFILE].keys():
        for field, order in PRECEDENCE.items():
            values: list[tuple[str, str]] = []
            for source, _table in order:
                rec = source_indices[source].get(eid)
                if rec is not None and rec.get(field):
                    values.append((source, str(rec[field])))

            if len(values) < 2:
                continue

            win_source, win_value = values[0]
            win_norm = _normalise_for_compare(field, win_value)
            for lose_source, lose_value in values[1:]:
                if _normalise_for_compare(field, lose_value) != win_norm:
                    entries.append(ConflictLogEntry(
                        emirates_id=eid,
                        field=field,
                        winning_source=win_source,
                        winning_value=win_value,
                        losing_source=lose_source,
                        losing_value=lose_value,
                        logged_at=now,
                    ))

    out = SILVER_DIR / "conflict" / "log.ndjson"
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w") as f:
        for e in entries:
            f.write(json.dumps(e.model_dump(), default=str) + "\n")

    return [e.model_dump() for e in entries]
