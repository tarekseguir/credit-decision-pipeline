"""
Decision input vector assembly.

For each customer, and for each product, build the flat feature vector that
the decision engine will consume. The vector is point-in-time: we use the
latest silver record per source, joining on emirates_id.

Conflict precedence (from architecture §3):
  phone, email     → profile wins
  full_name, dob   → profile wins
  credit score etc → AECB
  fraud score etc  → Fraud
  aml status etc   → AML
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from src.config import GOLD_DIR, Product, SERVING_DB, Source
from src.silver._io import read_silver_table


def _schema_path() -> Path:
    return Path(__file__).parent / "sql" / "schema.sql"


def _ensure_schema() -> None:
    with sqlite3.connect(SERVING_DB) as conn:
        conn.executescript(_schema_path().read_text())


def _index_by_eid(rows: list[dict]) -> dict[str, dict]:
    """Keep the most recent row per Emirates ID (simple last-wins)."""
    return {r["emirates_id"]: r for r in rows if r.get("emirates_id")}


def _kyc_age_days(kyc_ts: str) -> int:
    ts = datetime.fromisoformat(kyc_ts)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - ts).days


def _aml_stale_seconds(received_at: str) -> int:
    ts = datetime.fromisoformat(received_at)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return int((datetime.now(timezone.utc) - ts).total_seconds())


def build_vectors() -> list[dict]:
    """Assemble one vector per (customer, product)."""
    profiles = _index_by_eid(read_silver_table(Source.PROFILE, "customer"))
    aecb =     _index_by_eid(read_silver_table(Source.AECB,    "credit_report"))
    fraud =    _index_by_eid(read_silver_table(Source.FRAUD,   "score"))
    aml =      _index_by_eid(read_silver_table(Source.AML,     "screening"))

    now = datetime.now(timezone.utc)
    vectors: list[dict] = []

    for eid, p in profiles.items():
        a = aecb.get(eid)
        f = fraud.get(eid)
        m = aml.get(eid)

        base = {
            "emirates_id": eid,
            "as_of_ts": now.isoformat(),
            "full_name": p["full_name"],
            "dob": p["dob"],
            "phone": p["phone"],
            "email": p["email"],
            "emirate": p["emirate"],
            "kyc_age_days": _kyc_age_days(p["kyc_verified_at"]),
            "is_new_user": bool(p.get("is_new_user", False)),
            "had_overdue_before": bool(p.get("had_overdue_before", False)),

            "credit_score": a["credit_score"] if a else None,
            "score_range": a["score_range"] if a else None,
            "payment_order_flag": bool(a["payment_order_flag"]) if a else False,
            "num_bounced_cheques": a["num_bounced_cheques"] if a else 0,
            "total_bounced_cheques_amount": a["total_bounced_cheques_amount"] if a else 0.0,
            "num_court_cases": a["num_court_cases"] if a else 0,
            "num_open_court_cases": a["num_open_court_cases"] if a else 0,
            "total_claim_amount": a["total_claim_amount"] if a else 0.0,
            "gross_annual_income": a["gross_annual_income"] if a else None,
            "aecb_available": a is not None,

            "fraud_score": f["score"] if f else None,
            "fraud_decision": f["decision"] if f else None,
            "fraud_reason_codes": f["reason_codes"] if f else [],
            "fraud_available": f is not None,

            "aml_status": m["status"] if m else None,
            "aml_matched_lists": m["matched_lists"] if m else [],
            "aml_available": m is not None,
            "aml_stale_seconds": _aml_stale_seconds(m["received_at"]) if m else None,
        }

        for product in Product.ALL:
            vectors.append({**base, "product": product})

    return vectors


def persist_vectors(vectors: list[dict]) -> None:
    _ensure_schema()
    with sqlite3.connect(SERVING_DB) as conn:
        conn.execute("DELETE FROM decision_input_vector")
        conn.executemany(
            """INSERT INTO decision_input_vector (
                emirates_id, product, as_of_ts,
                full_name, dob, phone, email, emirate, kyc_age_days,
                is_new_user, had_overdue_before,
                credit_score, score_range, payment_order_flag,
                num_bounced_cheques, total_bounced_cheques_amount,
                num_court_cases, num_open_court_cases, total_claim_amount,
                gross_annual_income, aecb_available,
                fraud_score, fraud_decision, fraud_reason_codes, fraud_available,
                aml_status, aml_matched_lists, aml_available, aml_stale_seconds
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [(
                v["emirates_id"], v["product"], v["as_of_ts"],
                v["full_name"], v["dob"], v["phone"], v["email"],
                v["emirate"], v["kyc_age_days"],
                1 if v["is_new_user"] else 0,
                1 if v["had_overdue_before"] else 0,
                v["credit_score"], v["score_range"],
                1 if v["payment_order_flag"] else 0,
                v["num_bounced_cheques"], v["total_bounced_cheques_amount"],
                v["num_court_cases"], v["num_open_court_cases"], v["total_claim_amount"],
                v["gross_annual_income"],
                1 if v["aecb_available"] else 0,
                v["fraud_score"], v["fraud_decision"],
                json.dumps(v["fraud_reason_codes"]),
                1 if v["fraud_available"] else 0,
                v["aml_status"], json.dumps(v["aml_matched_lists"]),
                1 if v["aml_available"] else 0,
                v["aml_stale_seconds"],
            ) for v in vectors]
        )

    # Also persist a snapshot to gold/ for parquet-like archival
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    with (GOLD_DIR / "decision_input_vector.ndjson").open("w") as f:
        for v in vectors:
            f.write(json.dumps(v, default=str) + "\n")


def gold_vector_task(context: dict) -> None:
    vectors = build_vectors()
    persist_vectors(vectors)
    context["vectors_built"] = len(vectors)
    unique_customers = len({v["emirates_id"] for v in vectors})
    print(f"built {len(vectors)} vectors ({unique_customers} customers × "
          f"{len(Product.ALL)} products)", end="")
