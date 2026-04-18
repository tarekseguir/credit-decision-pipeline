"""
Immutable decision snapshot writer.

For every decision we write:

  1. A content-addressed feature-vector file
       data/audit/feature_vectors/<sha256[:2]>/<sha256>.json

  2. A snapshot file linking the vector, the inputs, and the outcome,
     chained to the previous snapshot's hash:
       data/audit/snapshots/dt=YYYY-MM-DD/product=<product>/<decision_id>.json

  3. A row in audit.db indexing every snapshot for fast lookup, with the
     previous-hash pointer forming an append-only chain per product.

Immutability is enforced locally by marking files read-only (chmod -w).
In production this would be S3 Object Lock in Compliance mode.

Hash chain design:
  each_snapshot = sha256(canonical_json(snapshot_without_this_hash))
  snapshot.prev_snapshot_sha256 points to the previous one for the same product.
  Any byte tampering is detected by re-walking the chain from genesis.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import stat
from datetime import datetime, timezone
from pathlib import Path

from src.config import AUDIT_DB, AUDIT_DIR, PIPELINE_VERSION
from src.models import DecisionOutcome


GENESIS_HASH = "0" * 64


AUDIT_SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshot (
    seq                    INTEGER PRIMARY KEY AUTOINCREMENT,
    decision_id            TEXT NOT NULL UNIQUE,
    emirates_id            TEXT NOT NULL,
    product                TEXT NOT NULL,
    decision_ts            TEXT NOT NULL,
    outcome                TEXT NOT NULL,
    feature_vector_sha256  TEXT NOT NULL,
    prev_snapshot_sha256   TEXT NOT NULL,
    this_snapshot_sha256   TEXT NOT NULL,
    snapshot_path          TEXT NOT NULL,
    engine_version         TEXT NOT NULL,
    rule_pack_version      TEXT NOT NULL,
    pipeline_version       TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_snapshot_product_seq ON snapshot(product, seq);
CREATE INDEX IF NOT EXISTS idx_snapshot_emirates    ON snapshot(emirates_id);
"""


def _canonical_sha256(obj) -> str:
    """Stable hash — sort keys, compact separators, utf-8."""
    payload = json.dumps(obj, sort_keys=True, separators=(",", ":"),
                         default=str, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _mark_readonly(path: Path) -> None:
    path.chmod(path.stat().st_mode & ~stat.S_IWUSR & ~stat.S_IWGRP & ~stat.S_IWOTH)


def _ensure_schema() -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(AUDIT_DB) as conn:
        conn.executescript(AUDIT_SCHEMA)


def _last_hash_for_product(product: str) -> str:
    with sqlite3.connect(AUDIT_DB) as conn:
        row = conn.execute(
            "SELECT this_snapshot_sha256 FROM snapshot "
            "WHERE product = ? ORDER BY seq DESC LIMIT 1",
            (product,),
        ).fetchone()
    return row[0] if row else GENESIS_HASH


def _write_feature_vector(vector: dict) -> tuple[str, str]:
    """Write the feature vector to a content-addressed file. Returns (hash, path)."""
    vector_hash = _canonical_sha256(vector)
    fv_dir = AUDIT_DIR / "feature_vectors" / vector_hash[:2]
    fv_dir.mkdir(parents=True, exist_ok=True)
    fv_path = fv_dir / f"{vector_hash}.json"
    if not fv_path.exists():
        fv_path.write_text(json.dumps(vector, sort_keys=True, indent=2, default=str))
        _mark_readonly(fv_path)
    return vector_hash, str(fv_path.relative_to(AUDIT_DIR.parent))


def write_snapshot(outcome: DecisionOutcome, vector: dict) -> dict:
    """Write one snapshot. Returns the snapshot dict (for dashboards / tests)."""
    _ensure_schema()

    fv_hash, fv_path = _write_feature_vector(vector)
    prev_hash = _last_hash_for_product(outcome.product)
    dt = outcome.decision_ts.date().isoformat()

    body = {
        "decision_id": outcome.decision_id,
        "emirates_id": outcome.emirates_id,
        "product": outcome.product,
        "decision_ts": outcome.decision_ts.isoformat(),
        "outcome": outcome.outcome,
        "reason_codes": outcome.reason_codes,
        "engine_version": outcome.engine_version,
        "rule_pack_version": outcome.rule_pack_version,
        "pipeline_version": PIPELINE_VERSION,
        "feature_vector_sha256": fv_hash,
        "feature_vector_path": fv_path,
        "prev_snapshot_sha256": prev_hash,
    }
    body["this_snapshot_sha256"] = _canonical_sha256(body)

    out_dir = AUDIT_DIR / "snapshots" / f"dt={dt}" / f"product={outcome.product}"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{outcome.decision_id}.json"
    path.write_text(json.dumps(body, sort_keys=True, indent=2))
    _mark_readonly(path)

    with sqlite3.connect(AUDIT_DB) as conn:
        conn.execute(
            """INSERT INTO snapshot (
                decision_id, emirates_id, product, decision_ts, outcome,
                feature_vector_sha256, prev_snapshot_sha256,
                this_snapshot_sha256, snapshot_path,
                engine_version, rule_pack_version, pipeline_version
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (outcome.decision_id, outcome.emirates_id, outcome.product,
             outcome.decision_ts.isoformat(), outcome.outcome,
             fv_hash, prev_hash, body["this_snapshot_sha256"],
             str(path.relative_to(AUDIT_DIR.parent)),
             outcome.engine_version, outcome.rule_pack_version, PIPELINE_VERSION),
        )

    return body
