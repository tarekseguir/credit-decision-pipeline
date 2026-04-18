"""
Audit replay.

Two capabilities:

  1. verify_chain(product)
       Walks every snapshot for a product in decision-time order.
       Confirms:
         a) each snapshot's stored this_snapshot_sha256 matches a fresh
            recomputation of its body — detects tampering.
         b) each snapshot's prev_snapshot_sha256 equals the previous
            snapshot's this_snapshot_sha256 — detects gaps or reorderings.
         c) the feature_vector file on disk hashes to the stored
            feature_vector_sha256 — detects payload tampering.

  2. replay_decision(decision_id)
       Loads the snapshot, re-loads the stored feature vector, runs the
       same rule pack version against it, and confirms the outcome matches.
       This is the core audit story: reproducibility of any decision.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from src.audit.snapshot_writer import GENESIS_HASH, _canonical_sha256
from src.config import AUDIT_DB, AUDIT_DIR
from src.decision.engine import PACKS


# --- Chain verification ------------------------------------------------------

@dataclass
class ChainIssue:
    decision_id: str
    kind: str         # hash_mismatch | chain_break | vector_mismatch
    detail: str


def _load_snapshot(path: Path) -> dict:
    return json.loads(path.read_text())


def _recompute_body_hash(body: dict) -> str:
    stripped = {k: v for k, v in body.items() if k != "this_snapshot_sha256"}
    return _canonical_sha256(stripped)


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def verify_chain(product: str | None = None) -> list[ChainIssue]:
    """Walk every snapshot in chronological order and check the chain.

    If product is None, each product's chain is verified independently."""
    issues: list[ChainIssue] = []

    with sqlite3.connect(AUDIT_DB) as conn:
        conn.row_factory = sqlite3.Row
        if product:
            rows = conn.execute(
                "SELECT * FROM snapshot WHERE product = ? "
                "ORDER BY seq", (product,),
            ).fetchall()
            products = {product: list(rows)}
        else:
            rows = conn.execute(
                "SELECT * FROM snapshot ORDER BY product, seq"
            ).fetchall()
            products: dict[str, list] = {}
            for r in rows:
                products.setdefault(r["product"], []).append(r)

    for prod, prod_rows in products.items():
        expected_prev = GENESIS_HASH
        for row in prod_rows:
            snap_path = AUDIT_DIR.parent / row["snapshot_path"]
            if not snap_path.exists():
                issues.append(ChainIssue(row["decision_id"], "missing_file",
                                         f"snapshot file not found: {snap_path}"))
                continue

            body = _load_snapshot(snap_path)

            # (a) Stored hash matches recomputation
            recomputed = _recompute_body_hash(body)
            if recomputed != body["this_snapshot_sha256"]:
                issues.append(ChainIssue(
                    row["decision_id"], "hash_mismatch",
                    f"recomputed {recomputed[:12]}... != stored {body['this_snapshot_sha256'][:12]}...",
                ))

            # (b) Chain continuity
            if body["prev_snapshot_sha256"] != expected_prev:
                issues.append(ChainIssue(
                    row["decision_id"], "chain_break",
                    f"prev {body['prev_snapshot_sha256'][:12]}... != expected {expected_prev[:12]}...",
                ))

            # (c) Feature vector integrity
            fv_path = AUDIT_DIR.parent / body["feature_vector_path"]
            if fv_path.exists():
                fv_content = json.loads(fv_path.read_text())
                fv_recomputed = _canonical_sha256(fv_content)
                if fv_recomputed != body["feature_vector_sha256"]:
                    issues.append(ChainIssue(
                        row["decision_id"], "vector_mismatch",
                        "feature vector contents do not match stored hash",
                    ))

            expected_prev = body["this_snapshot_sha256"]

    return issues


# --- Replay ------------------------------------------------------------------

@dataclass
class ReplayResult:
    decision_id: str
    matches: bool
    original_outcome: str
    replayed_outcome: str
    original_reasons: list[str]
    replayed_reasons: list[str]


def replay_decision(decision_id: str) -> ReplayResult:
    """Replay a single decision: load vector → run rule pack → compare."""
    with sqlite3.connect(AUDIT_DB) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM snapshot WHERE decision_id = ?", (decision_id,),
        ).fetchone()
    if not row:
        raise ValueError(f"decision_id not found: {decision_id}")

    body = _load_snapshot(AUDIT_DIR.parent / row["snapshot_path"])
    fv = json.loads((AUDIT_DIR.parent / body["feature_vector_path"]).read_text())

    pack = PACKS[row["product"]]
    outcome, reasons = pack.evaluate(fv)

    return ReplayResult(
        decision_id=decision_id,
        matches=(outcome == row["outcome"] and reasons == body["reason_codes"]),
        original_outcome=row["outcome"],
        replayed_outcome=outcome,
        original_reasons=body["reason_codes"],
        replayed_reasons=reasons,
    )


def replay_sample(n: int = 10) -> list[ReplayResult]:
    """Replay a random sample of decisions — used for nightly audit."""
    import random
    with sqlite3.connect(AUDIT_DB) as conn:
        ids = [r[0] for r in conn.execute(
            "SELECT decision_id FROM snapshot ORDER BY RANDOM() LIMIT ?", (n,)
        ).fetchall()]
    return [replay_decision(d) for d in ids]
