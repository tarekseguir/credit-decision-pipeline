"""
Shared helpers for silver parsers.

All parsers read from bronze (not landing) because bronze is the immutable
source of truth. Each parser walks the bronze folder for its source, loads
the envelope + raw payload, and produces normalised records.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Iterator

from src.config import BRONZE_DIR, SILVER_DIR
from src.models import BronzeEnvelope


def iter_bronze_records(source: str) -> Iterator[tuple[BronzeEnvelope, bytes]]:
    """Yield (envelope, raw_bytes) pairs for every landed file in a source."""
    source_root = BRONZE_DIR / source
    if not source_root.exists():
        return

    for envelope_path in sorted(source_root.rglob("*.envelope.json")):
        envelope = BronzeEnvelope.model_validate_json(envelope_path.read_text())
        raw_path = envelope_path.with_name(envelope_path.name.replace(".envelope.json", ""))
        if not raw_path.exists():
            continue
        yield envelope, raw_path.read_bytes()


def write_silver_table(source: str, table: str, records: list[dict]) -> Path:
    """Write normalised records as newline-delimited JSON (one per line).

    NDJSON is used here rather than parquet to keep the demo dependency-light
    and make files diffable in git. The schema would be identical in parquet.
    """
    out_dir = SILVER_DIR / source
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{table}.ndjson"

    with path.open("w") as f:
        for rec in records:
            f.write(json.dumps(rec, default=str) + "\n")
    return path


def read_silver_table(source: str, table: str) -> list[dict]:
    path = SILVER_DIR / source / f"{table}.ndjson"
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
