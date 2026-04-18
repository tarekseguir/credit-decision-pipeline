"""
Bronze layer — the immutable raw zone.

For every file in landing/, we:
  1. Compute a SHA-256 over the raw bytes.
  2. Copy the file into bronze/<source>/ingest_date=YYYY-MM-DD/ using the hash
     as part of the filename (so duplicates naturally collapse on disk).
  3. Emit a provenance envelope as a sidecar JSON file.

The envelope is the bronze contract — every downstream layer can trust
these fields even if the payload itself is malformed. In production S3 would
do this with Object Lock; here we simply mark bronze files read-only.

Bronze makes no attempt to parse or validate source content. That's silver's job.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from src.config import BRONZE_DIR, LANDING_DIR, Source
from src.models import BronzeEnvelope


# --- Helpers ------------------------------------------------------------------

def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _source_format(source: str) -> str:
    if source == Source.AECB:
        return "xml"
    if source == Source.PROFILE:
        return "sqlite"
    return "json"


def _mark_readonly(path: Path) -> None:
    path.chmod(path.stat().st_mode & ~stat.S_IWUSR & ~stat.S_IWGRP & ~stat.S_IWOTH)


def _iter_source_files(source: str) -> Iterable[Path]:
    """Walk the landing area for a given source."""
    source_dir = LANDING_DIR / source
    if not source_dir.exists():
        return []
    if source == Source.AECB:
        return sorted(source_dir.glob("*.xml"))
    if source == Source.PROFILE:
        return sorted(source_dir.glob("*.db"))
    # fraud / aml deliver JSON
    return sorted(source_dir.glob("*.json"))


# --- Landing -----------------------------------------------------------------

def _land_one(source: str, src_path: Path, ingest_run_id: str, ingest_date: str) -> BronzeEnvelope:
    payload_format = _source_format(source)
    content_hash = _sha256_file(src_path)

    dest_dir = BRONZE_DIR / source / f"ingest_date={ingest_date}"
    dest_dir.mkdir(parents=True, exist_ok=True)

    dest = dest_dir / f"{content_hash[:16]}_{src_path.name}"

    if not dest.exists():
        shutil.copy2(src_path, dest)
        _mark_readonly(dest)

    envelope = BronzeEnvelope(
        source=source,
        source_event_id=src_path.stem,
        ingest_run_id=ingest_run_id,
        received_at=datetime.now(timezone.utc),
        content_sha256=content_hash,
        raw_path=str(dest.relative_to(BRONZE_DIR.parent)),
        payload_format=payload_format,
    )
    envelope_path = dest.with_suffix(dest.suffix + ".envelope.json")
    envelope_path.write_text(envelope.model_dump_json(indent=2))
    _mark_readonly(envelope_path)

    return envelope


def land_source(source: str, ingest_run_id: str) -> dict:
    """Land all files for a single source. Idempotent: re-running the same
    payload is a no-op because the destination filename is content-addressed."""
    ingest_date = datetime.now(timezone.utc).date().isoformat()

    envelopes: list[BronzeEnvelope] = []
    for src_path in _iter_source_files(source):
        envelopes.append(_land_one(source, src_path, ingest_run_id, ingest_date))

    return {
        "source": source,
        "files_landed": len(envelopes),
        "unique_hashes": len({e.content_sha256 for e in envelopes}),
    }


def land_all(ingest_run_id: str) -> list[dict]:
    """Land every source. Returns per-source summaries."""
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    summaries = []
    for source in Source.ALL:
        summaries.append(land_source(source, ingest_run_id))
    return summaries


# --- Task function for the DAG -----------------------------------------------

def bronze_land_task(context: dict) -> None:
    """DAG-compatible entrypoint. Stashes summaries in context for later tasks."""
    run_id = context["run_id"]
    summaries = land_all(run_id)
    context["bronze_summary"] = summaries
    total = sum(s["files_landed"] for s in summaries)
    unique = sum(s["unique_hashes"] for s in summaries)
    print(f"landed {total} files ({unique} unique)", end="")


if __name__ == "__main__":
    # Allow manual invocation for testing
    import uuid
    run_id = f"manual_{uuid.uuid4().hex[:8]}"
    for s in land_all(run_id):
        print(s)
