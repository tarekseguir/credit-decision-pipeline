"""Parse fraud provider JSON responses into silver records."""
from __future__ import annotations

import json

from src.models import SilverFraud
from src.silver._io import iter_bronze_records, write_silver_table


def parse_fraud(ingest_run_id: str) -> list[dict]:
    records: list[dict] = []
    for envelope, raw in iter_bronze_records("fraud"):
        payload = json.loads(raw)
        for item in payload:
            rec = SilverFraud(
                provider_ref=item["provider_ref"],
                emirates_id=item["emirates_id"].strip().upper(),
                phone=item["phone"],
                email=item["email"].lower(),
                score=item["score"],
                decision=item["decision"],
                reason_codes=item.get("reason_codes", []),
                model_version=item["model_version"],
                scored_at=item["scored_at"],
                ingest_run_id=envelope.ingest_run_id,
            )
            records.append(rec.model_dump())

    write_silver_table("fraud", "score", records)
    return records
