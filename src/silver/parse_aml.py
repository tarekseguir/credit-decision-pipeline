"""Parse AML/PEP webhook callbacks into silver records."""
from __future__ import annotations

import json

from src.models import SilverAML
from src.silver._io import iter_bronze_records, write_silver_table


def parse_aml(ingest_run_id: str) -> list[dict]:
    records: list[dict] = []
    for envelope, raw in iter_bronze_records("aml"):
        payload = json.loads(raw)
        for item in payload:
            rec = SilverAML(
                callback_id=item["callback_id"],
                emirates_id=item["emirates_id"].strip().upper(),
                full_name=item["full_name"],
                dob=item["dob"],
                status=item["status"],
                matched_lists=item.get("matched_lists", []),
                received_at=item["received_at"],
                ingest_run_id=envelope.ingest_run_id,
            )
            records.append(rec.model_dump())

    write_silver_table("aml", "screening", records)
    return records
