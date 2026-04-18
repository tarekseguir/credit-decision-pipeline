"""
Parse the internal customer profile SQLite database into silver records.

Reads the bronze-landed profile.db, joins users → applications → instalments,
and derives two behavioural features per user:

    is_new_user          True if the user has no applications at all
    had_overdue_before   True if any past instalment was OVERDUE or PAID_LATE

These features are materialised into the silver profile table so the decision
engine reads them at constant cost — no joins needed at serving time.
"""
from __future__ import annotations

import sqlite3

from src.models import SilverProfile
from src.silver._io import iter_bronze_records, write_silver_table


# The query that produces one silver row per user with behavioural features.
USER_FEATURES_SQL = """
SELECT
    u.internal_uuid,
    u.emirates_id,
    u.first_name,
    u.last_name,
    u.full_name,
    u.dob,
    u.phone,
    u.email,
    u.emirate,
    u.kyc_verified_at,

    CASE WHEN apps.app_count IS NULL OR apps.app_count = 0
         THEN 1 ELSE 0 END                              AS is_new_user,

    CASE WHEN bad.bad_count IS NOT NULL AND bad.bad_count > 0
         THEN 1 ELSE 0 END                              AS had_overdue_before

FROM users u

LEFT JOIN (
    SELECT internal_uuid, COUNT(*) AS app_count
    FROM applications
    GROUP BY internal_uuid
) apps ON apps.internal_uuid = u.internal_uuid

LEFT JOIN (
    SELECT a.internal_uuid, COUNT(*) AS bad_count
    FROM instalments i
    JOIN applications a ON a.application_id = i.application_id
    WHERE i.status IN ('OVERDUE', 'PAID_LATE')
    GROUP BY a.internal_uuid
) bad ON bad.internal_uuid = u.internal_uuid;
"""


def parse_profile(ingest_run_id: str) -> list[dict]:
    """Profile is delivered as a single SQLite DB file per ingest run.
    The bronze layer stored a byte-identical copy; we open it read-only
    and run the feature query against it."""
    records: list[dict] = []

    for envelope, raw in iter_bronze_records("profile"):
        # Write bytes to a temp file so sqlite3 can open it (can't open bytes directly)
        # In production this would be an S3 path; here we already have it on disk.
        # envelope.raw_path is relative to data/ — resolve it.
        from src.config import DATA_ROOT
        db_file = DATA_ROOT / envelope.raw_path

        conn = sqlite3.connect(f"file:{db_file}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(USER_FEATURES_SQL).fetchall()
        finally:
            conn.close()

        for row in rows:
            rec = SilverProfile(
                internal_uuid=row["internal_uuid"],
                emirates_id=row["emirates_id"].strip().upper(),
                first_name=row["first_name"],
                last_name=row["last_name"],
                full_name=row["full_name"],
                dob=row["dob"],
                phone=row["phone"],
                email=row["email"].lower(),
                emirate=row["emirate"],
                kyc_verified_at=row["kyc_verified_at"],
                ingest_run_id=envelope.ingest_run_id,
                is_new_user=bool(row["is_new_user"]),
                had_overdue_before=bool(row["had_overdue_before"]),
            )
            records.append(rec.model_dump())

    write_silver_table("profile", "customer", records)
    return records
