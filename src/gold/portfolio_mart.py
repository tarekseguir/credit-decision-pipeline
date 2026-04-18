"""
Portfolio mart loader.

Populates dim_customer from silver.profile. fact_decision is populated by
the decision task (§decision/tasks.py); dim_product is seeded by schema.sql.
"""
from __future__ import annotations

import sqlite3

from src.config import SERVING_DB, Source
from src.silver._io import read_silver_table


def portfolio_mart_task(context: dict) -> None:
    profiles = read_silver_table(Source.PROFILE, "customer")
    with sqlite3.connect(SERVING_DB) as conn:
        conn.execute("DELETE FROM dim_customer")
        conn.executemany(
            """INSERT INTO dim_customer
               (emirates_id, full_name, emirate, kyc_verified_at)
               VALUES (?, ?, ?, ?)""",
            [(p["emirates_id"], p["full_name"], p["emirate"], p["kyc_verified_at"])
             for p in profiles],
        )
    context["dim_customer_count"] = len(profiles)
    print(f"customers={len(profiles)}", end="")
