"""
Entity resolution: link every silver record to a canonical Emirates ID.

Strategy (per architecture doc §3):

  Tier 1  Emirates ID present & valid        conf 1.00
  Tier 2  phone + email match a profile      conf 0.95  (backfill Emirates ID)
  Tier 3  name + DOB fuzzy (Jaro-Winkler)    conf 0.75  (backfill Emirates ID)
  else    quarantine                         conf 0.00

Only AECB and Profile sources are allowed to introduce new Emirates IDs.
Fraud and AML can only attach to existing customers; unresolved fraud/AML
records are quarantined for human review (prevents phantom customers from
noisy upstream data).

Output:
  silver/er/links.ndjson         — one ERLink per resolved record
  silver/er/quarantine.ndjson    — records that failed all tiers
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

from rapidfuzz.distance import JaroWinkler

from src.config import NAME_FUZZY_THRESHOLD, SILVER_DIR, Source
from src.models import ERLink
from src.silver._io import read_silver_table, write_silver_table


EID_PATTERN = re.compile(r"^784-\d{4}-\d{7}-\d$")


# --- Lookup index -------------------------------------------------------------

class ProfileIndex:
    """In-memory index over the customer profile table.

    Profile is the authoritative source for identity, so every other source
    resolves against it. Indices are rebuilt every silver run (small data)
    but would be cached in production.
    """

    def __init__(self, profiles: list[dict]):
        self.by_eid: dict[str, dict] = {}
        self.by_phone_email: dict[tuple[str, str], dict] = {}
        self.by_dob: dict[str, list[dict]] = {}

        for p in profiles:
            eid = p["emirates_id"]
            self.by_eid[eid] = p
            self.by_phone_email[(p["phone"], p["email"].lower())] = p
            self.by_dob.setdefault(p["dob"], []).append(p)

    def lookup_eid(self, eid: str | None) -> dict | None:
        if not eid:
            return None
        return self.by_eid.get(eid)

    def lookup_phone_email(self, phone: str, email: str) -> dict | None:
        return self.by_phone_email.get((phone, email.lower()))

    def lookup_name_dob_fuzzy(self, full_name: str, dob: str) -> tuple[dict | None, float]:
        candidates = self.by_dob.get(dob, [])
        if not candidates:
            return None, 0.0

        scored = [
            (c, JaroWinkler.similarity(full_name.lower(), c["full_name"].lower()))
            for c in candidates
        ]
        strong = [(c, s) for c, s in scored if s >= NAME_FUZZY_THRESHOLD]

        if len(strong) == 1:
            return strong[0]
        # 0 matches or >1 strong matches → ambiguous
        return None, max((s for _, s in scored), default=0.0)


# --- Resolver -----------------------------------------------------------------

def _valid_eid(eid: str | None) -> bool:
    return bool(eid) and EID_PATTERN.match(eid) is not None


def resolve_record(
    *,
    source: str,
    source_event_id: str,
    emirates_id: str | None,
    phone: str | None,
    email: str | None,
    full_name: str | None,
    dob: str | None,
    index: ProfileIndex,
) -> ERLink:
    """Run the 4-tier resolver on one record and return the ERLink."""
    now = datetime.now(timezone.utc)

    # Tier 1 — Emirates ID exact match
    if _valid_eid(emirates_id):
        hit = index.lookup_eid(emirates_id)
        if hit is not None:
            return ERLink(
                source=source, source_event_id=source_event_id,
                emirates_id=emirates_id, match_tier="eid_exact",
                confidence=1.00, resolved_at=now,
            )
        # New Emirates ID — only Profile and AECB may mint one
        if source in (Source.PROFILE, Source.AECB):
            return ERLink(
                source=source, source_event_id=source_event_id,
                emirates_id=emirates_id, match_tier="eid_exact",
                confidence=1.00, resolved_at=now,
                notes="new customer (EID not yet in profile index)",
            )

    # Tier 2 — phone + email match
    if phone and email:
        hit = index.lookup_phone_email(phone, email)
        if hit is not None:
            return ERLink(
                source=source, source_event_id=source_event_id,
                emirates_id=hit["emirates_id"], match_tier="phone_email_exact",
                confidence=0.95, resolved_at=now,
                notes="backfilled emirates_id from profile",
            )

    # Tier 3 — name + DOB fuzzy
    if full_name and dob:
        hit, score = index.lookup_name_dob_fuzzy(full_name, dob)
        if hit is not None:
            return ERLink(
                source=source, source_event_id=source_event_id,
                emirates_id=hit["emirates_id"], match_tier="name_dob_fuzzy",
                confidence=round(score, 3), resolved_at=now,
                notes=f"jaro-winkler={score:.3f}, backfilled emirates_id",
            )

    # Quarantine
    return ERLink(
        source=source, source_event_id=source_event_id,
        emirates_id=emirates_id or "",
        match_tier="quarantine", confidence=0.0, resolved_at=now,
        notes="no match in any tier",
    )


# --- Batch drivers ------------------------------------------------------------

def _extract_record_inputs(source: str, rec: dict) -> dict:
    """Map each silver record's fields onto the generic resolver inputs."""
    common = {
        "source": source,
        "emirates_id": rec.get("emirates_id"),
        "phone": rec.get("phone"),
        "email": rec.get("email"),
        "full_name": rec.get("full_name"),
        "dob": rec.get("dob"),
    }
    if source == Source.AECB:
        common["source_event_id"] = rec["report_id"]
    elif source == Source.FRAUD:
        common["source_event_id"] = rec["provider_ref"]
    elif source == Source.AML:
        common["source_event_id"] = rec["callback_id"]
    elif source == Source.PROFILE:
        common["source_event_id"] = rec["internal_uuid"]
    return common


def resolve_all() -> dict:
    """Resolve every non-profile silver record against the profile index."""
    profiles = read_silver_table("profile", "customer")
    index = ProfileIndex(profiles)

    links: list[dict] = []
    quarantine: list[dict] = []
    tier_counts: dict[str, int] = {}

    # Profiles themselves resolve trivially — every profile IS the canonical record.
    for p in profiles:
        link = resolve_record(**_extract_record_inputs(Source.PROFILE, p), index=index)
        links.append(link.model_dump())
        tier_counts[link.match_tier] = tier_counts.get(link.match_tier, 0) + 1

    for source, table in [(Source.AECB, "credit_report"),
                          (Source.FRAUD, "score"),
                          (Source.AML, "screening")]:
        for rec in read_silver_table(source, table):
            link = resolve_record(**_extract_record_inputs(source, rec), index=index)
            if link.match_tier == "quarantine":
                quarantine.append({**link.model_dump(), "source_record": rec})
            else:
                links.append(link.model_dump())
            tier_counts[link.match_tier] = tier_counts.get(link.match_tier, 0) + 1

    _write_ndjson(SILVER_DIR / "er" / "links.ndjson", links)
    _write_ndjson(SILVER_DIR / "er" / "quarantine.ndjson", quarantine)

    return {
        "total_links": len(links),
        "total_quarantine": len(quarantine),
        "tier_counts": tier_counts,
    }


def _write_ndjson(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    import json
    with path.open("w") as f:
        for r in records:
            f.write(json.dumps(r, default=str) + "\n")
