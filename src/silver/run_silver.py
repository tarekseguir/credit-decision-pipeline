"""Silver task functions — thin wrappers that the DAG calls."""
from __future__ import annotations

from src.silver.conflict_log import detect_conflicts
from src.silver.entity_resolution import resolve_all
from src.silver.parse_aecb import parse_aecb
from src.silver.parse_aml import parse_aml
from src.silver.parse_fraud import parse_fraud
from src.silver.parse_profile import parse_profile


def silver_parse_task(context: dict) -> None:
    """Parse all 4 sources from bronze into silver tables."""
    run_id = context["run_id"]

    profile_n = len(parse_profile(run_id))
    aecb_n = len(parse_aecb(run_id))
    fraud_n = len(parse_fraud(run_id))
    aml_n = len(parse_aml(run_id))

    context["silver_counts"] = {
        "profile": profile_n,
        "aecb": aecb_n,
        "fraud": fraud_n,
        "aml": aml_n,
    }
    print(f"profile={profile_n} aecb={aecb_n} fraud={fraud_n} aml={aml_n}", end="")


def silver_er_task(context: dict) -> None:
    """Run entity resolution over all silver records."""
    summary = resolve_all()
    context["er_summary"] = summary
    tc = summary["tier_counts"]
    print(
        f"links={summary['total_links']} "
        f"quarantine={summary['total_quarantine']} "
        f"[eid={tc.get('eid_exact', 0)} "
        f"phone_email={tc.get('phone_email_exact', 0)} "
        f"fuzzy={tc.get('name_dob_fuzzy', 0)}]",
        end="",
    )


def silver_conflict_task(context: dict) -> None:
    """Detect and log field-level conflicts between sources."""
    conflicts = detect_conflicts()
    context["conflict_count"] = len(conflicts)
    print(f"conflicts={len(conflicts)}", end="")
