"""
Central configuration. Paths and thresholds live here so nothing else hardcodes them.

In production these would come from env vars / Parameter Store; for the demo a
single module keeps things obvious.
"""
from __future__ import annotations

from pathlib import Path

# --- Paths --------------------------------------------------------------------

ROOT = Path(__file__).resolve().parent.parent
DATA_ROOT = ROOT / "data"

LANDING_DIR = DATA_ROOT / "sample"         # committed sample dataset (50 customers)
BRONZE_DIR = DATA_ROOT / "bronze"          # immutable raw copies + provenance
SILVER_DIR = DATA_ROOT / "silver"          # parsed, validated, identity-resolved
GOLD_DIR = DATA_ROOT / "gold"              # business-ready parquet marts
AUDIT_DIR = DATA_ROOT / "audit"            # decision snapshots (hash-chained)

SERVING_DB = ROOT / "serving.db"           # SQLite: decision_input_vector + portfolio mart
AUDIT_DB = ROOT / "audit.db"               # SQLite: snapshot index (payloads stay on disk)
ORCHESTRATION_DB = ROOT / "orchestration.db"  # SQLite: DAG run history

# --- Versions -----------------------------------------------------------------

PIPELINE_VERSION = "0.1.0"
DECISION_ENGINE_VERSION = "v1.0.0"

# --- Source names -------------------------------------------------------------

class Source:
    AECB = "aecb"
    FRAUD = "fraud"
    AML = "aml"
    PROFILE = "profile"

    ALL = [AECB, FRAUD, AML, PROFILE]

# --- Products -----------------------------------------------------------------

class Product:
    PERSONAL_FINANCE = "personal_finance"
    BNPL = "bnpl"
    CREDIT_CARD_ALT = "credit_card_alt"

    ALL = [PERSONAL_FINANCE, BNPL, CREDIT_CARD_ALT]

# --- Data quality thresholds --------------------------------------------------

# Must-pass rules tolerate zero fails. Warn rules have per-rule thresholds.
WARN_THRESHOLDS = {
    "aml_coverage": 0.05,          # up to 5% of customers may have no AML callback
    "fraud_phone_conflict": 0.15,  # up to 15% phone mismatch between fraud and profile
    "er_quarantine_rate": 0.02,    # up to 2% of records may land in quarantine
}

# --- Entity resolution --------------------------------------------------------

NAME_FUZZY_THRESHOLD = 0.92        # Jaro-Winkler similarity floor for name+DOB matching
