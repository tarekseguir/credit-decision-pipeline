"""
Generate fake data for all 4 sources.

Profile source is now a SQLite database with three tables:
  users         — one row per customer (identity)
  applications  — prior credit applications with outcome
  instalments   — payment history per approved application

This mirrors how a real internal customer platform would be laid out:
users are authoritative for identity, applications and instalments drive
behavioural features (is_new_user, had_overdue_before).
"""
from __future__ import annotations

import argparse
import json
import random
import sqlite3
import uuid
import xml.etree.ElementTree as ET
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

FIRST_NAMES = ["Ahmed", "Mohammed", "Fatima", "Aisha", "Omar", "Yusuf", "Layla",
               "Zainab", "Khalid", "Noura", "Hassan", "Mariam", "Ali", "Sara",
               "Hamad", "Maryam", "Saeed", "Hind", "Rashid", "Shamsa"]
LAST_NAMES = ["Al Mansoori", "Al Maktoum", "Al Nahyan", "Al Qasimi", "Al Nuaimi",
              "Al Sharqi", "Al Mualla", "Khan", "Patel", "Sharma",
              "Hussein", "Abdullah", "Rashid", "Al Hashimi", "Al Marzooqi"]
EMIRATES = ["Dubai", "Abu Dhabi", "Sharjah", "Ajman", "Ras Al Khaimah",
            "Fujairah", "Umm Al Quwain"]
FRAUD_REASONS = ["DEVICE_MISMATCH", "VELOCITY", "GEO_ANOMALY", "EMAIL_AGE",
                 "IP_REPUTATION", "SYNTHETIC_ID_RISK"]
PRODUCTS_PRIOR = ["personal_finance", "bnpl", "credit_card_alt"]


PROFILE_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    internal_uuid    TEXT PRIMARY KEY,
    emirates_id      TEXT NOT NULL UNIQUE,
    first_name       TEXT NOT NULL,
    last_name        TEXT NOT NULL,
    full_name        TEXT NOT NULL,
    dob              TEXT NOT NULL,
    phone            TEXT NOT NULL,
    email            TEXT NOT NULL,
    emirate          TEXT NOT NULL,
    kyc_verified_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS applications (
    application_id    TEXT PRIMARY KEY,
    internal_uuid     TEXT NOT NULL REFERENCES users(internal_uuid),
    product           TEXT NOT NULL,
    amount            REAL NOT NULL,
    decision          TEXT NOT NULL,
    applied_at        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS instalments (
    instalment_id    TEXT PRIMARY KEY,
    application_id   TEXT NOT NULL REFERENCES applications(application_id),
    due_date         TEXT NOT NULL,
    amount           REAL NOT NULL,
    paid_at          TEXT,
    status           TEXT NOT NULL,
    overdue_days     INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_app_user ON applications(internal_uuid);
CREATE INDEX IF NOT EXISTS idx_inst_app ON instalments(application_id);
"""


@dataclass
class Customer:
    internal_uuid: str
    emirates_id: str
    first_name: str
    last_name: str
    dob: str
    phone: str
    email: str
    emirate: str

    @property
    def full_name(self) -> str:
        return f"{self.first_name} {self.last_name}"


def _emirates_id() -> str:
    year = random.randint(1970, 2005)
    serial = random.randint(1_000_000, 9_999_999)
    check = random.randint(0, 9)
    return f"784-{year}-{serial}-{check}"


def _phone() -> str:
    return f"+9715{random.randint(0, 9)}{random.randint(1_000_000, 9_999_999)}"


def _email(first: str, last: str) -> str:
    return f"{first.lower()}.{last.replace(' ', '').lower()}{random.randint(1, 999)}@example.ae"


def _dob() -> str:
    start = datetime(1960, 1, 1)
    end = datetime(2005, 1, 1)
    delta = (end - start).days
    return (start + timedelta(days=random.randint(0, delta))).date().isoformat()


def _sample_indices(n: int, pct: float) -> set[int]:
    return set(random.sample(range(n), k=max(1, int(n * pct))))


def generate_customers(n: int) -> list[Customer]:
    customers = []
    for _ in range(n):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        customers.append(Customer(
            internal_uuid=str(uuid.uuid4()),
            emirates_id=_emirates_id(),
            first_name=first, last_name=last,
            dob=_dob(), phone=_phone(), email=_email(first, last),
            emirate=random.choice(EMIRATES),
        ))
    return customers


def _generate_application_history(
    customer: Customer, is_new: bool, had_overdue: bool
) -> tuple[list[dict], list[dict]]:
    if is_new:
        return [], []

    apps: list[dict] = []
    instalments: list[dict] = []

    num_apps = random.randint(1, 4)
    for _ in range(num_apps):
        app_id = f"app_{uuid.uuid4().hex[:12]}"
        applied_at = datetime.now(timezone.utc) - timedelta(days=random.randint(60, 720))
        decision = random.choices(
            ["APPROVE", "DECLINE", "REFER"], weights=[70, 20, 10]
        )[0]
        amount = round(random.uniform(1000, 50000), 2)

        apps.append({
            "application_id": app_id,
            "internal_uuid": customer.internal_uuid,
            "product": random.choice(PRODUCTS_PRIOR),
            "amount": amount,
            "decision": decision,
            "applied_at": applied_at.isoformat(),
        })

        if decision != "APPROVE":
            continue

        num_instalments = random.choice([3, 6, 12])
        per_instalment = round(amount / num_instalments, 2)

        for i in range(num_instalments):
            due = applied_at + timedelta(days=30 * (i + 1))
            inst_id = f"inst_{uuid.uuid4().hex[:12]}"

            if due < datetime.now(timezone.utc):
                if had_overdue and random.random() < 0.25:
                    if random.random() < 0.5:
                        status, paid_at, overdue_days = (
                            "OVERDUE", None,
                            (datetime.now(timezone.utc) - due).days,
                        )
                    else:
                        paid_at_dt = due + timedelta(days=random.randint(5, 60))
                        status, paid_at, overdue_days = (
                            "PAID_LATE", paid_at_dt.isoformat(),
                            (paid_at_dt - due).days,
                        )
                else:
                    status, paid_at, overdue_days = (
                        "PAID_ON_TIME", due.isoformat(), 0,
                    )
            else:
                status, paid_at, overdue_days = "UPCOMING", None, 0

            instalments.append({
                "instalment_id": inst_id,
                "application_id": app_id,
                "due_date": due.date().isoformat(),
                "amount": per_instalment,
                "paid_at": paid_at,
                "status": status,
                "overdue_days": overdue_days,
            })

    return apps, instalments


def write_profile_sqlite(customers: list[Customer], out: Path) -> dict:
    db_path = out / "profile" / "profile.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()

    new_user_idx = _sample_indices(len(customers), 0.30)
    overdue_idx = _sample_indices(len(customers), 0.20)

    total_apps = 0
    total_instalments = 0

    with sqlite3.connect(db_path) as conn:
        conn.executescript(PROFILE_SCHEMA)

        for i, c in enumerate(customers):
            is_new = i in new_user_idx
            had_overdue = (i in overdue_idx) and not is_new

            conn.execute(
                """INSERT INTO users (
                    internal_uuid, emirates_id, first_name, last_name, full_name,
                    dob, phone, email, emirate, kyc_verified_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (c.internal_uuid, c.emirates_id, c.first_name, c.last_name,
                 c.full_name, c.dob, c.phone, c.email, c.emirate,
                 (datetime.now(timezone.utc)
                  - timedelta(days=random.randint(30, 730))).isoformat()),
            )

            apps, instalments = _generate_application_history(c, is_new, had_overdue)
            for a in apps:
                conn.execute(
                    """INSERT INTO applications VALUES (?, ?, ?, ?, ?, ?)""",
                    (a["application_id"], a["internal_uuid"], a["product"],
                     a["amount"], a["decision"], a["applied_at"]),
                )
            for inst in instalments:
                conn.execute(
                    """INSERT INTO instalments VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (inst["instalment_id"], inst["application_id"],
                     inst["due_date"], inst["amount"], inst["paid_at"],
                     inst["status"], inst["overdue_days"]),
                )
            total_apps += len(apps)
            total_instalments += len(instalments)

    return {
        "users": len(customers),
        "new_users": len(new_user_idx),
        "had_overdue": len(overdue_idx),
        "applications": total_apps,
        "instalments": total_instalments,
    }


# --- AECB XML (unchanged — will swap when real sample arrives) --------------

def _ddmmyyyy(iso_date: str) -> str:
    """Convert ISO YYYY-MM-DD to AECB's DDMMYYYY."""
    d = datetime.fromisoformat(iso_date).date()
    return d.strftime("%d%m%Y")


def _aecb_xml(customer: Customer, omit_eid: bool, delinquent: bool) -> str:
    """Generate an AECB-format SOAP envelope with a CRIF NAE_RES payload.

    Matches the real structure shared by the business — soap:Envelope
    → soap:Body → MGResponse → NAE_RES in the urn:NAE namespace.
    Names here are deliberately faithful so the real parser can consume them.
    """
    # Register namespaces so ElementTree writes the correct prefixes
    ET.register_namespace("soap", "http://schemas.xmlsoap.org/soap/envelope/")
    ET.register_namespace("u3", "urn:NAE")

    envelope = ET.Element(
        "{http://schemas.xmlsoap.org/soap/envelope/}Envelope",
        {
            "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
        },
    )

    # --- Header ---
    header = ET.SubElement(envelope, "{http://schemas.xmlsoap.org/soap/envelope/}Header")
    msg_resp = ET.SubElement(
        header, "{urn:crif-message:2006-08-23}MessageResponse",
        {
            "GId": f"G{random.randint(1, 999)}",
            "MId": f"MSG{random.randint(1, 999999):06d}",
            "MTs": datetime.now(timezone.utc).isoformat(),
            "METs": datetime.now(timezone.utc).isoformat(),
        },
    )

    # --- Body / MGResponse / NAE_RES ---
    body = ET.SubElement(envelope, "{http://schemas.xmlsoap.org/soap/envelope/}Body")
    mg_outer = ET.SubElement(body, "{urn:crif-messagegateway:2006-08-23}MGResponse")
    mg_inner = ET.SubElement(mg_outer, "{urn:crif-messagegateway:2006-08-23}MGResponse")
    nae = ET.SubElement(mg_inner, "{urn:NAE}NAE_RES")

    ET.SubElement(nae, "{urn:NAE}ResponseId").text = str(uuid.uuid4())

    new_app = ET.SubElement(nae, "{urn:NAE}NewApplication")
    ET.SubElement(new_app, "{urn:NAE}CBContractId").text = f"A{random.randint(10000000, 99999999)}"
    ET.SubElement(new_app, "{urn:NAE}ProviderApplicationNo").text = (
        f"APPL_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
    )

    # Enquired subject
    enq = ET.SubElement(nae, "{urn:NAE}EnquiredSubject")
    enq_ind = ET.SubElement(enq, "{urn:NAE}Individual")
    enq_name = ET.SubElement(enq_ind, "{urn:NAE}NameEN")
    ET.SubElement(enq_name, "{urn:NAE}FirstName").text = customer.first_name.upper()
    ET.SubElement(enq_name, "{urn:NAE}LastName").text = customer.last_name.upper()
    ET.SubElement(enq_ind, "{urn:NAE}Gender").text = random.choice(["M", "F"])
    ET.SubElement(enq_ind, "{urn:NAE}DOB").text = _ddmmyyyy(customer.dob)
    ET.SubElement(enq_ind, "{urn:NAE}Nationality").text = random.choice(["AE", "EG", "IN", "PK", "PH"])
    if not omit_eid:
        ET.SubElement(enq_ind, "{urn:NAE}EmiratesId").text = customer.emirates_id
    ET.SubElement(enq_ind, "{urn:NAE}PrimaryMobileNo").text = customer.phone.lstrip("+")

    # Response / MatchedSubject
    response = ET.SubElement(nae, "{urn:NAE}Response")
    matched = ET.SubElement(response, "{urn:NAE}MatchedSubject")
    ET.SubElement(matched, "{urn:NAE}CBSubjectId").text = str(random.randint(100000000, 999999999))
    m_ind = ET.SubElement(matched, "{urn:NAE}Individual")
    m_name = ET.SubElement(m_ind, "{urn:NAE}NameEN")
    ET.SubElement(m_name, "{urn:NAE}FirstName").text = customer.first_name.upper()
    ET.SubElement(m_name, "{urn:NAE}LastName").text = customer.last_name.upper()
    ET.SubElement(m_ind, "{urn:NAE}FullNameEN").text = customer.full_name.upper()
    ET.SubElement(m_ind, "{urn:NAE}Gender").text = random.choice(["M", "F"])
    ET.SubElement(m_ind, "{urn:NAE}DOB").text = _ddmmyyyy(customer.dob)
    if not omit_eid:
        ET.SubElement(m_ind, "{urn:NAE}EmiratesId").text = customer.emirates_id

    # Employment history — one actual record
    emp_hist = ET.SubElement(response, "{urn:NAE}EmploymentHistory")
    update = ET.SubElement(emp_hist, "{urn:NAE}UpdateInfo")
    ET.SubElement(update, "{urn:NAE}ProviderNo").text = f"B{random.randint(1, 20):02d}"
    ET.SubElement(update, "{urn:NAE}DateOfUpdate").text = _ddmmyyyy(
        (datetime.now(timezone.utc) - timedelta(days=random.randint(30, 730))).date().isoformat()
    )
    emp_info = ET.SubElement(emp_hist, "{urn:NAE}EmploymentInfo")
    ET.SubElement(emp_info, "{urn:NAE}Name").text = random.choice(
        ["ACME TRADING", "UAE TECH SOLUTIONS", "EMIRATES LOGISTICS", "DUBAI RETAIL GROUP"]
    )
    ET.SubElement(emp_info, "{urn:NAE}GrossAnnualIncome").text = str(
        random.choice([60000, 80000, 120000, 180000, 240000, 360000])
    )
    ET.SubElement(emp_info, "{urn:NAE}ActualFlag").text = "1"

    # Payment orders (bounced cheques) — only for delinquent customers
    if delinquent:
        for _ in range(random.randint(1, 5)):
            po = ET.SubElement(response, "{urn:NAE}PaymentOrder")
            ET.SubElement(po, "{urn:NAE}ProviderNo").text = f"B{random.randint(1, 20):02d}"
            ET.SubElement(po, "{urn:NAE}Type").text = "1"
            ET.SubElement(po, "{urn:NAE}IBAN").text = f"{'*' * 19}{random.randint(1000, 9999)}"
            ET.SubElement(po, "{urn:NAE}Number").text = str(random.randint(100000, 999999))
            ET.SubElement(po, "{urn:NAE}Amount").text = str(random.randint(5000, 100000))
            ET.SubElement(po, "{urn:NAE}ReasonCode").text = "A"
            ret_date = _ddmmyyyy(
                (datetime.now(timezone.utc) - timedelta(days=random.randint(30, 700))).date().isoformat()
            )
            ET.SubElement(po, "{urn:NAE}ReturnDate").text = ret_date
            ET.SubElement(po, "{urn:NAE}Severity").text = "1"
            ET.SubElement(po, "{urn:NAE}ReferenceDate").text = ret_date
            ET.SubElement(po, "{urn:NAE}ChequeStatus").text = "Bounced"

    # Court cases — some delinquent customers have open cases
    if delinquent and random.random() < 0.5:
        for _ in range(random.randint(1, 3)):
            case = ET.SubElement(response, "{urn:NAE}Cases")
            ET.SubElement(case, "{urn:NAE}CodOrganization").text = random.choice(
                ["DXB Courts", "RAK Courts", "AUH Courts"]
            )
            ET.SubElement(case, "{urn:NAE}ProviderCaseNo").text = f"{random.randint(100, 999)}-2023-{random.randint(1000, 99999)}"
            ET.SubElement(case, "{urn:NAE}ReferenceDate").text = _ddmmyyyy(
                datetime.now(timezone.utc).date().isoformat()
            )
            ET.SubElement(case, "{urn:NAE}CaseOpenDate").text = _ddmmyyyy(
                (datetime.now(timezone.utc) - timedelta(days=random.randint(90, 500))).date().isoformat()
            )
            # status 55 = open, 90 = closed
            ET.SubElement(case, "{urn:NAE}CaseStatusCode").text = random.choice(["55", "55", "90"])
            ET.SubElement(case, "{urn:NAE}InitialTotalClaimAmount").text = str(random.randint(10000, 200000))

    # Score
    score_node = ET.SubElement(response, "{urn:NAE}Score")
    data = ET.SubElement(score_node, "{urn:NAE}Data")
    # AECB scores are on a different scale in the real XML (300 seen in sample with range 'A')
    if delinquent:
        score_val = random.randint(300, 500)
        score_range = random.choice(["A", "B"])
    else:
        score_val = random.randint(500, 900)
        score_range = random.choice(["C", "D", "E", "F"])
    ET.SubElement(data, "{urn:NAE}Index").text = str(score_val)
    ET.SubElement(data, "{urn:NAE}Range").text = score_range
    ET.SubElement(score_node, "{urn:NAE}PaymentOrderFlag").text = "Y" if delinquent else "N"

    xml_str = ET.tostring(envelope, encoding="unicode", xml_declaration=True)
    return xml_str


def write_aecb(customers: list[Customer], out: Path) -> dict:
    path = out / "aecb"
    path.mkdir(parents=True, exist_ok=True)

    missing_eid = _sample_indices(len(customers), 0.05)
    duplicates = _sample_indices(len(customers), 0.02)
    delinquent = _sample_indices(len(customers), 0.15)

    written = 0
    for i, c in enumerate(customers):
        xml = _aecb_xml(c, omit_eid=(i in missing_eid), delinquent=(i in delinquent))
        filename = f"{c.emirates_id.replace('-', '')}_{datetime.now(timezone.utc).date().isoformat()}.xml"
        (path / filename).write_text(xml)
        written += 1
        if i in duplicates:
            (path / f"dup_{filename}").write_text(xml)
            written += 1

    return {
        "files_written": written, "missing_eid": len(missing_eid),
        "duplicates": len(duplicates), "delinquent": len(delinquent),
    }


# --- Fraud -------------------------------------------------------------------

def _mutate_string(s: str) -> str:
    if len(s) < 4:
        return s
    i = random.randint(1, len(s) - 2)
    chars = list(s)
    chars[i] = str(random.randint(0, 9)) if chars[i].isdigit() else chr(ord(chars[i]) + 1)
    return "".join(chars)


def write_fraud(customers: list[Customer], out: Path) -> dict:
    phone_conflict = _sample_indices(len(customers), 0.10)
    email_conflict = _sample_indices(len(customers), 0.05)
    high_risk = _sample_indices(len(customers), 0.08)

    records = []
    for i, c in enumerate(customers):
        phone = _mutate_string(c.phone) if i in phone_conflict else c.phone
        email = _mutate_string(c.email) if i in email_conflict else c.email

        if i in high_risk:
            score = random.randint(800, 1000)
            decision = "FAIL"
            reasons = random.sample(FRAUD_REASONS, k=random.randint(2, 3))
        else:
            score = random.randint(0, 700)
            decision = "REVIEW" if score > 500 else "PASS"
            reasons = random.sample(FRAUD_REASONS, k=0 if decision == "PASS" else 1)

        records.append({
            "provider_ref": f"frd_{uuid.uuid4().hex[:12]}",
            "emirates_id": c.emirates_id, "phone": phone, "email": email,
            "score": score, "decision": decision, "reason_codes": reasons,
            "model_version": "v3.2.1",
            "scored_at": datetime.now(timezone.utc).isoformat(),
        })

    path = out / "fraud" / "scores.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(records, indent=2))
    return {
        "records_written": len(records),
        "phone_conflict": len(phone_conflict),
        "email_conflict": len(email_conflict),
        "high_risk": len(high_risk),
    }


# --- AML ---------------------------------------------------------------------

def _name_variant(name: str) -> str:
    parts = name.split()
    if len(parts[0]) < 4:
        return name
    first = parts[0]
    i = random.randint(1, len(first) - 2)
    mutated = first[:i] + first[i + 1] + first[i] + first[i + 2:]
    return " ".join([mutated] + parts[1:])


def write_aml(customers: list[Customer], out: Path) -> dict:
    name_variant = _sample_indices(len(customers), 0.05)
    skipped = _sample_indices(len(customers), 0.03)
    hits = _sample_indices(len(customers), 0.02)

    records = []
    for i, c in enumerate(customers):
        if i in skipped:
            continue
        name = _name_variant(c.full_name) if i in name_variant else c.full_name

        if i in hits:
            status = "HIT"
            lists = random.choice([["UN_SANCTIONS"], ["PEP_DOMESTIC"],
                                   ["OFAC", "UN_SANCTIONS"]])
        else:
            status = random.choices(["CLEAR", "REVIEW"], weights=[95, 5])[0]
            lists = [] if status == "CLEAR" else ["PEP_FAMILY"]

        records.append({
            "callback_id": f"aml_{uuid.uuid4().hex[:12]}",
            "emirates_id": c.emirates_id,
            "full_name": name, "dob": c.dob,
            "status": status, "matched_lists": lists,
            "received_at": datetime.now(timezone.utc).isoformat(),
        })

    path = out / "aml" / "callbacks.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(records, indent=2))
    return {
        "records_written": len(records), "name_variants": len(name_variant),
        "missing_callbacks": len(skipped), "hits": len(hits),
    }


# --- Entry point -------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--customers", type=int, default=50)
    parser.add_argument("--output", type=Path, default=Path("data\sample"))
    args = parser.parse_args()

    args.output.mkdir(parents=True, exist_ok=True)

    print(f"Generating {args.customers} customers → {args.output}\n")
    customers = generate_customers(args.customers)

    (args.output / "_ground_truth.json").write_text(
        json.dumps([asdict(c) for c in customers], indent=2)
    )

    profile = write_profile_sqlite(customers, args.output)
    aecb = write_aecb(customers, args.output)
    fraud = write_fraud(customers, args.output)
    aml = write_aml(customers, args.output)

    print(f"  profile.db  users={profile['users']:>4} "
          f"(new={profile['new_users']}, had_overdue={profile['had_overdue']}) "
          f"apps={profile['applications']} instalments={profile['instalments']}")
    print(f"  aecb        {aecb['files_written']:>5} XML files  "
          f"({aecb['missing_eid']} missing EID, {aecb['duplicates']} dups, "
          f"{aecb['delinquent']} delinquent)")
    print(f"  fraud       {fraud['records_written']:>5} records    "
          f"({fraud['phone_conflict']} phone conflicts, "
          f"{fraud['email_conflict']} email conflicts, {fraud['high_risk']} high-risk)")
    print(f"  aml         {aml['records_written']:>5} callbacks  "
          f"({aml['name_variants']} name variants, "
          f"{aml['missing_callbacks']} missing, {aml['hits']} hits)")
    print(f"\nOutput: {args.output}")


if __name__ == "__main__":
    main()
