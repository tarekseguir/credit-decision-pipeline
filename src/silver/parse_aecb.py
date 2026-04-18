"""
Parse AECB credit bureau reports into silver records.

AECB delivers a SOAP envelope wrapping a CRIF MessageGateway response,
which in turn contains an NAE (National Account Enquiry) response in the
urn:NAE namespace. The parser navigates through the envelope and extracts
only the fields the decision engine actually consumes.

Fields extracted per report:

  Identity       EmiratesId, full name, DOB (DDMMYYYY → ISO), nationality,
                 AECB subject ID, contract ID
  Score          Index (numeric), Range letter, PaymentOrderFlag
  Risk signals   # bounced cheques and total amount (Severity=1 payment orders)
                 # court cases, # open cases, total claim amount
  Income         gross annual income from the most recent EmploymentInfo

Emirates ID may be absent from EnquiredSubject but present in MatchedSubject;
we check both. If neither has it, the report is emitted with emirates_id=None
and relies on fallback entity resolution.
"""
from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Optional

from src.models import SilverAECB
from src.silver._io import iter_bronze_records, write_silver_table


# Namespaces we need. Prefix names are arbitrary, URIs are fixed.
NS = {
    "soap": "http://schemas.xmlsoap.org/soap/envelope/",
    "mg":   "urn:crif-messagegateway:2006-08-23",
    "nae":  "urn:NAE",
    "msg":  "urn:crif-message:2006-08-23",
}


# --- Helpers ----------------------------------------------------------------

def _text(node: Optional[ET.Element], tag: str, ns: dict = NS) -> Optional[str]:
    """Get the text of a direct child element by namespaced tag."""
    if node is None:
        return None
    el = node.find(tag, ns)
    return el.text.strip() if el is not None and el.text else None


def _findall(node: Optional[ET.Element], tag: str, ns: dict = NS) -> list[ET.Element]:
    return list(node.findall(tag, ns)) if node is not None else []


def _normalise_eid(eid: Optional[str]) -> Optional[str]:
    if not eid:
        return None
    return eid.strip().upper()


def _normalise_dob(dob: Optional[str]) -> str:
    """AECB delivers DOB as DDMMYYYY. Convert to ISO YYYY-MM-DD."""
    if not dob or len(dob) != 8 or not dob.isdigit():
        return dob or ""
    try:
        return datetime.strptime(dob, "%d%m%Y").date().isoformat()
    except ValueError:
        return dob


def _normalise_date(d: Optional[str]) -> Optional[str]:
    """AECB dates are DDMMYYYY; normalise to ISO."""
    if not d or len(d) != 8 or not d.isdigit():
        return d
    try:
        return datetime.strptime(d, "%d%m%Y").date().isoformat()
    except ValueError:
        return d


def _to_float(s: Optional[str]) -> float:
    try:
        return float(s) if s is not None else 0.0
    except ValueError:
        return 0.0


def _to_int(s: Optional[str]) -> Optional[int]:
    try:
        return int(s) if s is not None else None
    except ValueError:
        return None


# --- Subject identity -------------------------------------------------------

def _extract_subject_identity(nae_res: ET.Element) -> dict:
    """EmiratesId can live on EnquiredSubject or MatchedSubject. Prefer MatchedSubject's
    canonical record for the name but fall back to EnquiredSubject if needed."""
    enquired = nae_res.find("nae:EnquiredSubject/nae:Individual", NS)
    matched = nae_res.find("nae:Response/nae:MatchedSubject/nae:Individual", NS)

    eid = (_text(enquired, "nae:EmiratesId")
           or _text(matched, "nae:EmiratesId"))

    full_name = _text(matched, "nae:FullNameEN")
    if not full_name:
        first = _text(enquired, "nae:NameEN/nae:FirstName") or ""
        last = _text(enquired, "nae:NameEN/nae:LastName") or ""
        full_name = (first + " " + last).strip()

    dob = _text(enquired, "nae:DOB") or _text(matched, "nae:DOB")
    nationality = _text(enquired, "nae:Nationality") or _text(matched, "nae:Nationality")

    cb_subject_id = _text(
        nae_res.find("nae:Response/nae:MatchedSubject", NS),
        "nae:CBSubjectId",
    )

    return {
        "emirates_id": _normalise_eid(eid),
        "full_name": full_name,
        "dob": _normalise_dob(dob),
        "nationality": nationality,
        "cb_subject_id": cb_subject_id,
    }


# --- Score ------------------------------------------------------------------

def _extract_score(nae_res: ET.Element) -> dict:
    score = nae_res.find("nae:Response/nae:Score", NS)
    if score is None:
        return {"credit_score": None, "score_range": None, "payment_order_flag": False}

    data = score.find("nae:Data", NS)
    return {
        "credit_score": _to_int(_text(data, "nae:Index")),
        "score_range": _text(data, "nae:Range"),
        "payment_order_flag": (_text(score, "nae:PaymentOrderFlag") or "").upper() == "Y",
    }


# --- Risk signals -----------------------------------------------------------

def _extract_payment_orders(nae_res: ET.Element) -> tuple[int, float]:
    """Count severity-1 payment orders (bounced cheques) and sum amounts."""
    response = nae_res.find("nae:Response", NS)
    orders = _findall(response, "nae:PaymentOrder")

    count = 0
    total = 0.0
    for po in orders:
        sev = _text(po, "nae:Severity")
        if sev == "1":
            count += 1
            total += _to_float(_text(po, "nae:Amount"))
    return count, total


def _extract_cases(nae_res: ET.Element) -> tuple[int, int, float]:
    """Court cases — (total, open, total_claim_amount). Status code 90 = closed."""
    response = nae_res.find("nae:Response", NS)
    cases = _findall(response, "nae:Cases")

    total = len(cases)
    open_count = 0
    total_claim = 0.0
    for c in cases:
        status = _text(c, "nae:CaseStatusCode")
        if status != "90":
            open_count += 1
        total_claim += _to_float(_text(c, "nae:InitialTotalClaimAmount"))
    return total, open_count, total_claim


def _extract_latest_income(nae_res: ET.Element) -> Optional[float]:
    """Pick the most recent EmploymentInfo with ActualFlag='1'."""
    response = nae_res.find("nae:Response", NS)
    if response is None:
        return None

    employments = _findall(response, "nae:EmploymentHistory")
    best_date = None
    best_income = None

    for emp in employments:
        info = emp.find("nae:EmploymentInfo", NS)
        if info is None or _text(info, "nae:ActualFlag") != "1":
            continue
        updates = _findall(emp, "nae:UpdateInfo")
        # Find the max DateOfUpdate across all update-info blocks on this employment
        dates = [_text(u, "nae:DateOfUpdate") for u in updates]
        dates = [_normalise_date(d) for d in dates if d]
        if not dates:
            continue
        latest = max(dates)
        income = _to_float(_text(info, "nae:GrossAnnualIncome"))
        if best_date is None or latest > best_date:
            best_date, best_income = latest, income

    return best_income


# --- Main parser ------------------------------------------------------------

def _find_nae_response(root: ET.Element) -> Optional[ET.Element]:
    """Drill through the SOAP envelope to the NAE_RES node."""
    return root.find(
        "./soap:Body/mg:MGResponse/mg:MGResponse/nae:NAE_RES", NS
    )


def parse_aecb_xml(raw: bytes, ingest_run_id: str) -> Optional[SilverAECB]:
    root = ET.fromstring(raw)
    nae_res = _find_nae_response(root)
    if nae_res is None:
        return None

    identity = _extract_subject_identity(nae_res)
    score = _extract_score(nae_res)
    n_bounced, total_bounced = _extract_payment_orders(nae_res)
    n_cases, n_open, total_claim = _extract_cases(nae_res)
    income = _extract_latest_income(nae_res)

    # Report date: prefer ResponseId's parent timestamp via MessageResponse/@MTs
    message_response = root.find("./soap:Header/msg:MessageResponse", NS)
    mts = message_response.get("MTs") if message_response is not None else None
    report_date = (mts[:10] if mts else datetime.now().date().isoformat())

    report_id = (_text(nae_res.find("nae:NewApplication", NS), "nae:CBContractId")
                 or _text(nae_res, "nae:ResponseId")
                 or "unknown")

    return SilverAECB(
        report_id=report_id,
        cb_subject_id=identity["cb_subject_id"],
        report_date=report_date,
        emirates_id=identity["emirates_id"],
        full_name=identity["full_name"],
        dob=identity["dob"],
        nationality=identity["nationality"],
        credit_score=score["credit_score"],
        score_range=score["score_range"],
        payment_order_flag=score["payment_order_flag"],
        num_bounced_cheques=n_bounced,
        total_bounced_cheques_amount=total_bounced,
        num_court_cases=n_cases,
        num_open_court_cases=n_open,
        total_claim_amount=total_claim,
        gross_annual_income=income,
        ingest_run_id=ingest_run_id,
    )


def parse_aecb(ingest_run_id: str) -> list[dict]:
    records: list[dict] = []
    for envelope, raw in iter_bronze_records("aecb"):
        try:
            rec = parse_aecb_xml(raw, envelope.ingest_run_id)
        except ET.ParseError as exc:
            print(f"⚠️  malformed AECB {envelope.source_event_id}: {exc}")
            continue
        if rec is None:
            print(f"⚠️  no NAE_RES in {envelope.source_event_id}")
            continue
        records.append(rec.model_dump())

    write_silver_table("aecb", "credit_report", records)
    return records
