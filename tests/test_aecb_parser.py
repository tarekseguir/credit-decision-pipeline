"""
Tests for the AECB parser using the real SOAP/CRIF/NAE sample XML.

Confirms we correctly extract identity, score, risk signals (bounced cheques,
court cases), and income from the genuine bureau response format.
"""
from __future__ import annotations

from pathlib import Path

from src.silver.parse_aecb import parse_aecb_xml


SAMPLE_XML = Path(__file__).resolve().parent.parent / "docs" / "aecb_sample.xml"


def test_parses_real_aecb_sample():
    raw = SAMPLE_XML.read_bytes()
    rec = parse_aecb_xml(raw, ingest_run_id="test_run")
    assert rec is not None


def test_identity_extracted_correctly():
    rec = parse_aecb_xml(SAMPLE_XML.read_bytes(), "test_run")
    assert rec.emirates_id == "784-1988-4973253-6"
    assert rec.full_name == "SHAFIK AHMED HUSSEIN MOHAMED"
    assert rec.dob == "1988-06-17"        # 17061988 → ISO
    assert rec.nationality == "EG"
    assert rec.cb_subject_id == "545529487"


def test_score_extracted():
    rec = parse_aecb_xml(SAMPLE_XML.read_bytes(), "test_run")
    assert rec.credit_score == 300
    assert rec.score_range == "A"
    assert rec.payment_order_flag is True


def test_payment_orders_aggregated():
    rec = parse_aecb_xml(SAMPLE_XML.read_bytes(), "test_run")
    # The sample contains multiple severity-1 bounced cheques
    assert rec.num_bounced_cheques >= 1
    assert rec.total_bounced_cheques_amount > 0


def test_court_cases_parsed():
    rec = parse_aecb_xml(SAMPLE_XML.read_bytes(), "test_run")
    # Sample has several cases, some open (status 55), some closed (status 90)
    assert rec.num_court_cases >= 1
    assert rec.total_claim_amount > 0


def test_income_extracted():
    rec = parse_aecb_xml(SAMPLE_XML.read_bytes(), "test_run")
    assert rec.gross_annual_income is not None
    assert rec.gross_annual_income > 0


def test_report_date_normalised():
    rec = parse_aecb_xml(SAMPLE_XML.read_bytes(), "test_run")
    # Should be ISO YYYY-MM-DD
    assert len(rec.report_date) == 10
    assert rec.report_date.count("-") == 2


def test_invalid_xml_returns_none_gracefully():
    # Empty envelope without NAE_RES — parser should return None, not crash
    empty = b"""<?xml version="1.0"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
<soap:Body></soap:Body></soap:Envelope>"""
    rec = parse_aecb_xml(empty, "test_run")
    assert rec is None
