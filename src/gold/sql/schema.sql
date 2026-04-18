-- Serving database schema.
-- Populated by gold layer; consumed by decision engine + dashboard.

-- The feature vector handed to the decision engine. One row per
-- (emirates_id, product, as_of_ts). In production this would be
-- assembled on-demand; for the demo we pre-compute one per customer per product.
CREATE TABLE IF NOT EXISTS decision_input_vector (
    emirates_id              TEXT    NOT NULL,
    product                  TEXT    NOT NULL,
    as_of_ts                 TEXT    NOT NULL,

    -- Profile
    full_name                TEXT    NOT NULL,
    dob                      TEXT    NOT NULL,
    phone                    TEXT    NOT NULL,
    email                    TEXT    NOT NULL,
    emirate                  TEXT    NOT NULL,
    kyc_age_days             INTEGER NOT NULL,
    is_new_user              INTEGER NOT NULL,
    had_overdue_before       INTEGER NOT NULL,

    -- AECB (CRIF NAE response)
    credit_score                INTEGER,
    score_range                 TEXT,
    payment_order_flag          INTEGER NOT NULL DEFAULT 0,
    num_bounced_cheques         INTEGER NOT NULL DEFAULT 0,
    total_bounced_cheques_amount REAL NOT NULL DEFAULT 0,
    num_court_cases             INTEGER NOT NULL DEFAULT 0,
    num_open_court_cases        INTEGER NOT NULL DEFAULT 0,
    total_claim_amount          REAL NOT NULL DEFAULT 0,
    gross_annual_income         REAL,
    aecb_available              INTEGER NOT NULL,

    -- Fraud
    fraud_score              INTEGER,
    fraud_decision           TEXT,
    fraud_reason_codes       TEXT,   -- JSON array
    fraud_available          INTEGER NOT NULL,

    -- AML
    aml_status               TEXT,
    aml_matched_lists        TEXT,   -- JSON array
    aml_available            INTEGER NOT NULL,
    aml_stale_seconds        INTEGER,

    PRIMARY KEY (emirates_id, product, as_of_ts)
);

CREATE INDEX IF NOT EXISTS idx_div_emirates_id ON decision_input_vector(emirates_id);

-- Portfolio fact: one row per credit decision.
CREATE TABLE IF NOT EXISTS fact_decision (
    decision_id          TEXT PRIMARY KEY,
    emirates_id          TEXT NOT NULL,
    product              TEXT NOT NULL,
    decision_ts          TEXT NOT NULL,
    outcome              TEXT NOT NULL,    -- APPROVE | DECLINE | REFER
    reason_codes         TEXT NOT NULL,    -- JSON array
    engine_version       TEXT NOT NULL,
    rule_pack_version    TEXT NOT NULL,
    credit_score         INTEGER,
    fraud_score          INTEGER,
    aml_status           TEXT
);

CREATE INDEX IF NOT EXISTS idx_fact_decision_emirates ON fact_decision(emirates_id);
CREATE INDEX IF NOT EXISTS idx_fact_decision_product  ON fact_decision(product);
CREATE INDEX IF NOT EXISTS idx_fact_decision_ts       ON fact_decision(decision_ts);

-- Dimensions kept intentionally small for the demo.
CREATE TABLE IF NOT EXISTS dim_product (
    product_id   TEXT PRIMARY KEY,
    product_name TEXT NOT NULL,
    description  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_customer (
    emirates_id  TEXT PRIMARY KEY,
    full_name    TEXT NOT NULL,
    emirate      TEXT NOT NULL,
    kyc_verified_at TEXT NOT NULL
);

-- Seed dim_product
INSERT OR IGNORE INTO dim_product VALUES
    ('personal_finance', 'Personal Finance',       'Unsecured personal loan'),
    ('bnpl',             'Buy Now Pay Later',      'Short-term point-of-sale credit'),
    ('credit_card_alt',  'Credit Card Alternative','Revolving credit facility');
