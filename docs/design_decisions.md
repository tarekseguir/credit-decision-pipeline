# Design Decisions

## 1. Custom DAG runner instead of Airflow

**Choice:** A ~200-line DAG runner in `src/orchestration/dag.py`.

**Why:** Airflow is the right answer in production, but installing it makes a demo painful to run. A plain script loses the orchestration story entirely. The middle path demonstrates the concepts explicitly — topological sort, task state, retries, upstream-failure propagation, persisted run history — in code a reviewer can read in one sitting. Every `Task.fn` maps 1:1 to an Airflow `PythonOperator` with zero rewrite.

## 2. Emirates ID as the universal join key

**Choice:** Emirates ID is treated as present on every source. Phone+email and name+DOB are fallback tiers, not primary paths.

**Why:** In the UAE every customer has an Emirates ID captured at KYC, and bureaus/fraud/AML all accept it as a reference identifier on outbound calls. Using it as the primary key collapses the identity problem from "resolve across 4 incompatible keys" to "resolve a small number of edge cases" (pre-KYC pre-checks, delayed async callbacks).

## 3. Fraud and AML cannot mint new canonical customers

**Choice:** Only `PROFILE` and `AECB` sources are allowed to introduce a previously-unseen canonical Emirates ID. Fraud and AML must attach to an existing customer or be quarantined.

**Why:** A noisy fraud API or a webhook from a stranger shouldn't create a customer record. The internal profile is the source of truth for customer identity; AECB is authoritative as a government-linked source. This rule is tested.

## 4. Content-addressed feature vectors

**Choice:** Each feature vector is stored at `feature_vectors/<sha256[:2]>/<sha256>.json` rather than as a payload embedded in the snapshot.

**Why:**
- Two identical feature vectors deduplicate naturally on disk.
- The 256-bucket prefix prevents a single directory from holding millions of files.
- Any single-byte change is detectable by re-hashing; makes tamper detection trivially cheap.

## 5. Per-product hash chain, not one global chain

**Choice:** Each product has its own independent snapshot chain.

**Why:** A global chain would force decisions to serialise across products, creating a contention bottleneck at scale. Per-product chains parallelise naturally and an audit failure in one product doesn't invalidate the others. The downside — no single linear log — is acceptable because `decision_id` is globally unique and each chain is independently verifiable.

## 6. SQLite for serving

**Choice:** SQLite for the serving and audit databases.

**Why:** The demo needs to run with `make all` on a fresh laptop in under a minute. Postgres would add a Docker dependency without changing any of the SQL shown here. The SQL schema (`gold/sql/schema.sql`) is Postgres-compatible; the migration is a dependency change, not a logic change.

## 7. NDJSON in silver rather than Parquet

**Choice:** Silver tables persist as newline-delimited JSON.

**Why:** NDJSON is human-readable and diffable in git, which matters for a demo where a reviewer may want to `cat` a file. The schema is identical to what Parquet would store; swapping is a one-line change in `silver/_io.py`.

## 8. DQ framework with declarative `Rule` subclasses

**Choice:** Each rule is a class subclassing `Rule`, with a `check()` method that returns a `DQResult`.

**Why:** Flat functions would work for 8 rules but would make adding rule 50 painful. A base class gives every rule consistent result structure, threshold handling, and severity semantics. New rules are ~20 lines and automatically picked up by the runner when registered in `RULES`.

## 9. DQ gate as a first-class DAG task, not a side effect

**Choice:** The DQ gate is its own task (`dq_gate`) that the gold tasks depend on. If the gate fails, gold tasks don't run.

**Why:** Makes the promotion rule explicit in the DAG topology. A reviewer can see at a glance that silver→gold is gated. A plain assertion inside the scorecard task would work but would hide the promotion contract.

## 10. Messy data injected on purpose

**Choice:** The generator ships with 9 specific kinds of messiness (missing EIDs, phone/email conflicts, name typos, missing AML callbacks, duplicate files, etc.).

**Why:** A demo with clean data proves nothing. Each injected scenario exercises a specific pipeline path — entity resolution fallbacks, conflict log, DQ warnings, idempotent bronze. The generator prints counts so a reviewer can verify the pipeline handled the exact cases it was supposed to.

## 11. Snapshot immutability via filesystem permissions

**Choice:** After writing, snapshots and feature vectors are `chmod -w`.

**Why:** In production this is S3 Object Lock. Locally, chmod is the closest equivalent. It doesn't stop a determined attacker — nothing local does — but it prevents accidental overwrites and mirrors the production pattern.

## 12. Rule pack versioning

**Choice:** Each rule pack has a hardcoded `version = "pf_v1.0.0"` string. The version is captured in every snapshot.

**Why:** The audit story requires answering "what logic produced this decision?". Without a version string, a code change silently invalidates every past snapshot's replayability. Bumping the version on every change is a developer-discipline requirement backed by a snapshot field.

## 13. Outcome = {APPROVE, DECLINE, REFER}, not just {APPROVE, DECLINE}

**Choice:** Three outcomes, not two.

**Why:** Real credit decisioning always has a refer-to-human path for ambiguous cases (thin file, fraud review flag, pending AML). Collapsing to two outcomes forces the engine to auto-decline cases that should go to manual review, which is a policy failure. The cost is one extra branch per rule pack.

## 14. AML pending doesn't block decisions

**Choice:** If no AML callback has arrived, the rule pack can still emit REFER (instead of waiting).

**Why:** AML is asynchronous and can take hours. Blocking every decision on AML would tank conversion. The snapshot records `aml_available=False`, so the auditor can see the decision was made without AML data and was routed to manual review.

## 15. No ML in the demo

**Choice:** Rule-based decisioning only.

**Why:** The brief asks about data pipelines, not credit models. A rules engine with three versioned packs demonstrates the decision-versioning and audit requirements without inventing a model whose performance isn't the point.
