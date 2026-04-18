"""
Credit Pipeline Demo — Streamlit dashboard.

Three pages:
  1. Portfolio overview       — approval rates, reason distribution, product mix
  2. Data quality             — latest scorecard + rule history
  3. Decision lookup / audit  — pick a decision, see the full snapshot + replay

Run:  streamlit run dashboard/app.py
"""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

# Allow dashboard/ to import src/ when run with `streamlit run`
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pandas as pd
import streamlit as st

from src.audit import replay_decision, verify_chain
from src.config import AUDIT_DB, AUDIT_DIR, SERVING_DB

st.set_page_config(page_title="Credit Pipeline Demo", layout="wide", page_icon="💳")


# ---------- Shared helpers ----------------------------------------------------

def _check_dbs() -> bool:
    missing = [p for p in (SERVING_DB, AUDIT_DB) if not p.exists()]
    if missing:
        st.error("Pipeline has not been run yet. Run `make generate && make run` first.")
        st.stop()
        return False
    return True


@st.cache_data(ttl=5)
def _load_decisions() -> pd.DataFrame:
    with sqlite3.connect(SERVING_DB) as conn:
        return pd.read_sql_query("SELECT * FROM fact_decision", conn)


@st.cache_data(ttl=5)
def _load_dq_latest() -> pd.DataFrame:
    with sqlite3.connect(SERVING_DB) as conn:
        latest_run = conn.execute(
            "SELECT run_id FROM dq_result ORDER BY evaluated_at DESC LIMIT 1"
        ).fetchone()
        if not latest_run:
            return pd.DataFrame()
        return pd.read_sql_query(
            "SELECT * FROM dq_result WHERE run_id = ? ORDER BY severity, rule_id",
            conn, params=(latest_run[0],),
        )


@st.cache_data(ttl=5)
def _load_snapshots() -> pd.DataFrame:
    with sqlite3.connect(AUDIT_DB) as conn:
        return pd.read_sql_query("SELECT * FROM snapshot ORDER BY decision_ts DESC", conn)


# ---------- Sidebar nav -------------------------------------------------------

_check_dbs()

st.sidebar.title("💳 Credit Pipeline")
st.sidebar.caption("Demo dashboard")
page = st.sidebar.radio(
    "Navigate",
    ["Portfolio overview", "Data quality", "Decision lookup & audit"],
)
st.sidebar.divider()
st.sidebar.caption("Data refreshes from `serving.db` and `audit.db` every 5s.")


# ---------- Page 1: Portfolio -------------------------------------------------

if page == "Portfolio overview":
    st.title("Portfolio overview")

    decisions = _load_decisions()
    if decisions.empty:
        st.warning("No decisions in fact_decision yet.")
        st.stop()

    decisions["reason_codes"] = decisions["reason_codes"].apply(json.loads)

    total = len(decisions)
    approved = (decisions["outcome"] == "APPROVE").sum()
    declined = (decisions["outcome"] == "DECLINE").sum()
    referred = (decisions["outcome"] == "REFER").sum()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total decisions", f"{total:,}")
    c2.metric("Approved", f"{approved:,}", f"{approved / total:.1%}")
    c3.metric("Declined", f"{declined:,}", f"{declined / total:.1%}")
    c4.metric("Referred", f"{referred:,}", f"{referred / total:.1%}")

    st.divider()

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Outcome by product")
        pivot = (decisions
                 .groupby(["product", "outcome"]).size()
                 .unstack(fill_value=0))
        st.bar_chart(pivot)

    with col2:
        st.subheader("Top decline reasons")
        exploded = (decisions[decisions["outcome"] == "DECLINE"]
                    .explode("reason_codes"))
        if not exploded.empty:
            top = (exploded["reason_codes"]
                   .value_counts()
                   .head(10)
                   .rename_axis("reason")
                   .reset_index(name="count"))
            st.bar_chart(top.set_index("reason"))
        else:
            st.info("No declines.")

    st.divider()
    st.subheader("Approval rate by product")
    rates = (decisions
             .assign(approved=(decisions["outcome"] == "APPROVE").astype(int))
             .groupby("product")
             .agg(decisions=("decision_id", "count"),
                  approved=("approved", "sum")))
    rates["approval_rate"] = (rates["approved"] / rates["decisions"] * 100).round(1)
    st.dataframe(rates, use_container_width=True)

    st.divider()
    st.subheader("Credit score distribution (approved vs declined)")
    scored = decisions[decisions["credit_score"].notna()]
    if not scored.empty:
        chart_df = (scored[["outcome", "credit_score"]]
                    .pivot(columns="outcome", values="credit_score"))
        st.bar_chart(chart_df)


# ---------- Page 2: Data quality ---------------------------------------------

elif page == "Data quality":
    st.title("Data quality scorecard")
    st.caption("Latest run. MUST_PASS breaches block promotion to gold.")

    dq = _load_dq_latest()
    if dq.empty:
        st.warning("No DQ results yet — run the pipeline first.")
        st.stop()

    must_pass_fails = ((dq["severity"] == "MUST_PASS") & (dq["breached"] == 1)).sum()
    warn_fails = ((dq["severity"] == "WARN") & (dq["breached"] == 1)).sum()

    c1, c2, c3 = st.columns(3)
    c1.metric("Rules evaluated", len(dq))
    c2.metric("Must-pass failures", int(must_pass_fails),
              delta="BLOCKED" if must_pass_fails else "clean",
              delta_color="inverse")
    c3.metric("Warnings", int(warn_fails))

    if must_pass_fails:
        st.error(f"⛔ Gold promotion blocked — {must_pass_fails} must-pass rule(s) breached.")
    else:
        st.success("✅ All must-pass rules clean. Gold promotion allowed.")

    st.divider()
    st.subheader("Rule results")

    def _status(row):
        if not row["breached"]:
            return "✅ pass"
        return "❌ fail" if row["severity"] == "MUST_PASS" else "⚠️ warn"

    display = dq.copy()
    display["status"] = display.apply(_status, axis=1)
    display["fail_ratio"] = (display["fail_ratio"] * 100).round(2).astype(str) + "%"
    display["threshold"] = (display["threshold"] * 100).round(2).astype(str) + "%"
    st.dataframe(
        display[["rule_id", "severity", "source", "description",
                 "checked", "failed", "fail_ratio", "threshold", "status"]],
        use_container_width=True, hide_index=True,
    )

    st.divider()
    with st.expander("Sample failures"):
        for _, row in dq[dq["breached"] == 1].iterrows():
            st.write(f"**{row['rule_id']}** ({row['severity']})")
            st.code(row["sample_failures"], language="json")


# ---------- Page 3: Decision lookup & audit ----------------------------------

elif page == "Decision lookup & audit":
    st.title("Decision lookup & audit")
    st.caption("Pick any decision to see the full snapshot, the feature vector, and replay it.")

    snapshots = _load_snapshots()
    if snapshots.empty:
        st.warning("No snapshots yet.")
        st.stop()

    c1, c2 = st.columns([1, 3])
    with c1:
        product_filter = st.selectbox(
            "Product", ["all"] + sorted(snapshots["product"].unique().tolist())
        )
        outcome_filter = st.selectbox(
            "Outcome", ["all", "APPROVE", "DECLINE", "REFER"]
        )

    filtered = snapshots.copy()
    if product_filter != "all":
        filtered = filtered[filtered["product"] == product_filter]
    if outcome_filter != "all":
        filtered = filtered[filtered["outcome"] == outcome_filter]

    with c2:
        selected = st.selectbox(
            f"Decision ID ({len(filtered)} match)",
            filtered["decision_id"].tolist()[:500],   # cap for UI
        )

    if not selected:
        st.stop()

    row = snapshots[snapshots["decision_id"] == selected].iloc[0]

    st.divider()
    st.subheader(f"Decision {selected}")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Outcome", row["outcome"])
    c2.metric("Product", row["product"])
    c3.metric("Engine", row["engine_version"])
    c4.metric("Rule pack", row["rule_pack_version"])

    # Load snapshot + vector
    snap_path = AUDIT_DIR.parent / row["snapshot_path"]
    snap_body = json.loads(snap_path.read_text())
    fv_path = AUDIT_DIR.parent / snap_body["feature_vector_path"]
    fv_body = json.loads(fv_path.read_text())

    tab1, tab2, tab3, tab4 = st.tabs([
        "Snapshot", "Feature vector", "Chain pointers", "Replay",
    ])

    with tab1:
        st.json(snap_body)
    with tab2:
        st.json(fv_body)
    with tab3:
        st.write("**This snapshot hash**")
        st.code(snap_body["this_snapshot_sha256"])
        st.write("**Previous snapshot hash (in chain for this product)**")
        st.code(snap_body["prev_snapshot_sha256"])
        st.write("**Feature vector hash**")
        st.code(snap_body["feature_vector_sha256"])
    with tab4:
        if st.button("Replay this decision"):
            result = replay_decision(selected)
            if result.matches:
                st.success(f"✅ Replay matches: {result.replayed_outcome}")
            else:
                st.error(
                    f"❌ Replay mismatch — original={result.original_outcome} "
                    f"replayed={result.replayed_outcome}"
                )
            st.write("**Original reason codes**", result.original_reasons)
            st.write("**Replayed reason codes**", result.replayed_reasons)

    st.divider()
    st.subheader("Chain integrity check")
    if st.button(f"Verify full chain for product={row['product']}"):
        issues = verify_chain(row["product"])
        if not issues:
            st.success(f"✅ Chain intact across all snapshots for {row['product']}.")
        else:
            st.error(f"❌ {len(issues)} integrity issue(s) detected.")
            st.dataframe(pd.DataFrame([i.__dict__ for i in issues]))
