"""
The pipeline DAG.

Topology:

  bronze_land
      └─► silver_parse
              ├─► silver_entity_resolution ──► dq_scorecard ──► dq_gate ──┐
              └─► silver_conflict_log                                     │
                                          ┌───────────────────────────────┘
                                          ├─► gold_decision_vector ─► decision_and_snapshot ─► chain_verify
                                          └─► gold_portfolio_mart

The DQ gate is a hard fence: if any MUST_PASS rule breaches, gold tasks
are skipped and the DAG fails. This matches the architecture's "promotion
gate" concept.
"""
from __future__ import annotations

from src.bronze import bronze_land_task
from src.decision.tasks import chain_verify_task, decision_task
from src.dq import dq_gate_task, dq_scorecard_task
from src.gold import gold_vector_task, portfolio_mart_task
from src.orchestration import DAG, Task
from src.silver import silver_conflict_task, silver_er_task, silver_parse_task


def build_pipeline_dag() -> DAG:
    dag = DAG("credit_pipeline")

    dag.add(Task(task_id="bronze_land", fn=bronze_land_task,
                 description="Land raw source files with provenance envelopes."))

    dag.add(Task(task_id="silver_parse", fn=silver_parse_task,
                 upstream=["bronze_land"],
                 description="Parse bronze into normalised silver tables."))

    dag.add(Task(task_id="silver_entity_resolution", fn=silver_er_task,
                 upstream=["silver_parse"],
                 description="Resolve each record to a canonical Emirates ID."))

    dag.add(Task(task_id="silver_conflict_log", fn=silver_conflict_task,
                 upstream=["silver_parse"],
                 description="Log field-level disagreements between sources."))

    dag.add(Task(task_id="dq_scorecard", fn=dq_scorecard_task,
                 upstream=["silver_entity_resolution"],
                 description="Evaluate every DQ rule and persist results."))

    dag.add(Task(task_id="dq_gate", fn=dq_gate_task,
                 upstream=["dq_scorecard"],
                 description="Block gold promotion on MUST_PASS breach."))

    dag.add(Task(task_id="gold_decision_vector", fn=gold_vector_task,
                 upstream=["dq_gate"],
                 description="Assemble the point-in-time decision input vector."))

    dag.add(Task(task_id="gold_portfolio_mart", fn=portfolio_mart_task,
                 upstream=["dq_gate"],
                 description="Populate dim_customer for the portfolio mart."))

    dag.add(Task(task_id="decision_and_snapshot", fn=decision_task,
                 upstream=["gold_decision_vector"],
                 description="Evaluate all vectors and write immutable, chained snapshots."))

    dag.add(Task(task_id="chain_verify", fn=chain_verify_task,
                 upstream=["decision_and_snapshot"],
                 description="Verify the audit chain integrity end-to-end."))

    return dag
