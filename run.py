"""
Pipeline entry point. Builds the DAG and runs it.

Usage:
    python run.py
"""
from __future__ import annotations

import sys

from src.config import ORCHESTRATION_DB
from src.orchestration import Runner, RunHistory
from src.orchestration.registry import build_pipeline_dag


def main() -> int:
    dag = build_pipeline_dag()
    history = RunHistory(ORCHESTRATION_DB)
    runner = Runner(dag, history)
    _, success = runner.run()
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
