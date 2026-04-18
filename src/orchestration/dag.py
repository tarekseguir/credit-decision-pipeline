"""
Minimal DAG runner. Models the orchestration concepts that would live in
Airflow: tasks, dependencies, run state, retries, and a persistent run history.

Why build this rather than just chaining functions?
- Reviewers can see the orchestration model explicitly (topological sort,
  task states, retries, run logs) without having to spin up Airflow.
- Tasks are testable in isolation.
- Adding a new step is declarative (one dependency edge) rather than editing
  a giant main() function.

In production, these Task objects would map 1:1 to Airflow PythonOperators.
"""
from __future__ import annotations

import json
import sqlite3
import traceback
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Callable, Optional


class TaskState(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"          # upstream failure


@dataclass
class Task:
    """A unit of work in the DAG."""
    task_id: str
    fn: Callable[[dict], None]        # receives a shared context dict
    upstream: list[str] = field(default_factory=list)
    retries: int = 0
    description: str = ""


@dataclass
class TaskRun:
    run_id: str
    task_id: str
    state: TaskState
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    attempt: int = 0
    error: Optional[str] = None

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.started_at and self.finished_at:
            return (self.finished_at - self.started_at).total_seconds()
        return None


class DAG:
    """A directed acyclic graph of tasks."""

    def __init__(self, dag_id: str):
        self.dag_id = dag_id
        self._tasks: dict[str, Task] = {}

    def add(self, task: Task) -> Task:
        if task.task_id in self._tasks:
            raise ValueError(f"Duplicate task_id: {task.task_id}")
        self._tasks[task.task_id] = task
        return task

    def topological_order(self) -> list[Task]:
        """Kahn's algorithm. Raises if a cycle is detected."""
        in_degree: dict[str, int] = defaultdict(int)
        for t in self._tasks.values():
            in_degree[t.task_id]  # ensure key exists
            for up in t.upstream:
                if up not in self._tasks:
                    raise ValueError(f"Task {t.task_id!r} depends on unknown task {up!r}")
                in_degree[t.task_id] += 1

        queue = deque(tid for tid, deg in in_degree.items() if deg == 0)
        ordered: list[Task] = []

        while queue:
            tid = queue.popleft()
            ordered.append(self._tasks[tid])
            for t in self._tasks.values():
                if tid in t.upstream:
                    in_degree[t.task_id] -= 1
                    if in_degree[t.task_id] == 0:
                        queue.append(t.task_id)

        if len(ordered) != len(self._tasks):
            raise ValueError("Cycle detected in DAG")
        return ordered


# --- Run history persistence --------------------------------------------------

RUN_HISTORY_SCHEMA = """
CREATE TABLE IF NOT EXISTS dag_run (
    run_id          TEXT PRIMARY KEY,
    dag_id          TEXT NOT NULL,
    started_at      TEXT NOT NULL,
    finished_at     TEXT,
    state           TEXT NOT NULL,
    context_json    TEXT
);

CREATE TABLE IF NOT EXISTS task_run (
    run_id          TEXT NOT NULL,
    task_id         TEXT NOT NULL,
    attempt         INTEGER NOT NULL,
    state           TEXT NOT NULL,
    started_at      TEXT,
    finished_at     TEXT,
    duration_s      REAL,
    error           TEXT,
    PRIMARY KEY (run_id, task_id, attempt)
);
"""


class RunHistory:
    """Persists DAG and task runs to SQLite for audit + dashboard."""

    def __init__(self, db_path):
        self.db_path = str(db_path)
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(RUN_HISTORY_SCHEMA)

    def start_dag(self, run_id: str, dag_id: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO dag_run (run_id, dag_id, started_at, state) VALUES (?, ?, ?, ?)",
                (run_id, dag_id, datetime.now(timezone.utc).isoformat(), TaskState.RUNNING.value),
            )

    def finish_dag(self, run_id: str, state: TaskState, context: dict) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "UPDATE dag_run SET finished_at = ?, state = ?, context_json = ? WHERE run_id = ?",
                (datetime.now(timezone.utc).isoformat(), state.value,
                 json.dumps(context, default=str), run_id),
            )

    def record_task(self, tr: TaskRun) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """INSERT OR REPLACE INTO task_run
                   (run_id, task_id, attempt, state, started_at, finished_at, duration_s, error)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (tr.run_id, tr.task_id, tr.attempt, tr.state.value,
                 tr.started_at.isoformat() if tr.started_at else None,
                 tr.finished_at.isoformat() if tr.finished_at else None,
                 tr.duration_seconds, tr.error),
            )


# --- Runner -------------------------------------------------------------------

class Runner:
    """Executes a DAG sequentially with retries and persisted history."""

    def __init__(self, dag: DAG, history: RunHistory):
        self.dag = dag
        self.history = history

    def run(self, context: Optional[dict] = None) -> tuple[str, bool]:
        run_id = datetime.now(timezone.utc).strftime("run_%Y%m%d_%H%M%S_") + uuid.uuid4().hex[:6]
        context = context or {}
        context["run_id"] = run_id
        context["dag_id"] = self.dag.dag_id

        print(f"\n▶ DAG {self.dag.dag_id!r} starting — run_id={run_id}")
        self.history.start_dag(run_id, self.dag.dag_id)

        task_states: dict[str, TaskState] = {}
        success = True

        for task in self.dag.topological_order():
            # Skip if any upstream failed
            if any(task_states.get(up) in (TaskState.FAILED, TaskState.SKIPPED)
                   for up in task.upstream):
                print(f"  ⊘ {task.task_id}  SKIPPED (upstream failed)")
                self.history.record_task(TaskRun(
                    run_id=run_id, task_id=task.task_id, attempt=0,
                    state=TaskState.SKIPPED,
                ))
                task_states[task.task_id] = TaskState.SKIPPED
                continue

            task_states[task.task_id] = self._run_task(run_id, task, context)
            if task_states[task.task_id] == TaskState.FAILED:
                success = False

        final_state = TaskState.SUCCESS if success else TaskState.FAILED
        self.history.finish_dag(run_id, final_state, context)
        print(f"\n{'✅' if success else '❌'} DAG {self.dag.dag_id!r} finished: {final_state.value}\n")
        return run_id, success

    def _run_task(self, run_id: str, task: Task, context: dict) -> TaskState:
        total_attempts = task.retries + 1
        for attempt in range(1, total_attempts + 1):
            started = datetime.now(timezone.utc)
            marker = f"[{attempt}/{total_attempts}]" if total_attempts > 1 else ""
            print(f"  ▸ {task.task_id:<30} {marker}", end=" ", flush=True)
            try:
                task.fn(context)
            except Exception as exc:  # noqa: BLE001 — we want to swallow and record
                finished = datetime.now(timezone.utc)
                err = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"
                self.history.record_task(TaskRun(
                    run_id=run_id, task_id=task.task_id, attempt=attempt,
                    state=TaskState.FAILED, started_at=started,
                    finished_at=finished, error=err,
                ))
                if attempt < total_attempts:
                    print(f"❌ {type(exc).__name__} — retrying")
                    continue
                print(f"❌ {type(exc).__name__}: {exc}")
                return TaskState.FAILED

            finished = datetime.now(timezone.utc)
            duration = (finished - started).total_seconds()
            self.history.record_task(TaskRun(
                run_id=run_id, task_id=task.task_id, attempt=attempt,
                state=TaskState.SUCCESS, started_at=started, finished_at=finished,
            ))
            print(f"✅ {duration:.2f}s")
            return TaskState.SUCCESS
