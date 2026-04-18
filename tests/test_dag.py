"""
Tests for the DAG runner itself.

These cover: topological sort, cycle detection, upstream-failure propagation,
and retry semantics.
"""
from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from src.orchestration import DAG, RunHistory, Runner, Task, TaskState


def _history() -> RunHistory:
    return RunHistory(Path(tempfile.mkstemp(suffix=".db")[1]))


def test_linear_topological_order():
    dag = DAG("t")
    dag.add(Task("a", fn=lambda c: None))
    dag.add(Task("b", fn=lambda c: None, upstream=["a"]))
    dag.add(Task("c", fn=lambda c: None, upstream=["b"]))

    order = [t.task_id for t in dag.topological_order()]
    assert order == ["a", "b", "c"]


def test_diamond_topological_order():
    dag = DAG("t")
    dag.add(Task("a", fn=lambda c: None))
    dag.add(Task("b", fn=lambda c: None, upstream=["a"]))
    dag.add(Task("c", fn=lambda c: None, upstream=["a"]))
    dag.add(Task("d", fn=lambda c: None, upstream=["b", "c"]))

    order = [t.task_id for t in dag.topological_order()]
    assert order[0] == "a"
    assert order[-1] == "d"
    assert set(order[1:3]) == {"b", "c"}


def test_cycle_raises():
    dag = DAG("t")
    dag.add(Task("a", fn=lambda c: None, upstream=["b"]))
    dag.add(Task("b", fn=lambda c: None, upstream=["a"]))

    with pytest.raises(ValueError, match="[Cc]ycle"):
        dag.topological_order()


def test_unknown_dependency_raises():
    dag = DAG("t")
    dag.add(Task("a", fn=lambda c: None, upstream=["ghost"]))

    with pytest.raises(ValueError, match="unknown"):
        dag.topological_order()


def test_upstream_failure_skips_downstream():
    called = {"a": 0, "b": 0, "c": 0}

    def fail(c):
        raise RuntimeError("boom")

    dag = DAG("t")
    dag.add(Task("a", fn=lambda c: called.update({"a": called["a"] + 1})))
    dag.add(Task("b", fn=fail, upstream=["a"]))
    dag.add(Task("c", fn=lambda c: called.update({"c": called["c"] + 1}),
                 upstream=["b"]))

    _, success = Runner(dag, _history()).run()
    assert not success
    assert called == {"a": 1, "b": 0, "c": 0}  # c skipped because b failed


def test_retries_are_attempted():
    attempts = {"n": 0}

    def flaky(c):
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise RuntimeError("not yet")

    dag = DAG("t")
    dag.add(Task("a", fn=flaky, retries=3))

    _, success = Runner(dag, _history()).run()
    assert success
    assert attempts["n"] == 3
