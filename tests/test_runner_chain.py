import pytest

from tasktiger import Task, Worker
from tasktiger.runner import BaseRunner, DefaultRunner
from tasktiger.runner_chain import ChainedRunner, RunnerContext, validate_runner_chain

from .tasks import (
    BatchTraceRunner,
    ContextReaderRunner,
    ContextWriterRunner,
    EagerChainRunner,
    ExecutorTraceRunner,
    FailingBeforeRunner,
    MyRunnerClass,
    PermanentErrorTraceRunner,
    PermanentErrorTraceRunner2,
    SuppressingRunner,
    TraceAfterRunner,
    TraceBeforeRunner,
    batch_task,
    exception_task,
    simple_task,
)
from .test_base import BaseTestCase


class TestRunnerChain(BaseTestCase):
    def test_single_task_hook_order(self):
        self.tiger.delay(simple_task, runner_class=[TraceBeforeRunner, TraceAfterRunner])
        Worker(self.tiger).run(once=True)
        trace = self.conn.lrange("runner_trace", 0, -1)
        assert trace == [
            "before:TraceBeforeRunner",
            "before:TraceAfterRunner",
            "after:TraceAfterRunner",
            "after:TraceBeforeRunner",
        ]
        self._ensure_queues()

    def test_error_hook_order(self):
        self.tiger.delay(exception_task, runner_class=[TraceBeforeRunner, TraceAfterRunner])
        Worker(self.tiger).run(once=True)
        trace = self.conn.lrange("runner_trace", 0, -1)
        assert trace == [
            "before:TraceBeforeRunner",
            "before:TraceAfterRunner",
            "error:TraceAfterRunner",
            "error:TraceBeforeRunner",
        ]
        self._ensure_queues(error={"default": 1})

    def test_error_suppression(self):
        self.tiger.delay(exception_task, runner_class=[TraceBeforeRunner, SuppressingRunner])
        Worker(self.tiger).run(once=True)
        self._ensure_queues()

    def test_context_sharing(self):
        task = self.tiger.delay(simple_task, runner_class=[ContextWriterRunner, ContextReaderRunner])
        Worker(self.tiger).run(once=True)
        assert self.conn.get("context_writer_was_here") == "True"
        assert self.conn.get("context_task_id") == task.id
        self._ensure_queues()

    def test_permanent_error_propagation(self):
        self.tiger.delay(exception_task, runner_class=[PermanentErrorTraceRunner, PermanentErrorTraceRunner2])
        Worker(self.tiger).run(once=True)
        trace = self.conn.lrange("permanent_error_trace", 0, -1)
        assert trace == ["PermanentErrorTraceRunner", "PermanentErrorTraceRunner2"]
        self._ensure_queues(error={"default": 1})

    def test_batch_task_chain(self):
        self.tiger.delay(batch_task, args=[1], runner_class=[BatchTraceRunner])
        self.tiger.delay(batch_task, args=[2], runner_class=[BatchTraceRunner])
        Worker(self.tiger).run(once=True)
        trace = self.conn.lrange("batch_trace", 0, -1)
        assert any("before:" in e for e in trace)
        assert any("after:" in e for e in trace)
        self._ensure_queues()

    def test_executor_in_chain(self):
        self.tiger.delay(simple_task, runner_class=[TraceBeforeRunner, ExecutorTraceRunner])
        Worker(self.tiger).run(once=True)
        trace = self.conn.lrange("runner_trace", 0, -1)
        assert "before:TraceBeforeRunner" in trace
        assert "after:TraceBeforeRunner" in trace
        self._ensure_queues()

    def test_backward_compatible_single_runner(self):
        task = self.tiger.delay(simple_task, runner_class=MyRunnerClass)
        Worker(self.tiger).run(once=True)
        assert self.conn.get("task_id") == task.id
        self._ensure_queues()

    def test_chain_serializes_through_redis(self):
        task = self.tiger.delay(simple_task, runner_class=[TraceBeforeRunner, TraceAfterRunner])
        raw = self.conn.get("t:task:%s" % task.id)
        import json
        data = json.loads(raw)
        assert isinstance(data["runner_class"], list)
        assert len(data["runner_class"]) == 2
        assert all(":" in p for p in data["runner_class"])
        Worker(self.tiger).run(once=True)
        trace = self.conn.lrange("runner_trace", 0, -1)
        assert trace == [
            "before:TraceBeforeRunner",
            "before:TraceAfterRunner",
            "after:TraceAfterRunner",
            "after:TraceBeforeRunner",
        ]
        self._ensure_queues()

    def test_eager_mode_chain(self):
        self.tiger.config["ALWAYS_EAGER"] = True
        Task(self.tiger, simple_task, runner_class=[EagerChainRunner]).delay()
        assert self.conn.get("eager_chain_ran") == "True"

    def test_default_runner_chain_config(self):
        self.tiger.config["DEFAULT_RUNNER_CHAIN"] = [TraceBeforeRunner]
        self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        trace = self.conn.lrange("runner_trace", 0, -1)
        assert "before:TraceBeforeRunner" in trace
        assert "after:TraceBeforeRunner" in trace
        self._ensure_queues()

    def test_explicit_runner_overrides_default(self):
        self.tiger.config["DEFAULT_RUNNER_CHAIN"] = [TraceBeforeRunner]
        self.tiger.delay(simple_task, runner_class=[TraceAfterRunner])
        Worker(self.tiger).run(once=True)
        trace = self.conn.lrange("runner_trace", 0, -1)
        assert not any("TraceBeforeRunner" in e for e in trace)
        assert any("TraceAfterRunner" in e for e in trace)
        self._ensure_queues()

    def test_validation_rejects_non_runner(self):
        with pytest.raises(TypeError):
            Task(self.tiger, simple_task, runner_class=[str])

    def test_validation_rejects_empty_chain(self):
        with pytest.raises(ValueError):
            Task(self.tiger, simple_task, runner_class=[])

    def test_validation_rejects_duplicates(self):
        with pytest.raises(ValueError):
            Task(self.tiger, simple_task, runner_class=[TraceBeforeRunner, TraceBeforeRunner])

    def test_before_hook_failure_triggers_cleanup(self):
        self.tiger.delay(simple_task, runner_class=[TraceBeforeRunner, FailingBeforeRunner])
        Worker(self.tiger).run(once=True)
        trace = self.conn.lrange("runner_trace", 0, -1)
        assert trace == [
            "before:TraceBeforeRunner",
            "before:FailingBeforeRunner",
            "after:TraceBeforeRunner",
        ]
        self._ensure_queues(error={"default": 1})

    def test_runner_context_dict_interface(self):
        ctx = RunnerContext()
        ctx.set("a", 1)
        assert ctx.get("a") == 1
        assert ctx.get("missing", 42) == 42
        assert "a" in ctx
        assert "missing" not in ctx
        ctx["b"] = 2
        assert ctx["b"] == 2
        del ctx["b"]
        assert "b" not in ctx
        ctx.set("x", 10)
        ctx.set("y", 20)
        assert len(ctx) == 3
        assert bool(ctx) is True
        assert set(ctx.keys()) == {"a", "x", "y"}
        assert set(ctx.values()) == {1, 10, 20}
        assert dict(ctx.items()) == {"a": 1, "x": 10, "y": 20}
        val = ctx.pop("x")
        assert val == 10
        assert "x" not in ctx
        ctx.update({"m": 100})
        assert ctx["m"] == 100
        ctx.setdefault("m", 999)
        assert ctx["m"] == 100
        ctx.setdefault("n", 999)
        assert ctx["n"] == 999
        d = ctx.as_dict()
        assert isinstance(d, dict)
        assert d == {"a": 1, "y": 20, "m": 100, "n": 999}
        ctx.start_timing()
        assert ctx.elapsed >= 0.0
        ctx.record_timing("TestRunner", "before", 0.005)
        assert len(ctx.timings) == 1
        assert ctx.timings[0]["runner_name"] == "TestRunner"
        ctx.clear()
        assert len(ctx) == 0
        assert bool(ctx) is False
        assert repr(ctx).startswith("RunnerContext(")

    def test_chained_runner_properties(self):
        chained = ChainedRunner(self.tiger, runner_classes=[TraceBeforeRunner, TraceAfterRunner])
        assert len(chained.runners) == 2
        assert len(chained.hook_runners) == 2
        assert chained.runner_classes == [TraceBeforeRunner, TraceAfterRunner]
        assert isinstance(chained.executor, DefaultRunner)
