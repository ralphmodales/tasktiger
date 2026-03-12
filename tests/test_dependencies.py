import datetime
import time
from multiprocessing import Pool

import redis

from tasktiger import Task, Worker
from tasktiger.retry import fixed

from .config import DELAY, REDIS_HOST, TEST_DB
from .tasks import exception_task, retry_task_3
from .utils import external_worker


QUEUED = "queued"
SCHEDULED = "scheduled"
ERROR = "error"
WAITING = "waiting"


def _get_state_count(tiger, queue, state):
    stats = tiger.get_queue_stats().get(queue, {})
    return int(stats.get(state, 0))


def record_order(value, key="task_order"):
    with redis.Redis(host=REDIS_HOST, db=TEST_DB, decode_responses=True) as conn:
        conn.rpush(key, value)


def record_order_then_fail(value, key="task_order"):
    with redis.Redis(host=REDIS_HOST, db=TEST_DB, decode_responses=True) as conn:
        conn.rpush(key, value)
    raise Exception("fail")


def sleep_then_record(value, delay=DELAY, key="task_order"):
    time.sleep(delay)
    record_order(value, key=key)


class TestDependencies:
    def test_no_deps_queued_immediately(self, tiger, redis):
        redis.delete("task_order")

        tiger.delay(record_order, args=("task",), kwargs={"key": "task_order"}, depends_on=[])

        assert _get_state_count(tiger, "default", WAITING) == 0
        assert _get_state_count(tiger, "default", QUEUED) == 1

        Worker(tiger).run(once=True, force_once=True)

        assert redis.lrange("task_order", 0, -1) == ["task"]

    def test_task_with_one_pending_dep_enters_waiting(self, tiger, redis):
        redis.delete("task_order")

        dep = tiger.delay(record_order, args=("dep",), kwargs={"key": "task_order"})
        tiger.delay(record_order, args=("task",), kwargs={"key": "task_order"}, depends_on=[dep])

        assert _get_state_count(tiger, "default", WAITING) == 1

        Worker(tiger).run(once=True, force_once=True)

        assert redis.lrange("task_order", 0, -1) == ["dep"]

        Worker(tiger).run(once=True, force_once=True)

        assert redis.lrange("task_order", 0, -1) == ["dep", "task"]

    def test_task_with_multiple_deps_waits_for_all(self, tiger, redis):
        redis.delete("task_order")

        dep1 = tiger.delay(record_order, args=("dep1",), kwargs={"key": "task_order"})
        dep2 = tiger.delay(record_order, args=("dep2",), kwargs={"key": "task_order"})
        tiger.delay(record_order, args=("task",), kwargs={"key": "task_order"}, depends_on=[dep1, dep2])

        assert _get_state_count(tiger, "default", WAITING) == 1

        Worker(tiger).run(once=True, force_once=True)
        assert set(redis.lrange("task_order", 0, -1)) <= {"dep1", "dep2"}
        assert "task" not in redis.lrange("task_order", 0, -1)
        assert _get_state_count(tiger, "default", WAITING) == 1

        Worker(tiger).run(once=True, force_once=True)
        assert set(redis.lrange("task_order", 0, -1)) == {"dep1", "dep2"}
        assert _get_state_count(tiger, "default", WAITING) == 0
        assert _get_state_count(tiger, "default", QUEUED) == 1

        Worker(tiger).run(once=True, force_once=True)
        assert redis.lrange("task_order", 0, -1)[-1] == "task"

    def test_dependency_success_unblocks_dependent(self, tiger, redis):
        dep = tiger.delay(record_order, args=("dep",), kwargs={"key": "task_order"})
        tiger.delay(record_order, args=("task",), kwargs={"key": "task_order"}, depends_on=[dep])

        assert _get_state_count(tiger, "default", WAITING) == 1

        Worker(tiger).run(once=True, force_once=True)

        assert _get_state_count(tiger, "default", WAITING) == 0
        assert _get_state_count(tiger, "default", QUEUED) == 1

    def test_dependency_permanent_failure_propagates(self, tiger, redis):
        redis.delete("task_order")

        dep = tiger.delay(record_order_then_fail, args=("dep",), kwargs={"key": "task_order"})
        tiger.delay(record_order, args=("task",), kwargs={"key": "task_order"}, depends_on=[dep])

        Worker(tiger).run(once=True, force_once=True)

        assert redis.lrange("task_order", 0, -1) == ["dep"]
        assert _get_state_count(tiger, "default", ERROR) == 2

    def test_dependency_permanent_failure_propagates_transitively(self, tiger, redis):
        redis.delete("task_order")

        a = tiger.delay(record_order_then_fail, args=("A",), kwargs={"key": "task_order"})
        b = tiger.delay(record_order, args=("B",), kwargs={"key": "task_order"}, depends_on=[a])
        tiger.delay(record_order, args=("C",), kwargs={"key": "task_order"}, depends_on=[b])

        Worker(tiger).run(once=True, force_once=True)

        assert redis.lrange("task_order", 0, -1) == ["A"]
        assert _get_state_count(tiger, "default", ERROR) == 3

    def test_dependency_retry_does_not_fail_dependent(self, tiger, redis):
        redis.delete("task_order")

        dep = tiger.delay(
            record_order_then_fail,
            args=("dep",),
            kwargs={"key": "task_order"},
            retry_method=fixed(DELAY, 2),
        )
        tiger.delay(record_order, args=("task",), kwargs={"key": "task_order"}, depends_on=[dep])

        Worker(tiger).run(once=True, force_once=True)

        assert redis.lrange("task_order", 0, -1) == ["dep"]
        assert _get_state_count(tiger, "default", SCHEDULED) == 1
        assert _get_state_count(tiger, "default", WAITING) == 1
        assert _get_state_count(tiger, "default", ERROR) == 0

    def test_dependency_retryexception_log_error_false_propagates_failure(self, tiger, redis):
        redis.delete("task_order")

        dep = tiger.delay(retry_task_3)
        tiger.delay(record_order, args=("task",), kwargs={"key": "task_order"}, depends_on=[dep])

        assert _get_state_count(tiger, "default", WAITING) == 1

        Worker(tiger).run(once=True)
        assert _get_state_count(tiger, "default", WAITING) == 1

        time.sleep(DELAY)

        Worker(tiger).run(once=True)
        Worker(tiger).run(once=True)

        assert _get_state_count(tiger, "default", WAITING) == 0
        assert _get_state_count(tiger, "default", QUEUED) == 0
        assert _get_state_count(tiger, "default", ERROR) == 1
        assert redis.lrange("task_order", 0, -1) == []

    def test_dependency_already_failed_marks_dependent_error_immediately(self, tiger, redis):
        redis.delete("task_order")

        dep = tiger.delay(record_order_then_fail, args=("dep",), kwargs={"key": "task_order"})
        Worker(tiger).run(once=True, force_once=True)

        assert _get_state_count(tiger, "default", ERROR) == 1

        tiger.delay(record_order, args=("task",), kwargs={"key": "task_order"}, depends_on=[dep])

        assert _get_state_count(tiger, "default", WAITING) == 0
        assert _get_state_count(tiger, "default", ERROR) == 2

    def test_end_to_end_external_worker_executes_in_order(self, tiger, redis):
        redis.delete("task_order")

        dep = tiger.delay(record_order, args=("A",), kwargs={"key": "task_order"})
        tiger.delay(record_order, args=("B",), kwargs={"key": "task_order"}, depends_on=[dep])

        Pool(1).map(external_worker, [None])
        time.sleep(DELAY)
        Pool(1).map(external_worker, [None])

        assert redis.lrange("task_order", 0, -1) == ["A", "B"]

    def test_concurrent_workers_do_not_double_unblock_dependents(self, tiger, redis):
        redis.delete("task_order")

        dep = tiger.delay(record_order, args=("A",), kwargs={"key": "task_order"}, queue="other")
        tiger.delay(record_order, args=("B",), kwargs={"key": "task_order"}, depends_on=[dep])

        assert _get_state_count(tiger, "default", WAITING) == 1

        Pool(4).map(
            external_worker,
            [
                None,
                None,
                None,
                None,
            ],
        )

        time.sleep(DELAY)

        Pool(4).map(
            external_worker,
            [
                None,
                None,
                None,
                None,
            ],
        )

        assert redis.lrange("task_order", 0, -1) == ["A", "B"]

    def test_cross_queue_dependency_unblocks_correct_queue(self, tiger, redis):
        redis.delete("task_order")

        dep = tiger.delay(
            record_order,
            args=("A",),
            kwargs={"key": "task_order"},
            queue="other",
        )
        tiger.delay(record_order, args=("B",), kwargs={"key": "task_order"}, depends_on=[dep])

        assert _get_state_count(tiger, "default", WAITING) == 1

        Worker(tiger, queues=["other"]).run(once=True, force_once=True)

        assert redis.lrange("task_order", 0, -1) == ["A"]
        assert _get_state_count(tiger, "default", WAITING) == 0
        assert _get_state_count(tiger, "default", QUEUED) == 1

        Worker(tiger, queues=["default"]).run(once=True, force_once=True)

        assert redis.lrange("task_order", 0, -1) == ["A", "B"]

    def test_scheduled_dependent_stays_scheduled_until_when(self, tiger, redis):
        redis.delete("task_order")

        dep = tiger.delay(
            record_order,
            args=("A",),
            kwargs={"key": "task_order"},
            queue="other",
        )
        tiger.delay(
            record_order,
            args=("B",),
            kwargs={"key": "task_order"},
            depends_on=[dep],
            when=datetime.timedelta(seconds=DELAY * 2),
        )

        Worker(tiger, queues=["other"]).run(once=True, force_once=True)

        assert redis.lrange("task_order", 0, -1) == ["A"]
        assert _get_state_count(tiger, "default", WAITING) == 0
        assert _get_state_count(tiger, "default", QUEUED) == 0
        assert _get_state_count(tiger, "default", SCHEDULED) == 1

        time.sleep(DELAY * 2 + DELAY / 2)

        Worker(tiger, queues=["default"]).run(once=True, force_once=True)
        Worker(tiger, queues=["default"]).run(once=True, force_once=True)

        assert redis.lrange("task_order", 0, -1) == ["A", "B"]

    def test_dependent_scheduled_time_passed_queues_immediately_on_unblock(self, tiger, redis):
        redis.delete("task_order")

        dep = tiger.delay(
            sleep_then_record,
            args=("A",),
            kwargs={"key": "task_order", "delay": DELAY},
            queue="other",
        )
        tiger.delay(
            record_order,
            args=("B",),
            kwargs={"key": "task_order"},
            depends_on=[dep],
            when=datetime.timedelta(seconds=DELAY / 4),
        )

        Worker(tiger, queues=["other"]).run(once=True, force_once=True)

        assert redis.lrange("task_order", 0, -1) == ["A"]
        assert _get_state_count(tiger, "default", SCHEDULED) == 0
        assert _get_state_count(tiger, "default", QUEUED) == 1

        Worker(tiger, queues=["default"]).run(once=True, force_once=True)

        assert redis.lrange("task_order", 0, -1) == ["A", "B"]

    def test_diamond_dependency_fan_in(self, tiger, redis):
        redis.delete("task_order")

        b = tiger.delay(record_order, args=("B",), kwargs={"key": "task_order"})
        c = tiger.delay(record_order, args=("C",), kwargs={"key": "task_order"})
        tiger.delay(
            record_order,
            args=("D",),
            kwargs={"key": "task_order"},
            depends_on=[b, c],
        )

        assert _get_state_count(tiger, "default", QUEUED) == 2
        assert _get_state_count(tiger, "default", WAITING) == 1

        Worker(tiger).run(once=True, force_once=True)
        executed = redis.lrange("task_order", 0, -1)
        assert set(executed) <= {"B", "C"}
        assert "D" not in executed

        Worker(tiger).run(once=True, force_once=True)
        assert set(redis.lrange("task_order", 0, -1)) == {"B", "C"}
        assert _get_state_count(tiger, "default", QUEUED) == 1

        Worker(tiger).run(once=True, force_once=True)
        assert redis.lrange("task_order", 0, -1)[-1] == "D"

    def test_group_executes_completion_after_all_members(self, tiger, redis):
        redis.delete("task_order")

        t1 = Task(tiger, record_order, args=["X"], kwargs={"key": "task_order"})
        t2 = Task(tiger, record_order, args=["Y"], kwargs={"key": "task_order"})
        t3 = Task(tiger, record_order, args=["Z"], kwargs={"key": "task_order"})
        comp = Task(tiger, record_order, args=["DONE"], kwargs={"key": "task_order"})

        result = tiger.group([t1, t2, t3], completion_task=comp)
        assert len(result) == 4

        assert _get_state_count(tiger, "default", QUEUED) == 3
        assert _get_state_count(tiger, "default", WAITING) == 1

        Worker(tiger).run(once=True, force_once=True)
        Worker(tiger).run(once=True, force_once=True)
        Worker(tiger).run(once=True, force_once=True)

        executed = redis.lrange("task_order", 0, -1)
        assert set(executed[:3]) == {"X", "Y", "Z"}
        assert _get_state_count(tiger, "default", QUEUED) == 1

        Worker(tiger).run(once=True, force_once=True)
        assert redis.lrange("task_order", 0, -1)[-1] == "DONE"

    def test_group_member_failure_propagates_to_completion(self, tiger, redis):
        redis.delete("task_order")

        t1 = Task(tiger, record_order, args=["X"], kwargs={"key": "task_order"})
        t2 = Task(
            tiger, record_order_then_fail, args=["FAIL"], kwargs={"key": "task_order"}
        )
        comp = Task(tiger, record_order, args=["DONE"], kwargs={"key": "task_order"})

        tiger.group([t1, t2], completion_task=comp)

        Worker(tiger).run(once=True, force_once=True)
        Worker(tiger).run(once=True, force_once=True)

        assert _get_state_count(tiger, "default", ERROR) == 2
        assert "DONE" not in redis.lrange("task_order", 0, -1)

    def test_retry_dep_failed_task_rechecks_dependencies(self, tiger, redis):
        redis.delete("task_order")

        a = tiger.delay(
            record_order_then_fail, args=("A",), kwargs={"key": "task_order"}
        )
        b = tiger.delay(
            record_order, args=("B",), kwargs={"key": "task_order"}, depends_on=[a]
        )

        Worker(tiger).run(once=True, force_once=True)
        assert _get_state_count(tiger, "default", ERROR) == 2

        a_task = Task.from_id(tiger, "default", ERROR, a.id)
        a_task.retry()
        assert _get_state_count(tiger, "default", QUEUED) == 1

        b_task = Task.from_id(tiger, "default", ERROR, b.id)
        b_task.retry()

        assert _get_state_count(tiger, "default", WAITING) == 1
        assert _get_state_count(tiger, "default", ERROR) == 0

    def test_retry_dep_failed_task_when_dep_still_errored(self, tiger, redis):
        redis.delete("task_order")

        a = tiger.delay(
            record_order_then_fail, args=("A",), kwargs={"key": "task_order"}
        )
        b = tiger.delay(
            record_order, args=("B",), kwargs={"key": "task_order"}, depends_on=[a]
        )

        Worker(tiger).run(once=True, force_once=True)
        assert _get_state_count(tiger, "default", ERROR) == 2

        b_task = Task.from_id(tiger, "default", ERROR, b.id)
        b_task.retry()

        assert _get_state_count(tiger, "default", ERROR) == 2

    def test_cancel_waiting_cascades_to_dependents(self, tiger, redis):
        redis.delete("task_order")

        a = tiger.delay(record_order, args=("A",), kwargs={"key": "task_order"})
        b = tiger.delay(
            record_order, args=("B",), kwargs={"key": "task_order"}, depends_on=[a]
        )
        tiger.delay(
            record_order, args=("C",), kwargs={"key": "task_order"}, depends_on=[b]
        )

        assert _get_state_count(tiger, "default", WAITING) == 2

        b_task = Task.from_id(tiger, "default", WAITING, b.id)
        b_task.cancel_waiting()

        assert _get_state_count(tiger, "default", WAITING) == 0
        assert _get_state_count(tiger, "default", ERROR) == 1

    def test_waiting_task_timeout_moves_to_error(self, tiger, redis):
        redis.delete("task_order")
        tiger.config["WAITING_TASK_TIMEOUT"] = 0.1

        dep = tiger.delay(
            record_order,
            args=("A",),
            kwargs={"key": "task_order"},
            queue="stuck",
        )
        tiger.delay(
            record_order,
            args=("B",),
            kwargs={"key": "task_order"},
            depends_on=[dep],
        )

        assert _get_state_count(tiger, "default", WAITING) == 1

        time.sleep(0.3)

        Worker(tiger, queues=["default"]).run(once=True, force_once=True)

        assert _get_state_count(tiger, "default", WAITING) == 0
        assert _get_state_count(tiger, "default", ERROR) == 1

        tiger.config["WAITING_TASK_TIMEOUT"] = None

    def test_deep_chain_end_to_end(self, tiger, redis):
        redis.delete("task_order")

        tasks = tiger.chain([
            Task(tiger, record_order, args=["A"], kwargs={"key": "task_order"}),
            Task(tiger, record_order, args=["B"], kwargs={"key": "task_order"}),
            Task(tiger, record_order, args=["C"], kwargs={"key": "task_order"}),
            Task(tiger, record_order, args=["D"], kwargs={"key": "task_order"}),
            Task(tiger, record_order, args=["E"], kwargs={"key": "task_order"}),
        ])

        assert len(tasks) == 5
        assert _get_state_count(tiger, "default", QUEUED) == 1
        assert _get_state_count(tiger, "default", WAITING) == 4

        for _ in range(5):
            Worker(tiger).run(once=True, force_once=True)

        assert redis.lrange("task_order", 0, -1) == ["A", "B", "C", "D", "E"]

    def test_purge_errored_tasks_cleans_dependency_metadata(self, tiger, redis):
        redis.delete("task_order")

        a = tiger.delay(
            record_order_then_fail, args=("A",), kwargs={"key": "task_order"}
        )
        b = tiger.delay(
            record_order, args=("B",), kwargs={"key": "task_order"}, depends_on=[a]
        )

        Worker(tiger).run(once=True, force_once=True)
        assert _get_state_count(tiger, "default", ERROR) == 2

        purged = tiger.purge_errored_tasks()
        assert purged == 2
        assert _get_state_count(tiger, "default", ERROR) == 0

    def test_partial_purge_prerequisite_removes_from_surviving_dependent_metadata(
        self, tiger, redis
    ):
        redis.delete("task_order")

        dep = tiger.delay(
            record_order_then_fail,
            args=("A",),
            kwargs={"key": "task_order"},
            queue="a",
        )
        other = tiger.delay(record_order, args=("C",), kwargs={"key": "task_order"}, queue="c")
        dependent = tiger.delay(
            record_order,
            args=("B",),
            kwargs={"key": "task_order"},
            depends_on=[dep, other],
            queue="default",
        )

        Worker(tiger, queues=["a"]).run(once=True, force_once=True)

        assert _get_state_count(tiger, "a", ERROR) == 1
        assert _get_state_count(tiger, "default", ERROR) == 1

        purged = tiger.purge_errored_tasks(queues=["a"])
        assert purged == 1

        dep_ids_from_task = Task.from_id(
            tiger, "default", ERROR, dependent.id
        ).depends_on_ids
        assert dep.id not in dep_ids_from_task

        assert dependent.id not in other.get_dependents()

    def test_concurrent_fan_in_multiple_deps_different_queues(self, tiger, redis):
        redis.delete("task_order")

        d1 = tiger.delay(
            record_order, args=("D1",), kwargs={"key": "task_order"}, queue="q1"
        )
        d2 = tiger.delay(
            record_order, args=("D2",), kwargs={"key": "task_order"}, queue="q2"
        )
        d3 = tiger.delay(
            record_order, args=("D3",), kwargs={"key": "task_order"}, queue="q3"
        )
        tiger.delay(
            record_order,
            args=("FINAL",),
            kwargs={"key": "task_order"},
            depends_on=[d1, d2, d3],
        )

        assert _get_state_count(tiger, "default", WAITING) == 1

        Pool(3).map(external_worker, [None, None, None])

        time.sleep(DELAY)

        Pool(1).map(external_worker, [None])

        executed = redis.lrange("task_order", 0, -1)
        assert set(executed[:3]) == {"D1", "D2", "D3"}
        assert executed[-1] == "FINAL"

    def test_get_dependency_status(self, tiger, redis):
        dep = tiger.delay(record_order, args=("dep",), kwargs={"key": "task_order"})
        task = tiger.delay(
            record_order,
            args=("task",),
            kwargs={"key": "task_order"},
            depends_on=[dep],
        )

        status = task.get_dependency_status()
        assert len(status) == 1
        assert status[0]["id"] == dep.id
        assert status[0]["state"] == QUEUED
