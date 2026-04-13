import sys
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Type

from ._internal import import_attribute, serialize_func_name
from .exceptions import TaskImportError
from .runner import BaseRunner, DefaultRunner

if TYPE_CHECKING:
    from . import Task, TaskTiger


class RunnerContext:
    __slots__ = ("_data", "_timings", "_start_time")

    def __init__(self) -> None:
        self._data: Dict[str, Any] = {}
        self._timings: List[Dict[str, Any]] = []
        self._start_time: Optional[float] = None

    def set(self, key: str, value: Any) -> None:
        self._data[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def __contains__(self, key: str) -> bool:
        return key in self._data

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self._data[key] = value

    def __delitem__(self, key: str) -> None:
        del self._data[key]

    def __len__(self) -> int:
        return len(self._data)

    def __bool__(self) -> bool:
        return bool(self._data)

    def keys(self):  # type: ignore[no-untyped-def]
        return self._data.keys()

    def values(self):  # type: ignore[no-untyped-def]
        return self._data.values()

    def items(self):  # type: ignore[no-untyped-def]
        return self._data.items()

    def pop(self, key: str, *args: Any) -> Any:
        return self._data.pop(key, *args)

    def update(self, other: Dict[str, Any]) -> None:
        self._data.update(other)

    def clear(self) -> None:
        self._data.clear()

    def setdefault(self, key: str, default: Any = None) -> Any:
        return self._data.setdefault(key, default)

    def as_dict(self) -> Dict[str, Any]:
        return dict(self._data)

    def start_timing(self) -> None:
        self._start_time = time.monotonic()

    def record_timing(self, runner_name: str, phase: str, duration: float) -> None:
        self._timings.append(
            {"runner_name": runner_name, "phase": phase, "duration": duration}
        )

    @property
    def timings(self) -> List[Dict[str, Any]]:
        return list(self._timings)

    @property
    def elapsed(self) -> float:
        if self._start_time is None:
            return 0.0
        return time.monotonic() - self._start_time

    def __repr__(self) -> str:
        return f"RunnerContext({self._data!r})"


def validate_runner_chain(runner_classes: List[Type[BaseRunner]]) -> None:
    if not runner_classes:
        raise ValueError("Runner chain must not be empty.")
    for cls in runner_classes:
        if not isinstance(cls, type):
            raise TypeError(f"{cls!r} is not a class.")
        if not issubclass(cls, BaseRunner):
            raise TypeError(f"{cls!r} is not a BaseRunner subclass.")
    seen: set = set()
    for cls in runner_classes:
        if cls in seen:
            raise ValueError(f"Duplicate runner class in chain: {cls!r}.")
        seen.add(cls)


def serialize_runner_chain(runner_classes: List[Type[BaseRunner]]) -> List[str]:
    return [serialize_func_name(cls) for cls in runner_classes]


def deserialize_runner_chain(paths: List[str]) -> List[Type[BaseRunner]]:
    result = []
    for path in paths:
        try:
            cls = import_attribute(path)
        except TaskImportError:
            raise
        result.append(cls)
    return result


def _runner_overrides_execution(runner_class: Type[BaseRunner]) -> bool:
    for method_name in ("run_single_task", "run_batch_tasks"):
        if getattr(runner_class, method_name) is not getattr(BaseRunner, method_name):
            return True
    return False


def _find_executor_index(runner_classes: List[Type[BaseRunner]]) -> Optional[int]:
    last_idx = None
    for i, cls in enumerate(runner_classes):
        if _runner_overrides_execution(cls):
            last_idx = i
    return last_idx


def resolve_runner_chain(
    runner_classes: List[Type[BaseRunner]],
) -> Tuple[Optional[Type[BaseRunner]], List[Type[BaseRunner]]]:
    executor_idx = _find_executor_index(runner_classes)
    if executor_idx is None:
        return None, list(runner_classes)
    executor_class = runner_classes[executor_idx]
    hook_classes = [cls for i, cls in enumerate(runner_classes) if i != executor_idx]
    return executor_class, hook_classes


class ChainedRunner(BaseRunner):
    def __init__(
        self, tiger: "TaskTiger", runner_classes: List[Type[BaseRunner]]
    ) -> None:
        super().__init__(tiger)
        validate_runner_chain(runner_classes)
        self._runner_classes = list(runner_classes)
        self._runners = [cls(tiger) for cls in runner_classes]
        executor_class, hook_classes = resolve_runner_chain(runner_classes)
        if executor_class is None:
            self._executor = DefaultRunner(tiger)
        else:
            idx = _find_executor_index(runner_classes)
            self._executor = self._runners[idx]  # type: ignore[index]
        self._hook_runners = [
            r for r in self._runners if r is not self._executor
        ]

    @property
    def runners(self) -> List[BaseRunner]:
        return list(self._runners)

    @property
    def executor(self) -> BaseRunner:
        return self._executor

    @property
    def hook_runners(self) -> List[BaseRunner]:
        return list(self._hook_runners)

    @property
    def runner_classes(self) -> List[Type[BaseRunner]]:
        return list(self._runner_classes)

    def _run_before_hooks(
        self,
        task_or_tasks: Any,
        context: RunnerContext,
        is_batch: bool = False,
    ) -> List[BaseRunner]:
        completed: List[BaseRunner] = []
        for runner in self._hook_runners:
            t0 = time.monotonic()
            try:
                if is_batch:
                    runner.before_batch_execute(task_or_tasks, context)
                else:
                    runner.before_execute(task_or_tasks, context)
            except Exception:
                context.record_timing(
                    type(runner).__name__,
                    "before",
                    time.monotonic() - t0,
                )
                for done in reversed(completed):
                    try:
                        if is_batch:
                            done.after_batch_execute(task_or_tasks, context)
                        else:
                            done.after_execute(task_or_tasks, context)
                    except Exception:
                        pass
                raise
            context.record_timing(
                type(runner).__name__,
                "before",
                time.monotonic() - t0,
            )
            completed.append(runner)
        return completed

    def _run_after_hooks(
        self,
        task_or_tasks: Any,
        context: RunnerContext,
        completed: List[BaseRunner],
        is_batch: bool = False,
    ) -> None:
        for runner in reversed(completed):
            t0 = time.monotonic()
            if is_batch:
                runner.after_batch_execute(task_or_tasks, context)
            else:
                runner.after_execute(task_or_tasks, context)
            context.record_timing(
                type(runner).__name__,
                "after",
                time.monotonic() - t0,
            )

    def _run_error_hooks(
        self,
        task_or_tasks: Any,
        context: RunnerContext,
        completed: List[BaseRunner],
        exc_info: tuple,
        is_batch: bool = False,
    ) -> bool:
        suppressed = False
        for runner in reversed(completed):
            t0 = time.monotonic()
            try:
                if is_batch:
                    result = runner.on_batch_execute_error(
                        task_or_tasks, context, exc_info
                    )
                else:
                    result = runner.on_execute_error(
                        task_or_tasks, context, exc_info
                    )
                if result is True:
                    suppressed = True
            except Exception:
                pass
            context.record_timing(
                type(runner).__name__,
                "error",
                time.monotonic() - t0,
            )
        return suppressed

    def run_single_task(self, task: "Task", hard_timeout: float) -> None:
        context = RunnerContext()
        context.start_timing()
        completed = self._run_before_hooks(task, context)
        try:
            t0 = time.monotonic()
            self._executor.run_single_task(task, hard_timeout)
            context.record_timing(
                type(self._executor).__name__, "execute", time.monotonic() - t0
            )
        except Exception:
            suppressed = self._run_error_hooks(
                task, context, completed, sys.exc_info()
            )
            if not suppressed:
                raise
            return
        self._run_after_hooks(task, context, completed)

    def run_batch_tasks(self, tasks: List["Task"], hard_timeout: float) -> None:
        context = RunnerContext()
        context.start_timing()
        completed = self._run_before_hooks(tasks, context, is_batch=True)
        try:
            t0 = time.monotonic()
            self._executor.run_batch_tasks(tasks, hard_timeout)
            context.record_timing(
                type(self._executor).__name__, "execute", time.monotonic() - t0
            )
        except Exception:
            suppressed = self._run_error_hooks(
                tasks, context, completed, sys.exc_info(), is_batch=True
            )
            if not suppressed:
                raise
            return
        self._run_after_hooks(tasks, context, completed, is_batch=True)

    def run_eager_task(self, task: "Task") -> Any:
        context = RunnerContext()
        context.start_timing()
        completed = self._run_before_hooks(task, context)
        try:
            t0 = time.monotonic()
            result = self._executor.run_eager_task(task)
            context.record_timing(
                type(self._executor).__name__, "execute", time.monotonic() - t0
            )
        except Exception:
            suppressed = self._run_error_hooks(
                task, context, completed, sys.exc_info()
            )
            if not suppressed:
                raise
            return None
        self._run_after_hooks(task, context, completed)
        return result

    def on_permanent_error(
        self, task: "Task", execution: Dict[str, Any] | None
    ) -> None:
        for runner in self._runners:
            runner.on_permanent_error(task, execution)


def make_chained_runner_class(
    runner_classes: List[Type[BaseRunner]],
) -> Type[ChainedRunner]:
    validated = list(runner_classes)

    class _BoundChainedRunner(ChainedRunner):
        def __init__(self, tiger: "TaskTiger") -> None:
            super().__init__(tiger, runner_classes=validated)

    _BoundChainedRunner.__qualname__ = (
        f"ChainedRunner[{','.join(c.__name__ for c in validated)}]"
    )
    return _BoundChainedRunner
