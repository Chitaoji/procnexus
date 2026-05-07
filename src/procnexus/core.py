"""
Contains the core of procnexus: nexus(...), etc.

NOTE: this module is private. All functions and objects are available in the main
`procnexus` namespace - use that instead.

"""

__all__ = ["nexus"]

from abc import ABC, abstractmethod
from multiprocessing import Pool
from multiprocessing.pool import AsyncResult
from multiprocessing.pool import Pool as PoolType
from multiprocessing.pool import ThreadPool
from os import cpu_count
from typing import Callable, Literal, overload


@overload
def nexus[**P, T](
    func: Callable[P, T], *, processes: int | None = None
) -> "ProcNexus[P, T]": ...


@overload
def nexus[**P, T](
    func: Callable[P, T], *, threads: int | None = None
) -> "ThreadNexus[P, T]": ...


def nexus[**P, T](
    func: Callable[P, T], **options: int | None
) -> "ParallelNexus[P, T]":
    """
    Create a process-backed or thread-backed scheduler for a callable.

    This validates arguments and returns a scheduler instance that can collect
    task arguments through ``submit`` and execute them in parallel through
    ``run``, or asynchronously through ``start``, ``join``, and ``get``.

    Parameters
    ----------
    func : Callable[P, T]
        Callable executed for each submitted task.
    processes : int | None, default=None
        Select a process-backed scheduler and configure its process pool.
        Positive values are forwarded to ``multiprocessing.Pool``. Negative
        values use ``os.cpu_count()``. ``0`` and ``None`` run in-process
        without creating a pool.
    threads : int | None, default=None
        Select a thread-backed scheduler and configure its thread pool.
        Positive values are forwarded to ``multiprocessing.pool.ThreadPool``.
        Negative values use ``os.cpu_count()``. ``0`` and ``None`` run
        in-process without creating a pool. This option is mutually exclusive
        with ``processes``.

    Returns
    -------
    ParallelNexus[P, T]
        A scheduler bound to ``func``.

    """
    if not callable(func):
        raise TypeError(f"func should be callable, got {func} instead")

    unexpected = set(options) - {"processes", "threads"}
    if unexpected:
        name = next(iter(unexpected))
        raise TypeError(f"nexus() got an unexpected keyword argument {name!r}")

    if "processes" in options and "threads" in options:
        raise TypeError("processes and threads are mutually exclusive")

    if "threads" in options:
        threads = _validate_worker_count("threads", options["threads"])
        return ThreadNexus(func, processes=threads)

    processes = _validate_worker_count("processes", options.get("processes"))
    return ProcNexus(func, processes=processes)


def _validate_worker_count(name: str, value: int | None) -> int | None:
    if not isinstance(value, int | None) or isinstance(value, bool):
        raise TypeError(
            f"invalid type for {name}: "
            f"expected {int | None}, got {type(value)} instead"
        )
    if value == 0:
        return None
    return value


class ParallelNexus[**P, T](ABC):
    """
    Shared scheduler implementation for process-backed and thread-backed runners.

    Subclasses provide the concrete pool implementation by overriding
    :meth:`_create_pool`; the lifecycle, task queueing, and result ordering
    behavior lives here so the concrete runners share one parent.

    Parameters
    ----------
    func : Callable[P, T]
        Callable executed for each submitted task.
    processes : int | None
        Number of workers used by the selected pool implementation, or ``None``
        to run in-process without creating a pool.

    """

    def __init__(self, func: Callable[P, T], *, processes: int | None) -> None:
        self.func = func
        self.processes = processes
        self.params: list[tuple[tuple[object, ...], dict[str, object]]] = []
        self._state: Literal["pending", "running", "joined"] = "pending"
        self._pool: PoolType | None = None
        self._async_results: list[AsyncResult[T]] = []
        self._result: list[T] = []

    def submit(self, *args: P.args, **kwargs: P.kwargs) -> None:
        """
        Queue or schedule one invocation.

        Before :meth:`start`, the invocation is stored for later execution.
        After :meth:`start` and before :meth:`join`, it is scheduled immediately
        and included in the eventual ordered result.

        Parameters
        ----------
        *args : P.args
            Positional arguments passed to the bound callable.
        **kwargs : P.kwargs
            Keyword arguments passed to the bound callable.

        """
        if self._state == "joined":
            raise RuntimeError("cannot submit tasks after the nexus has joined")
        if self._state == "pending":
            self.params.append((args, kwargs))
            return

        if self.processes is None:
            self._result.append(self.func(*args, **kwargs))
            return

        if self._pool is None:
            raise RuntimeError("nexus is not running")
        self._async_results.append(
            self._pool.apply_async(_invoke_func, (self.func, args, kwargs))
        )

    def start(self) -> None:
        """
        Start executing queued tasks.

        With ``processes=None`` or ``threads=None``, tasks are computed
        immediately in the current process. Otherwise, tasks are launched
        asynchronously in the selected worker pool.

        """
        if self._state != "pending":
            raise RuntimeError("cannot start a nexus that has already started")

        self._state = "running"
        if self.processes is None:
            self._result.extend(
                self.func(*args, **kwargs) for args, kwargs in self.params
            )
            self.params.clear()
            return

        processes = self.processes
        if processes < 0:
            processes = cpu_count() or 1

        self._pool = self._create_pool(processes)
        self._async_results = [
            self._pool.apply_async(_invoke_func, (self.func, args, kwargs))
            for args, kwargs in self.params
        ]
        self.params.clear()
        return

    def join(self, timeout: float | None = None) -> None:
        """
        Wait for asynchronous execution to finish.

        Results include every invocation submitted before this method is called,
        including invocations submitted after :meth:`start`. Use :meth:`get` to
        retrieve them.

        Parameters
        ----------
        timeout : float | None, default=None
            Maximum number of seconds to wait for each pool task. ``None`` waits
            indefinitely. When the timeout expires, unfinished workers are
            terminated and ``multiprocessing.TimeoutError`` is raised.

        """
        if self._state == "pending":
            raise RuntimeError("cannot join before start() has been called")
        if self._state == "joined":
            raise RuntimeError("cannot join a nexus more than once")

        self._state = "joined"
        if self.processes is None:
            return

        if self._pool is None:
            raise RuntimeError("nexus is not running")

        try:
            self._result = [
                async_result.get(timeout) for async_result in self._async_results
            ]
        except BaseException:
            self._pool.terminate()
            raise
        else:
            self._pool.close()
        finally:
            self._pool.join()
            self._pool = None
            self._async_results.clear()
        return

    def get(self) -> list[T]:
        """
        Return task results in submission order.

        If the nexus is still running, this raises instead of implicitly
        joining; call :meth:`join` before retrieving results.

        Returns
        -------
        list[T]
            Results returned by each submitted invocation, in submission order.

        """
        if self._state == "pending":
            raise RuntimeError("cannot get results before start() has been called")
        if self._state == "running":
            raise RuntimeError("cannot get results before join() has been called")
        return self._result

    def run(self) -> list[T]:
        """
        Execute all queued tasks and return their results.

        This one-shot convenience method does not alter the nexus lifecycle
        state or consume queued task parameters, so it can be called repeatedly
        before :meth:`start`.

        Returns
        -------
        list[T]
            Results returned by each submitted invocation, in submission order.

        """
        if self._state != "pending":
            raise RuntimeError("cannot run a nexus that has already started")

        if self.processes is None:
            return [self.func(*args, **kwargs) for args, kwargs in self.params]

        processes = self.processes
        if processes < 0:
            processes = cpu_count() or 1

        with self._create_pool(processes) as pool:
            return pool.starmap(
                _invoke_func,
                ((self.func, args, kwargs) for args, kwargs in self.params),
            )

    @abstractmethod
    def _create_pool(self, processes: int) -> PoolType:
        """Create the concrete worker pool used by this nexus."""


class ProcNexus[**P, T](ParallelNexus[P, T]):
    """Queue and execute function calls via process-based parallelism."""

    def _create_pool(self, processes: int) -> PoolType:
        return Pool(processes=processes)


class ThreadNexus[**P, T](ParallelNexus[P, T]):
    """
    Queue and execute function calls via thread-based parallelism.

    This has the same lifecycle and result-ordering behavior as
    the process-backed runner, but it uses ``multiprocessing.pool.ThreadPool``
    instead of ``multiprocessing.Pool``. Thread workers share memory with the parent
    process and do not require submitted callables or arguments to be picklable.

    Parameters
    ----------
    func : Callable[P, T]
        Callable executed for each submitted task.
    processes : int | None
        Number of worker threads used by ``multiprocessing.pool.ThreadPool``,
        or ``None`` to run in-process without creating a pool.

    """

    def _create_pool(self, processes: int) -> PoolType:
        return ThreadPool(processes=processes)


def _invoke_func[**P, T](
    func: Callable[P, T], args: tuple[object, ...], kwargs: dict[str, object]
) -> T:
    return func(*args, **kwargs)
