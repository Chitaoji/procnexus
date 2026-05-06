"""
Contains the core of procnexus: nexus(...), etc.

NOTE: this module is private. All functions and objects are available in the main
`procnexus` namespace - use that instead.

"""

from __future__ import annotations

__all__ = ["nexus"]

from multiprocessing import Pool
from multiprocessing.pool import AsyncResult
from multiprocessing.pool import Pool as PoolType
from os import cpu_count
from typing import Callable, Literal


def nexus[**P, T](func: Callable[P, T], *, processes: int = -1) -> "ProcNexus[P, T]":
    """
    Create a ``ProcNexus`` scheduler for a callable.

    This validates arguments and returns a scheduler instance that can collect
    task arguments through :meth:`ProcNexus.submit` and execute them in
    parallel through :meth:`ProcNexus.run`, or asynchronously through
    :meth:`ProcNexus.start` and :meth:`ProcNexus.join`.

    Parameters
    ----------
    func : Callable[P, T]
        Callable executed for each submitted task.
    processes : int, default=-1
        Number of worker processes to use. This value is forwarded to
        :class:`multiprocessing.Pool` when greater than zero. Negative values
        use ``os.cpu_count()``; zero runs in-process.

    Returns
    -------
    ProcNexus[P, T]
        A scheduler bound to ``func``.

    """
    if not isinstance(processes, int):
        raise TypeError(
            f"invalid type for processes: expected {int}, got {type(processes)} instead"
        )
    if not callable(func):
        raise TypeError(f"func should be callable, got {func} instead")
    return ProcNexus(func, processes=processes)


class ProcNexus[**P, T]:
    """
    Queue and execute function calls via process-based parallelism.

    Parameters
    ----------
    func : Callable[P, T]
        Callable executed for each submitted task.
    processes : int
        Number of worker processes used by ``multiprocessing.Pool``.

    """

    def __init__(self, func: Callable[P, T], *, processes: int) -> None:
        self.func = func
        self.processes = processes
        self.params: list[tuple[tuple[object, ...], dict[str, object]]] = []
        self._state: Literal["pending", "running", "joined"] = "pending"
        self._pool: PoolType | None = None
        self._async_result: AsyncResult[list[T]] | None = None
        self._result: list[T] | None = None

    def submit(self, *args: P.args, **kwargs: P.kwargs) -> None:
        """
        Queue one invocation for later execution.

        Parameters
        ----------
        *args : P.args
            Positional arguments passed to the bound callable.
        **kwargs : P.kwargs
            Keyword arguments passed to the bound callable.

        """
        if self._state != "pending":
            raise RuntimeError("cannot submit tasks after the nexus has started")
        self.params.append((args, kwargs))

    def start(self) -> "ProcNexus[P, T]":
        """
        Start executing queued tasks.

        With ``processes=0``, tasks are computed immediately in the current
        process. Otherwise, tasks are launched asynchronously in a process pool.

        Returns
        -------
        ProcNexus[P, T]
            This scheduler, so callers can chain ``nexus(...).start().join()``.

        """
        if self._state != "pending":
            raise RuntimeError("cannot start a nexus that has already started")

        self._state = "running"
        if self.processes == 0:
            self._result = [self.func(*args, **kwargs) for args, kwargs in self.params]
            return self

        processes = self.processes
        if processes < 0:
            processes = cpu_count() or 1

        self._pool = Pool(processes=processes)
        self._async_result = self._pool.starmap_async(
            _invoke_func,
            ((self.func, args, kwargs) for args, kwargs in self.params),
        )
        return self

    def join(self) -> list[T]:
        """
        Wait for asynchronous execution to finish and return task results.

        Returns
        -------
        list[T]
            Results returned by each submitted invocation, in submission order.

        """
        if self._state == "pending":
            raise RuntimeError("cannot join before start has been called")
        if self._state == "joined":
            raise RuntimeError("cannot join a nexus more than once")

        self._state = "joined"
        if self.processes == 0:
            return self._result or []

        if self._async_result is None or self._pool is None:
            raise RuntimeError("nexus is not running")

        try:
            res = self._async_result.get()
        except BaseException:
            self._pool.terminate()
            raise
        else:
            self._pool.close()
            return res
        finally:
            self._pool.join()
            self._pool = None
            self._async_result = None

    def run(self) -> list[T]:
        """
        Execute all queued tasks and return their results.

        Returns
        -------
        list[T]
            Results returned by each submitted invocation, in submission order.

        """
        return self.start().join()


def _invoke_func[**P, T](
    func: Callable[P, T], args: tuple[object, ...], kwargs: dict[str, object]
) -> T:
    return func(*args, **kwargs)
