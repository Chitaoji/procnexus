"""
Contains the core of procnexus: nexus(...), etc.

NOTE: this module is private. All functions and objects are available in the main
`procnexus` namespace - use that instead.

"""

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
    :meth:`ProcNexus.start`, :meth:`ProcNexus.join`, and :meth:`ProcNexus.get`.

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

        if self.processes == 0:
            self._result.append(self.func(*args, **kwargs))
            return

        self._async_results.append(self._submit_to_pool(args, kwargs))

    def start(self) -> None:
        """
        Start executing queued tasks.

        With ``processes=0``, tasks are computed immediately in the current
        process. Otherwise, tasks are launched asynchronously in a process pool.

        """
        if self._state != "pending":
            raise RuntimeError("cannot start a nexus that has already started")

        self._state = "running"
        if self.processes == 0:
            self._result.extend(
                self.func(*args, **kwargs) for args, kwargs in self.params
            )
            self.params.clear()
            return

        processes = self.processes
        if processes < 0:
            processes = cpu_count() or 1

        self._pool = Pool(processes=processes)
        self._async_results = [
            self._submit_to_pool(args, kwargs) for args, kwargs in self.params
        ]
        self.params.clear()
        return

    def join(self) -> None:
        """
        Wait for asynchronous execution to finish.

        Results include every invocation submitted before this method is called,
        including invocations submitted after :meth:`start`. Use :meth:`get` to
        retrieve them.

        """
        if self._state == "pending":
            raise RuntimeError("cannot join before start() has been called")
        if self._state == "joined":
            raise RuntimeError("cannot join a nexus more than once")

        self._state = "joined"
        if self.processes == 0:
            return

        if self._pool is None:
            raise RuntimeError("nexus is not running")

        try:
            self._result = [async_result.get() for async_result in self._async_results]
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

    def _submit_to_pool(
        self, args: tuple[object, ...], kwargs: dict[str, object]
    ) -> AsyncResult[T]:
        if self._pool is None:
            raise RuntimeError("nexus is not running")
        return self._pool.apply_async(_invoke_func, (self.func, args, kwargs))

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

        if self.processes == 0:
            return [self.func(*args, **kwargs) for args, kwargs in self.params]

        processes = self.processes
        if processes < 0:
            processes = cpu_count() or 1

        with Pool(processes=processes) as pool:
            return pool.starmap(
                _invoke_func,
                ((self.func, args, kwargs) for args, kwargs in self.params),
            )


def _invoke_func[**P, T](
    func: Callable[P, T], args: tuple[object, ...], kwargs: dict[str, object]
) -> T:
    return func(*args, **kwargs)
