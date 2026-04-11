"""
Contains the core of procnexus: nexus(...), etc.

NOTE: this module is private. All functions and objects are available in the main
`procnexus` namespace - use that instead.

"""

__all__ = ["nexus"]

from multiprocessing import Pool
from typing import Callable


def nexus[T, **P](func: Callable[P, T], processes: int = -1) -> "ProcNexus[P]":
    """
    Create a ``ProcNexus`` scheduler for a callable.

    This validates arguments and returns a scheduler instance that can collect
    task arguments through :meth:`ProcNexus.submit` and execute them in
    parallel through :meth:`ProcNexus.run`.

    Parameters
    ----------
    func : Callable[P, T]
        Callable executed for each submitted task.
    processes : int, default=-1
        Number of worker processes to use. This value is forwarded to
        :class:`multiprocessing.Pool`.

    Returns
    -------
    ProcNexus[P]
        A scheduler bound to ``func``.

    Raises
    ------
    TypeError
        If ``processes`` is not an integer.
    TypeError
        If ``func`` is not callable.

    """
    if not isinstance(processes, int):
        raise TypeError(
            f"invalid type for processes: expected {int}, got {type(processes)} instead"
        )
    if not callable(func):
        raise TypeError(f"func should be callable, got {func} instead")
    return ProcNexus(func, processes)


class ProcNexus[T, **P]:
    """
    Queue and execute function calls via process-based parallelism.

    Parameters
    ----------
    func : Callable[P, T]
        Callable executed for each submitted task.
    processes : int
        Number of worker processes used by ``multiprocessing.Pool``.

    """

    def __init__(self, func: Callable[P, T], processes: int) -> None:
        self.func = func
        self.processes = processes
        self.params: list[tuple[tuple[object, ...], dict[str, object]]] = []

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
        self.params.append((args, kwargs))

    def run(self) -> list[T]:
        """
        Execute all queued tasks and return their results.

        Returns
        -------
        list[T]
            Results returned by each submitted invocation, in submission order.

        """
        with Pool(processes=self.processes) as pool:
            res = pool.starmap(
                _invoke_func,
                ((self.func, args, kwargs) for args, kwargs in self.params),
            )
        return res


def _invoke_func[T, **P](
    func: Callable[P, T], args: tuple[object, ...], kwargs: dict[str, object]
) -> T:
    return func(*args, **kwargs)
