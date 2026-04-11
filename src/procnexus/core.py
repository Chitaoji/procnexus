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
    _summary_.

    Returns
    -------
    _type_
        _description_.

    Raises
    ------
    TypeError
        _description_.
    TypeError
        _description_.

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
    _summary_.

    Parameters
    ----------
    func : Callable[P, T]
        _description_.
    processes : int
        _description_.

    """

    def __init__(self, func: Callable[P, T], processes: int) -> None:
        self.func = func
        self.processes = processes
        self.params: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def submit(self, *args: P.args, **kwargs: P.kwargs) -> None:
        """
        _summary_.

        """
        self.params.append((args, kwargs))

    def run(self) -> list[T]:
        """
        _summary_.

        Returns
        -------
        list[T]
            _description_.

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
