"""
Contains the core of procnexus: ... , etc.

NOTE: this module is private. All functions and objects are available in the main
`procnexus` namespace - use that instead.

"""

__all__ = []

from multiprocessing import Pool
from typing import Callable


def nexus[T, **P](func: Callable[P, T], processes: int = -1) -> "ProcNexus[P]":
    if not isinstance(processes, int):
        raise TypeError(
            f"invalid type for processes: expected {int}, got {type(processes)} instead"
        )
    if not callable(func):
        raise TypeError(f"func should be callable, got {func} instead")
    return ProcNexus(func, processes)


class ProcNexus[T, **P]:
    def __init__(self, func: Callable[P, T], processes: int) -> None:
        self.func = func
        self.processes = processes
        self.params: list[P] = []

    def submit(self, *args: P.args, **kwargs: P.kwargs) -> None:
        pass

    def run(self) -> list[T]:
        with Pool(processes=self.processes) as pool:
            res = pool.starmap(self.func, self.params)
        return res
