"""
# procnexus
Provides tools for multiprocessing.

`procnexus` offers a tiny, explicit interface for collecting function calls and
executing them concurrently with Python's `multiprocessing.Pool`.

## 🛠️ Installation
```sh
$ pip install procnexus
```

## ✨ Features
* Simple task submission (`submit`) API.
* Batch execution with process pools.
* Asynchronous execution with `start()`, `join()`, and `get()`
* Ordered results (same order as submitted tasks).
* Lightweight wrapper around the standard library.

## 🚀 Quick Start
```python
from procnexus import nexus


def add(a: int, b: int) -> int:
    return a + b


job = nexus(add, processes=4)
job.submit(1, 2)
job.submit(10, 5)
job.submit(-1, 8)

results = job.run()
print(results)  # [3, 15, 7]

# Or start the work asynchronously and collect it later.
job = nexus(add, processes=4)
job.submit(1, 2)
job.submit(10, 5)
job.start()
# Do other work here, and optionally submit more tasks before joining.
job.submit(-1, 8)
job.join()
results = job.get()
print(results)  # [3, 15, 7]
```

## 🧩 API
### `nexus(func, processes=-1) -> ProcNexus`
Create a `ProcNexus` runner from a callable.
* `func`: target function for each task.
* `processes`: worker-process setting.
  * `< 0`: use `os.cpu_count()`.
  * `= 0`: do not create a process pool; run with normal in-process mapping.
  * `> 0`: pass directly to `multiprocessing.Pool`.

### `ProcNexus.submit(*args, **kwargs) -> None`
Queue one invocation of `func`. Before `start()`, the invocation is stored for later
execution. After `start()` and before `join()`, the invocation is scheduled immediately
and is included in the ordered `get()` result.

### `ProcNexus.start() -> None`
Start executing all queued tasks. With `processes=0`, this computes immediately in the
current process; otherwise it starts a process pool asynchronously.

### `ProcNexus.join(timeout=None) -> None`
Wait for a previously started run to finish. Results are stored on the runner instead
of being returned directly. For process-pool runs, `timeout` is passed to each task
result wait; if it expires, unfinished workers are terminated and
`multiprocessing.TimeoutError` is raised.

### `ProcNexus.get() -> list`
Return results in submission order, including tasks submitted after `start()`. If the
runner is still active, `get()` raises `RuntimeError`; call `join()` before retrieving results.

### `ProcNexus.run() -> list`
Execute all currently queued tasks in parallel and return results in submission order.
This one-shot convenience method leaves the runner in the pending state and keeps
submitted tasks queued, so it can be called repeatedly before `start()`.

## 📝 Notes
* The submitted callable should be picklable by `multiprocessing`.
* Arguments must also be serializable for inter-process communication.
* Exceptions from worker processes propagate when calling `join()` or `run()`.

## 🔗 See Also
### Github repository
* https://github.com/Chitaoji/procnexus/

### PyPI project
* https://pypi.org/project/procnexus/

## ⚖️ License
This project falls under the BSD 3-Clause License.

## 🕒 History
### v0.0.3
* Changed `get()` to reject calls while a nexus is still running, making `join()` the explicit synchronization point before result retrieval.
* Added `join(timeout=None)` support for process-pool runs, terminating unfinished workers and propagating `multiprocessing.TimeoutError` when a task wait expires.
* Simplified process-pool task scheduling by inlining `apply_async` usage and refreshed API documentation signatures for bound methods.

### v0.0.2
* Made `run()` a non-mutating convenience API to better align with Python conventions: it returns results without implicitly advancing the asynchronous `start()`/`join()` lifecycle or consuming queued tasks.
* Updated process-pool `run()` execution to use `multiprocessing.Pool.starmap`, preserving ordered results and keyword-argument handling while keeping queued tasks available for a later async run.
* Added unit coverage for repeated `run()` calls, process-pool execution, keyword arguments, and rejecting `run()` after `start()`.

### v0.0.1
* Added configurable process handling: negative values use `os.cpu_count()`, `0` runs in-process, and positive values configure `multiprocessing.Pool`.
* Added asynchronous execution with `start()`, `join()`, and `get()`, while keeping `run()` as the one-shot convenience API.
* Allowed `submit()` calls after `start()` and before `join()`, preserving submission-order results across queued and late-submitted tasks.
* Changed `start()` and `join()` to manage lifecycle only and return `None`; retrieve results with `get()` or `run()`.
* Expanded README/API documentation and added unit coverage for async lifecycle, ordered results, and invalid state transitions.

### v0.0.0
* Initial release.

"""

from . import core
from .core import *

__all__: list[str] = []
__all__.extend(core.__all__)
