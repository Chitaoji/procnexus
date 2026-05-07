"""
# procnexus
Provides tools for multiprocessing.

`procnexus` offers a tiny, explicit interface for collecting function calls and
executing them concurrently with Python's `multiprocessing.Pool` or
`multiprocessing.pool.ThreadPool`.

## ✨ Features
* Simple task submission (`submit`) API.
* Batch execution with process or thread pools.
* Asynchronous execution with `start()`, `join()`, and `get()`
* Ordered results (same order as submitted tasks).
* Lightweight wrapper around the standard library.
* Optional thread-based workers for shared-memory or non-picklable callables.

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

# Use threaded=True to run with ThreadNexus instead of ProcNexus.
job = nexus(add, processes=4, threaded=True)
job.submit(1, 2)
job.submit(10, 5)
print(job.run())  # [3, 15]
```

## 🧩 API
### `nexus(func, processes=-1, threaded=False) -> ProcNexus | ThreadNexus`
Create a process-based `ProcNexus` runner or, with `threaded=True`, a thread-based
`ThreadNexus` runner from a callable.
* `func`: target function for each task.
* `processes`: worker setting for the selected pool.
  * `< 0`: use `os.cpu_count()`.
  * `= 0`: do not create a pool; run with normal in-process mapping.
  * `> 0`: pass directly to `multiprocessing.Pool` or `multiprocessing.pool.ThreadPool`.
* `threaded`: when `False`, use process workers; when `True`, use thread workers and
  return `ThreadNexus`.

### `ProcNexus` and `ThreadNexus`
`ThreadNexus` has the same lifecycle and ordered result behavior as `ProcNexus`, but uses threads instead of processes. This means workers share memory with the parent process and the submitted callable/arguments do not need to be picklable.

### `ProcNexus.submit(*args, **kwargs) -> None` / `ThreadNexus.submit(*args, **kwargs) -> None`
Queue one invocation of `func`. Before `start()`, the invocation is stored for later
execution. After `start()` and before `join()`, the invocation is scheduled immediately
and is included in the ordered `get()` result.

### `ProcNexus.start() -> None`
Start executing all queued tasks. With `processes=0`, this computes immediately in the
current process; otherwise it starts a process pool asynchronously.

### `ProcNexus.join(timeout=None) -> None`
Wait for a previously started run to finish. Results are stored on the runner instead of
being returned directly. For process-pool runs, `timeout` is passed to each task result
wait; if it expires, unfinished workers are terminated and
`multiprocessing.TimeoutError` is raised.

### `ProcNexus.get() -> list` / `ThreadNexus.get() -> list`
Return results in submission order, including tasks submitted after `start()`. If the
runner is still active, `get()` raises `RuntimeError`; call `join()` before retrieving
results.

### `ProcNexus.run() -> list` / `ThreadNexus.run() -> list`
Execute all currently queued tasks in parallel and return results in submission order.
This one-shot convenience method leaves the runner in the pending state and keeps
submitted tasks queued, so it can be called repeatedly before `start()`.

## 📝 Notes
* For process workers, the submitted callable should be picklable by `multiprocessing`.
* For process workers, arguments must also be serializable for inter-process communication.
* Thread workers share memory and can run non-picklable callables, but Python thread scheduling still follows the normal GIL rules.
* Exceptions from workers propagate when calling `join()` or `run()`.

## 🔗 See Also
### Github repository
* https://github.com/Chitaoji/procnexus/

### PyPI project
* https://pypi.org/project/procnexus/

## ⚖️ License
This project falls under the BSD 3-Clause License.

"""

from . import core
from .core import *

__all__: list[str] = []
__all__.extend(core.__all__)
