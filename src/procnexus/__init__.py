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

# Use threads=... to run with threads instead of processes.
job = nexus(add, threads=4)
job.submit(1, 2)
job.submit(10, 5)
print(job.run())  # [3, 15]
```

## 🧩 API
### `nexus(func, processes=None)`
### `nexus(func, threads=None)`
Create a process-backed runner with `processes` or a thread-backed runner with `threads`.
* `func`: target function for each task.
* `processes`: process worker setting.
  * `< 0`: use `os.cpu_count()`.
  * `0` or `None`: do not create a pool; run with normal in-process mapping.
  * `> 0`: pass directly to `multiprocessing.Pool`.
* `threads`: thread worker setting. Passing this option selects `ThreadNexus`.
  * `< 0`: use `os.cpu_count()`.
  * `0` or `None`: do not create a pool; run with normal in-process mapping.
  * `> 0`: pass directly to `multiprocessing.pool.ThreadPool`.
* `processes` and `threads` are mutually exclusive.

### Runner behavior
Runners created by `nexus()` share the same lifecycle and ordered result behavior. The default runner uses processes, while passing `threads` uses threads. Thread workers share memory with the parent process and the submitted callable/arguments do not need to be picklable.

### `runner.submit(*args, **kwargs) -> None`
Queue one invocation of `func`. Before `start()`, the invocation is stored for later
execution. After `start()` and before `join()`, the invocation is scheduled immediately
and is included in the ordered `get()` result.

### `runner.start() -> None`
Start executing all queued tasks. With `processes=0`, `processes=None`, `threads=0`, or `threads=None`, this computes
immediately in the current process; otherwise it starts the selected worker pool
asynchronously.

### `runner.join(timeout=None) -> None`
Wait for a previously started run to finish. Results are stored on the runner instead of
being returned directly. For pooled runs, `timeout` is passed to each task result
wait; if it expires, unfinished workers are terminated and
`multiprocessing.TimeoutError` is raised.

### `runner.get() -> list`
Return results in submission order, including tasks submitted after `start()`. If the
runner is still active, `get()` raises `RuntimeError`; call `join()` before retrieving
results.

### `runner.run() -> list`
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
