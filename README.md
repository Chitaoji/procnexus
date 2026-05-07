# procnexus
Provides tools for multiprocessing.

`procnexus` offers a tiny, explicit interface for collecting function calls and executing them concurrently with Python's `multiprocessing.Pool` or `multiprocessing.pool.ThreadPool`.

## 🛠️ Installation
```sh
$ pip install procnexus
```

## ✨ Features
* Simple task submission (`submit`) API.
* Batch execution with process or thread pools.
* Asynchronous execution with `start()`, `join()`, and `get()`
* Ordered results (same order as submitted tasks).
* Lightweight wrapper around the standard library.
* Optional thread-based execution for shared-memory or non-picklable callables.

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
### `nexus(func, processes=None, threads=None) -> ParallelNexus`
Create a sequential runner by default, a process-backed runner with `processes`, or a thread-backed runner with `threads`.
* `func`: target function for each task.
* `processes`: process pool size setting.
  * `< 0`: use `os.cpu_count()`.
  * `0` or `None`: normalize to `None`.
  * `> 0`: pass directly to `multiprocessing.Pool`.
* `threads`: thread pool size setting.
  * `< 0`: use `os.cpu_count()`.
  * `0` or `None`: normalize to `None`.
  * `> 0`: pass directly to `multiprocessing.pool.ThreadPool`.
* After normalizing `0` to `None`, exactly one non-`None` setting selects process-backed or thread-backed execution, two non-`None` settings raise `TypeError`, and two `None` settings select sequential execution.

### `ParallelNexus`
Runners created by `nexus()` share the same lifecycle and ordered result behavior. The default runner is sequential; passing `processes` uses process-based concurrency, while passing `threads` uses thread-based concurrency. Threads share memory with the parent process, so the submitted callable and arguments do not need to be picklable.

### `submit(*args, **kwargs) -> None`
Queue one invocation of `func`. Before `start()`, the invocation is stored for later execution. After `start()` and before `join()`, the invocation is scheduled immediately and is included in the ordered `get()` result.

### `start() -> None`
Start executing all queued tasks. Sequential runners compute immediately in the current process; process- and thread-backed runners start asynchronous execution.

### `join(timeout=None) -> None`
Wait for a previously started run to finish. Results are stored on the runner instead of being returned directly. For pooled runs, `timeout` is passed to each task result wait; if it expires, unfinished work is stopped and `multiprocessing.TimeoutError` is raised.

### `get() -> list`
Return results in submission order, including tasks submitted after `start()`. If the runner is still active, `get()` raises `RuntimeError`; call `join()` before retrieving results.

### `run() -> list`
Execute all currently queued tasks in parallel and return results in submission order. This one-shot convenience method leaves the runner in the pending state and keeps submitted tasks queued, so it can be called repeatedly before `start()`.

## 📝 Notes
* For process runners, the submitted callable should be picklable by `multiprocessing`.
* For process runners, arguments must also be serializable for inter-process communication.
* Thread runners share memory and can run non-picklable callables, but Python thread scheduling still follows the normal GIL rules.
* Exceptions from submitted tasks propagate when calling `join()` or `run()`.

## 🔗 See Also
### Github repository
* https://github.com/Chitaoji/procnexus/

### PyPI project
* https://pypi.org/project/procnexus/

## ⚖️ License
This project falls under the BSD 3-Clause License.

## 🕒 History
### v0.0.3


### v0.0.2
* Made `run()` a non-mutating convenience API to better align with Python conventions: it returns results without implicitly advancing the asynchronous `start()`/`join()` lifecycle or consuming queued tasks.
* Updated process-pool `run()` execution to use `multiprocessing.Pool.starmap`, preserving ordered results and keyword-argument handling while keeping queued tasks available for a later async run.
* Added unit coverage for repeated `run()` calls, process-pool execution, keyword arguments, and rejecting `run()` after `start()`.

### v0.0.1
* Added asynchronous execution with `start()`, `join()`, and `get()`, while keeping `run()` as the one-shot convenience API.
* Allowed `submit()` calls after `start()` and before `join()`, preserving submission-order results across queued and late-submitted tasks.
* Expanded README/API documentation and added unit coverage for async lifecycle, ordered results, and invalid state transitions.

### v0.0.0
* Initial release.