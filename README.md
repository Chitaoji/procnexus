# procnexus
Provides tools for multiprocessing.

`procnexus` offers a tiny, explicit interface for collecting function calls and executing them concurrently with Python's `multiprocessing.Pool`.

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
Queue one invocation of `func`. Before `start()`, the invocation is stored for later execution. After `start()` and before `join()`, the invocation is scheduled immediately and is included in the ordered `get()` result.

### `ProcNexus.start() -> None`
Start executing all queued tasks. With `processes=0`, this computes immediately in the current process; otherwise it starts a process pool asynchronously.

### `ProcNexus.join() -> None`
Wait for a previously started run to finish. Results are stored on the runner instead of being returned directly.

### `ProcNexus.get() -> list`
Return results in submission order, including tasks submitted after `start()`. If the runner is still active, `get()` waits for it to finish before returning.

### `ProcNexus.run() -> list`
Execute all queued tasks in parallel and return results in submission order. This is equivalent to calling `start()`, `join()`, and then `get()`.

## 📝 Notes
* The submitted callable should be picklable by `multiprocessing`.
* Arguments must also be serializable for inter-process communication.
* Exceptions from worker processes propagate when calling `join()`, `get()`, or `run()`.

## 🔗 See Also
### Github repository
* https://github.com/Chitaoji/procnexus/

### PyPI project
* https://pypi.org/project/procnexus/

## ⚖️ License
This project falls under the BSD 3-Clause License.

## 🕒 History
### v0.0.0
* Initial release.