"""
# procnexus
Provides tools for multiprocessing.

`procnexus` offers a tiny, explicit interface for collecting function calls and
executing them concurrently with Python's `multiprocessing.Pool`.

## Features
* Simple task submission (`submit`) API.
* Batch execution with process pools.
* Asynchronous execution with `start()` and `join()`.
* Ordered results (same order as submitted tasks).
* Lightweight wrapper around the standard library.

## Quick Start
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
# Do other work here...
results = job.join()
print(results)  # [3, 15]
```

## API
### `nexus(func, processes=-1) -> ProcNexus`
Create a `ProcNexus` runner from a callable.
* `func`: target function for each task.
* `processes`: worker-process setting.
  * `< 0`: use `os.cpu_count()`.
  * `= 0`: do not create a process pool; run with normal in-process mapping.
  * `> 0`: pass directly to `multiprocessing.Pool`.

### `ProcNexus.submit(*args, **kwargs) -> None`
Queue one invocation of `func`.

### `ProcNexus.start() -> ProcNexus`
Start executing all queued tasks and return the current `ProcNexus` instance. With `processes=0`, this computes immediately in the current process; otherwise it starts a process pool asynchronously.

### `ProcNexus.join() -> list`
Wait for a previously started run to finish and return results in submission order.

### `ProcNexus.run() -> list`
Execute all queued tasks in parallel and return results in submission order. This is equivalent to `start().join()`.

## Notes
* The submitted callable should be picklable by `multiprocessing`.
* Arguments must also be serializable for inter-process communication.
* Exceptions from worker processes propagate when calling `join()` or `run()`.

## See Also
### Github repository
* https://github.com/Chitaoji/procnexus/

### PyPI project
* https://pypi.org/project/procnexus/

## License
This project falls under the BSD 3-Clause License.

"""

from . import core
from .core import *

__all__: list[str] = []
__all__.extend(core.__all__)
