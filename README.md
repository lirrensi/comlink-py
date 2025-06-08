# comlink-py

**comlink-py** is a lightweight, zero-boilerplate Python IPC (inter-process communication) library inspired by [comlink.js](https://github.com/GoogleChromeLabs/comlink).  
It lets you **spawn and control a Python worker process** (even from a different Python virtual environment) through sync and async proxy objects—like calling regular Python functions!

---

## Added v2 with ZeroMQ+msgpack as transport, now works interop with both Node | Py

See `comlink_ipc_v2/examples`

---

## Why use comlink-py?

**comlink-py** solves a real pain for ML, data science, and plugin authors:

-   🧠 **Isolate dependencies**: Run each process in its own virtual environment or Python version.
-   💥 **No REST, sockets, or "multiprocessing" hackery** needed for simple synchronous and asynchronous calls between Python processes.
-   🦾 **Dead-simple call syntax**: Just import your worker as a Python object!
-   🔬 Great for experimenting with models/tools that require different CUDA/drivers/Python versions on the same system.
-   💡 Useful for mixed environments (Windows, Linux) in modular/monolithic codebases.
-   💡 Useful so you do not to run servers to communicate if would be setting on network stack, and also can see print()s directly from your parent script - useful for debug

---

## How comlink-py works

-   Uses **stdin/stdout** pipes and **JSON messages** for communication.
-   Main process talks to worker with simple Python calls, and gets structured results/errors.
-   Child process can be anything runnable by Python: new venv, different Python binary, etc.

---

## Limitations

-   **NOT for heavy/high-frequency IPC**: Pipes + JSON are for small commands, not large data or high throughput.
-   **No file handles, sockets, or raw objects**: Only JSON-serializable results can cross the boundary.
-   **No built-in process clustering** (just one worker per controller).
-   **No remote network IPC out of the box** (no sockets, HTTP, etc).

---

## Install

Copy the `comlink` directory into your project, or install via pip (coming soon):
Requires Python 3.5 or higher (for built-in asyncio support) with no additional dependencies.

```bash
pip install git+https://github.com/lirrensi/comlink-py.git
```

---

## Example: Add Two Numbers in a Different venv

Suppose you have a worker script in a separate venv, possibly with conflicting dependencies.

`some_folder/worker_script.py`:

```python
import os
from comlink_ipc import ChildWorker

# you most definitely want to set CWD as script to use relative paths
# setting cwd from process may be unreliable
# anyway, its your problem to deal with paths
script_directory = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_directory)

class MyWorker(ChildWorker):
    def add(self, x, y):
        return x + y

    async def slow_add(self, x, y):
        import asyncio
        await asyncio.sleep(1)
        return x + y

if __name__ == '__main__':
    MyWorker().run()
```

Your main host script (can use a different venv!):

```python
from comlink_ipc import ParentWorker

# Synchronous usage
worker = ParentWorker(
    "some_folder/worker_script.py",
    python_executable="some_folder/venv/Scripts/python.exe",  # Path to target venv python
    thread_pool_size=3,
    auto_restart=True,
    max_restart_attempts=3,
)
worker.start()
try:
    print("ADD (sync):", worker.proxy.add(2, 3))
    try:
        # Will raise TimeoutError if too slow
        worker.proxy.slow_add(8, 1, _timeout=0.5)
    except TimeoutError:
        print("Timeout as expected!")
finally:
    worker.close()
```

Or fully async:

```python
import asyncio
from comlink_ipc import ParentWorker

async def main():
    worker = ParentWorker("some_folder/worker_script.py", "some_folder/venv/Scripts/python.exe")
    await worker.astart()
    try:
        out = await worker.aproxy.slow_add(4, 5, _timeout=2)
        print("slow_add async:", out)
    finally:
        await worker.aclose()

asyncio.run(main())
```

---

## Usage: Quickstart

-   Write a worker class inheriting from `ChildWorker`, implement some public methods (sync or async).
-   Run a `ParentWorker` in your main process, pointing to the worker script and Python executable.
-   Call `worker.proxy.your_method(...)` (sync) or `await worker.aproxy.your_method(...)` (async).
-   **Arguments and return values must be JSON-serializable!**

---

## API

### ParentWorker

```python
ParentWorker(
    script_path,
    python_executable=None,
    thread_pool_size=3,
    auto_restart=False,
    max_restart_attempts=3,
)
```

-   `.start()`, `.close()` — Setup and shutdown worker (sync)
-   `.astart()`, `.aclose()` — Async versions for async code
-   `.proxy.FUNC(...)` — Call function in the worker synchronously
-   `.aproxy.FUNC(...)` — Call function in the worker asynchronously

### ChildWorker

-   Inherit from, and implement methods
-   Supports both `def` and `async def`
-   If using files, always set `os.chdir(...)` at the start of your worker script

---

## Error Handling

-   Errors in the worker raise `RemoteCallError` in the main process, preserving traceback.
-   Timeouts (for both sync and async) raise `TimeoutError`, cancel the call, and clean up requests.
-   If the worker process dies, subsequent requests error out or are auto-restarted (if configured).

---

## FAQ

**Q: What can I pass between parent and worker?**  
A: Arguments and return values must be JSON-serializable.

**Q: Does it work on Windows, Linux, macOS?**  
A: Yes! Uses pure Python subprocess. Use correct `python_executable` path for your OS.

**Q: Can I have the child execute code asynchronously?**  
A: Yes! Just make your method `async def`—parent can call via `.aproxy` or `.proxy` transparently.

---

## Not works?

Take a look at [procbridge-python](https://github.com/gongzhang/procbridge-python)

---

## When to use this?

-   Want easy plugin/extension-style architecture with hard dependency or version isolation
-   ML, scraping, ETL, or automation code that must run in drastically different Python/driver/OS setups
-   Sane local IPC between Python scripts/venvs with zero ops/devops overhead

---

## When not to use

-   You need to pass large arrays, images, files, numpy tensors (use proxy by filename, not raw data), or extremely high performance IPC (consider ZeroMQ, gRPC, etc)
-   For distributed/networked/multiprocess web servers

---

## Credits

Inspired by comlink.js.  
Written by lirrensi + Claude 3.7. PRs/Improvements welcome!
