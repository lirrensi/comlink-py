# example.py
import asyncio
from comlink_ipc import ParentWorker

# main script
def main_sync():
    print("=== SYNC EXAMPLE ===")
    worker = ParentWorker(
        "some_folder/worker_script.py", # path to script
        "some_folder/venv/Scripts/python.exe", # path virtual env exe
        auto_restart=False, # if auto restart if dead | default False
        max_restart_attempts=2
    )
    worker.start()

    try:
        result = worker.proxy.add(2, 3)
        print("Result from sync add:", result)

        # Timeout example
        try:
            result = worker.proxy.slow_add(5, 6, _timeout=0.5)
        except TimeoutError as e:
            print("Timeout occurred:", e)

    finally:
        worker.close()

async def main_async():
    print("=== ASYNC EXAMPLE ===")
    worker = ParentWorker(
        "some_folder/worker_script.py", # path to script
        "some_folder/venv/Scripts/python.exe", # path virtual env exe
        thread_pool_size=5, # to limit max concurrent operations
        auto_restart=True, # if auto restart if dead | default False
        max_restart_attempts=2
    )
    await worker.astart()

    try:
        result = await worker.aproxy.add(10, 20)
        print("Async result from add:", result)

        result = await worker.aproxy.slow_add(7, 8, _timeout=2)
        print("Async result from slow_add:", result)

    finally:
        await worker.aclose()




# =====================================================
# worker_script.py
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

if __name__ == "__main__":
    MyWorker().run()