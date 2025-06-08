import time
import asyncio
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from comlink_v2 import ChildWorker

class SimpleWorker(ChildWorker):
    def slow_task(self, duration, task_id):
        print(f"[CHILD] Starting task {task_id} for {duration}s")
        time.sleep(duration)
        result = f"Task {task_id} done after {duration}s"
        print(f"[CHILD] Finished task {task_id}")
        return result
    
    async def async_task(self, duration, task_id):
        print(f"[CHILD] Starting async task {task_id} for {duration}s")
        await asyncio.sleep(duration)
        result = f"Async task {task_id} done after {duration}s"
        print(f"[CHILD] Finished async task {task_id}")
        return result

if __name__ == "__main__":
    worker = SimpleWorker()
    worker.run()