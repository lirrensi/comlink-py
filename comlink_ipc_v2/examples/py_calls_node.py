# Simple test to verify sync/async behavior
import asyncio
import time
import threading
import sys
import os
from concurrent.futures import ThreadPoolExecutor

# Test child worker (save as test_worker.py)


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from comlink_v2 import ParentWorker


import os
script_dir = os.path.dirname(os.path.abspath(__file__))
ts_file = os.path.join(script_dir, "..", "examples", "math_worker.ts")
worker = ParentWorker(ts_file, "tsx.cmd")

worker.start()

if __name__ == "__main__":

    fibo = worker.call.fibonacci(11)
    print("got fibo => ", fibo)

    fact = worker.call.factorial(11)
    print("got fact => ", fact)
    
    worker.close()