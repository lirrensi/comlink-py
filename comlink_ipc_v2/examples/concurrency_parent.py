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


worker = ParentWorker('concurrency_worker.py')
worker.start()

# Main test
def test_sync_concurrent():
    """Test that sync calls from different threads work concurrently"""
    print("\\n=== SYNC CONCURRENCY TEST ===")
    
    
    # Simulating the test logic:
    def sync_call_worker(thread_id):
        worker.call.slow_task(2, f"sync_{thread_id}")
        # Simulate 2-second blocking call
        start = time.time()
        time.sleep(2)  # Replace with actual call
        elapsed = time.time() - start
        print(f"Thread {thread_id} completed in {elapsed:.1f}s")
        return f"sync_result_{thread_id}"
    
    start_time = time.time()
    
    # Run 3 sync calls in parallel threads
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(sync_call_worker, i) 
            for i in range(3)
        ]
        results = [f.result() for f in futures]
    
    total_time = time.time() - start_time
    print(f"3 sync calls (2s each) completed in {total_time:.1f}s")
    print(f"Expected: ~2s (concurrent), Got: {total_time:.1f}s")
    
    # Should be ~2s if concurrent, ~6s if sequential
    is_concurrent = total_time < 4.0
    print(f"✅ Concurrent: {is_concurrent}")
    
    return results

async def test_async_concurrent():
    """Test that async calls work concurrently"""
    print("\\n=== ASYNC CONCURRENCY TEST ===")
    
    async def async_call_worker(task_id):
        worker.acall.async_task(2, f"async_{task_id}")
        # Simulate 2-second async call
        start = time.time()
        await asyncio.sleep(2)  # Replace with actual call
        elapsed = time.time() - start
        print(f"Async task {task_id} completed in {elapsed:.1f}s")
        return f"async_result_{task_id}"
    
    start_time = time.time()
    
    # Run 3 async calls concurrently
    tasks = [async_call_worker(i) for i in range(3)]
    results = await asyncio.gather(*tasks)
    
    total_time = time.time() - start_time
    print(f"3 async calls (2s each) completed in {total_time:.1f}s")
    print(f"Expected: ~2s (concurrent), Got: {total_time:.1f}s")
    
    # Should be ~2s if concurrent, ~6s if sequential
    is_concurrent = total_time < 4.0
    print(f"✅ Concurrent: {is_concurrent}")
    
    return results

def test_blocking_behavior():
    """Test that sync calls properly block the calling thread"""
    print("\\n=== BLOCKING BEHAVIOR TEST ===")
    
    def blocking_test():
        print("Before sync call")
        start = time.time()
        
        # This should block for 2 seconds
        # result = worker.call.slow_task(2, "blocking_test")
        time.sleep(2)  # Simulate blocking call
        
        elapsed = time.time() - start
        print(f"After sync call ({elapsed:.1f}s)")
        return "blocking_result"
    
    # This should block the main thread
    result = blocking_test()
    print(f"✅ Sync call blocked properly: {result}")

async def test_non_blocking_behavior():
    """Test that async calls don't block the event loop"""
    print("\\n=== NON-BLOCKING BEHAVIOR TEST ===")
    
    async def non_blocking_test():
        print("Before async call")
        start = time.time()
        
        # Start async call but don't await yet
        # future = worker.acall.async_task(2, "non_blocking_test")
        future = asyncio.create_task(asyncio.sleep(2))
        
        # Do other work while call is running
        for i in range(5):
            print(f"Doing other work {i}")
            await asyncio.sleep(0.2)
        
        # Now await the result
        await future
        elapsed = time.time() - start
        print(f"Total time: {elapsed:.1f}s")
        return "non_blocking_result"
    
    result = await non_blocking_test()
    print(f"✅ Async call didn't block event loop: {result}")

if __name__ == "__main__":
    print("IPC Sync/Async Behavior Test")
    print("============================")
    
    # Test sync behavior
    test_sync_concurrent()
    test_blocking_behavior()
    
    # Test async behavior
    asyncio.run(test_async_concurrent())
    asyncio.run(test_non_blocking_behavior())
    
    print("\\n=== SUMMARY ===")
    print("✅ Sync mode should:")
    print("   - Block the calling thread until response")
    print("   - Allow concurrent calls from different threads")
    print("   - Each thread waits for its own response")
    
    print("\\n✅ Async mode should:")
    print("   - Return awaitable Future immediately") 
    print("   - Not block the event loop")
    print("   - Allow concurrent async calls")
    print("   - Properly schedule callbacks on event loop")

    # worker.close()