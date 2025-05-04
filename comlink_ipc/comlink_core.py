
import subprocess
import json
import sys
import uuid
import time
import threading
import asyncio
import traceback
import os
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor


class RemoteCallError(Exception):
    """Exception for failures that occurred on the remote worker/child."""

# parent controller 
class ParentWorker:
    def __init__(self, script_path, python_executable=None, thread_pool_size=10, auto_restart=False, max_restart_attempts=5):
        self.APP_NAME = "comlink_py"
        self.script_path = script_path
        self.script_name = script_path.split('/')[-1]
        self.executable = python_executable if python_executable else sys.executable
        self.auto_restart = auto_restart
        self.max_restart_attempts = max_restart_attempts

        self.pending_requests = {}
        self.lock = threading.Lock()
        self.thread_pool = ThreadPoolExecutor(max_workers=thread_pool_size)

        # Proxy-like method handler
        self.proxy = self.create_sync_proxy()
        self.aproxy = self.create_async_proxy()


    def start(self):
        self.process = subprocess.Popen(
            [self.executable, self.script_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=1,
            universal_newlines=True
        )
        # Start background threads to read responses
        self.running = True

        # std out
        self.reader_thread = threading.Thread(target=self._read_responses)
        self.reader_thread.daemon = True
        self.reader_thread.start()

        # std err
        self.stderr_thread = threading.Thread(target=self._read_stderr)
        self.stderr_thread.daemon = True
        self.stderr_thread.start()

        # is not dead monitoring
        self.monitor_thread = threading.Thread(target=self._monitor_child_process)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()

    async def astart(self):
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.start)

    def create_sync_proxy(self):
        """Creates a sync proxy for calling remote methods synchronously."""
        class SyncRemoteProxy:
            def __init__(self, controller):
                self._controller = controller  # store reference to parent controller

            def __getattr__(self, name):
                def remote_method(*args, **kwargs):
                    # Extract timeout if provided as a kwarg
                    timeout = kwargs.pop('_timeout', None)
                    # Get the result of the function call on the controller
                    result = self._controller._call_function(name, *args, timeout=timeout, **kwargs)
                    return result  # Return result synchronously (no async handling)
                return remote_method
        return SyncRemoteProxy(self)
    
    def create_async_proxy(self):
        """Creates an async proxy for calling remote methods asynchronously."""
        class AsyncRemoteProxy:
            def __init__(self, controller):
                self._controller = controller  # store reference to parent controller

            def __getattr__(self, name):
                async def remote_method(*args, **kwargs):
                    # Extract timeout if provided as a kwarg
                    timeout = kwargs.pop('_timeout', None)

                    loop = asyncio.get_running_loop()
                    # Offload the synchronous _call_function to a thread to avoid blocking
                    return await loop.run_in_executor(
                        self._controller.thread_pool,
                        lambda: self._controller._call_function(name, *args, timeout=timeout, **kwargs)
                    )

                return remote_method
        return AsyncRemoteProxy(self)  # Returns an instance of AsyncRemoteProxy

    def _call_function(self, func_name, *args, timeout=None, **kwargs):
        """Sends the function call request to the child process and waits for the response."""
        # Check if process is still alive
        if self.process.poll() is not None:
            raise Exception("Child process is not running")
        
        request_id = str(uuid.uuid4())
        result = None
        error = None
        event = threading.Event()

        def callback(res, err):
            nonlocal result, error
            result = res
            error = err
            event.set()

        with self.lock:
            self.pending_requests[request_id] = callback

        # Send request to child process
        request = {
            "__app__": self.APP_NAME,
            "__id__": request_id,
            "__function__": func_name,
            "__args__": args,
            "__kwargs__": kwargs
        }

        try:
            self.process.stdin.write(json.dumps(request) + "\n")
            self.process.stdin.flush()
        except BrokenPipeError:
            with self.lock:
                if request_id in self.pending_requests:
                    del self.pending_requests[request_id]
            raise Exception("Child process has terminated")

        # Wait for response
        # Wait for response with optional timeout
        event_is_set = event.wait(timeout=timeout)
        if not event_is_set:
            with self.lock:
                if request_id in self.pending_requests:
                    del self.pending_requests[request_id]
            raise TimeoutError(f"Function '{func_name}' timed out after {timeout} seconds")


        if error:
            raise RemoteCallError(error)
        return result

    def _read_responses(self):
        while self.running:
            try:
                response = self.process.stdout.readline()
                if not response:
                    print("End of stdout stream reached")
                    break
                    
                try:
                    data = json.loads(response)
                    is_app = data.get("__app__")
                    request_id = data.get("__id__")

                    if is_app == self.APP_NAME:
                        with self.lock:
                            if request_id in self.pending_requests:
                                callback = self.pending_requests.pop(request_id)
                                if "__error__" in data:
                                    callback(None, data["__error__"])
                                else:
                                    callback(data["__result__"], None)
                    else:
                        # Regular stdout output
                        print(f"[Child STDOUT] {response.strip()}")
                except json.JSONDecodeError:
                    # Not JSON, just regular output
                    print(f"[{self.script_name} STDOUT] {response.strip()}")
            except Exception as e:
                print(f"Error reading response from {self.script_name}: {e}")
                # Check if we're still supposed to be running
                if not self.running or self.process.poll() is not None:
                    break

    def _read_stderr(self) -> None:
        """Reads standard error from the child process."""
        while self.running and self.process and self.process.poll() is None:
            try:
                line = self.process.stderr.readline()
                if not line:
                    break
                print(f"[{self.script_name} STDERR] {line.strip()}", file=sys.stderr)
            except Exception as e:
                print(f"Error reading stderr from {self.script_name}: {e}", file=sys.stderr)
                if not self.running:
                    break

    def _monitor_child_process(self):
        restart_count = 0
        max_restart_attempts = self.max_restart_attempts or 5
        while self.running:
            if self.process and self.process.poll() is not None:
                # Child has exited, notify all
                with self.lock:
                    for callback in self.pending_requests.values():
                        callback(None, "Child process terminated unexpectedly")
                    self.pending_requests = {}
                    
                if getattr(self, "auto_restart", False) and restart_count < max_restart_attempts:
                    try:
                        restart_count += 1
                        print(f"Restarting worker process (attempt {restart_count}/{max_restart_attempts})...")
                        time.sleep(1)  # Add delay before restart
                        # Don't call self.start() directly - just recreate the process
                        self.process = subprocess.Popen(
                            [self.executable, self.script_path],
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            bufsize=1, universal_newlines=True
                        )
                    except Exception as e:
                        print(f"Auto-restart failed: {e}")
                        break
                else:
                    if restart_count >= max_restart_attempts:
                        print("Max restart attempts reached")
                    break
            time.sleep(0.1)

    def close(self):
        """Closes the child process and releases resources."""
        self.running = False
        
        # Notify any pending requests
        with self.lock:
            for callback in self.pending_requests.values():
                callback(None, "Worker controller is shutting down")
            self.pending_requests = {}
        
        try:
            if hasattr(self, 'thread_pool'):
                self.thread_pool.shutdown(wait=False)
                
            if self.process:
                self.process.terminate()
                try:
                    self.process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    print("Process did not terminate cleanly, killing...")
                    self.process.kill()
        except Exception as e:
            print(f"Error during shutdown: {e}")
            if self.process:
                try:
                    self.process.kill()
                except:
                    pass

    async def aclose(self):
        """Asynchronous version of close."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.close)

    def __enter__(self): 
        return self
    def __del__(self):
        self.close()
    def __exit__(self, exc_type, exc_val, tb): 
        self.close()



# child base class controller
class ChildWorker:
    """Base class for creating IPC-enabled worker scripts."""
    
    def __init__(self):
        self.APP_NAME = "comlink_py"
        self._running = True
        # For CPU-bound operations in async functions
        self._thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=10)
        
    def _start(self):
        """Start processing commands from stdin."""
        while self._running:
            try:
                # Read command from parent process
                line = sys.stdin.readline()
                if not line:
                    break
                
                # Parse command
                request = json.loads(line)
                is_app = request.get('__app__')

                # if not out request then skip
                if is_app != self.APP_NAME:
                    continue
                request_id = request.get("__id__")
                func_name = request.get("__function__")
                args = request.get("__args__", [])
                kwargs = request.get("__kwargs__", {})
                
                # Find and call the function
                try:
                    func = getattr(self, func_name)
                    
                    # Check if the function is a coroutine (async def)
                    if asyncio.iscoroutinefunction(func):
                        # For async functions, run them in an event loop
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        result = loop.run_until_complete(func(*args, **kwargs))
                        loop.close()
                    else:
                        # For regular functions
                        result = func(*args, **kwargs)
                    
                    # Send successful response
                    response = {
                        "__app__": self.APP_NAME,
                        "__id__": request_id,
                        "__result__": result
                    }
                except Exception as e:
                    # Send error response
                    error_msg = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
                    response = {
                        "__app__": self.APP_NAME,
                        "__id__": request_id,
                        "__error__": error_msg
                    }
                
                # Send response to parent
                sys.stdout.write(json.dumps(response) + "\n")
                sys.stdout.flush()
                
            except Exception as e:
                # Handle errors in the main loop
                sys.stderr.write(f"Error in IPC loop: {e}\n")
                sys.stderr.flush()
    
    def _stop(self):
        """Stop processing commands."""
        self._running = False
        if hasattr(self, '_thread_pool'):
            self._thread_pool.shutdown(wait=False)


    def list_functions(self):
        """List available callable public methods (excluding private and base ones)."""
        excluded = set(dir(ChildWorker))  # Exclude inherited/internal methods
        return [
            name for name in dir(self)
            if callable(getattr(self, name))
            and not name.startswith("_")
            and name not in excluded
        ]

            
    async def run_in_thread(self, func, *args, **kwargs):
        """Helper to run CPU-bound operations in a thread pool."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._thread_pool,
            lambda: func(*args, **kwargs)
        )
    
    def _handle_signals(self):
        """Setup signal handlers for graceful shutdown."""
        import signal
        
        def signal_handler(sig, frame):
            print(f"Received signal {sig}, shutting down...")
            self._stop()
            
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def __enter__(self):
        self._start()
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        self._stop()

    def run(self):
        """Run the worker with signal handling."""
        self._handle_signals()
        try:
            self._start()
        finally:
            self._stop()


