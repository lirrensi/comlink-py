import zmq
import msgpack
import sys
import uuid
import time
import asyncio
import traceback
import os
import signal
import subprocess
from dataclasses import dataclass, field
import threading
import queue
import atexit
import concurrent.futures
from typing import Dict, Any, Optional, Union, List, Callable
from enum import Enum
from concurrent.futures import ThreadPoolExecutor

APP_NAME = 'comlink_ipc_v2'

class MessageType(Enum):
    """Standardized message types for ZeroMQ communication."""
    CALL = "call"
    RESPONSE = "response"
    ERROR = "error"
    STDOUT = "stdout"
    STDERR = "stderr"
    HEARTBEAT = "heartbeat"
    SHUTDOWN = "shutdown"


class ComlinkMessage:
    """
    Message container for ZeroMQ communication.
    
    Core fields (always present):
    - app: str = APP_NAME
    - id: str = unique UUID
    - type: str = "call"|"response"|"error"|"stdout"|"stderr"|"heartbeat"|"shutdown"  
    - timestamp: float = unix timestamp
    
    Call message fields:
    - function: str = function name to call
    - params: List[Dict] = serialized args/kwargs [{'i': index/key, 'd': data}, ...]
    - namespace: str = namespace (default: 'default')
    
    Response message fields:
    - result: Any = return value from function
    
    Error message fields:
    - error: str = error message/traceback
    
    Output message fields:
    - output: str = stdout/stderr content
    """
    
    def __init__(self, **kwargs):
        # Set defaults
        self.app = APP_NAME
        self.id = str(uuid.uuid4())
        self.type = None
        self.timestamp = time.time()
        
        # Override with any provided values
        for key, value in kwargs.items():
            setattr(self, key, value)
    
    @classmethod
    def create_call(cls, function: str, args: tuple = (), kwargs: dict = None,
                   namespace: str = 'default', msg_id: str = None):
        """Create a function call message with automatic parameter serialization."""
        args = args or []
        
        return cls(
            type=MessageType.CALL.value,
            id=msg_id or str(uuid.uuid4()),
            function=function,
            args=args,
            namespace=namespace
        )
    
    @classmethod
    def create_response(cls, result: Any, msg_id: str):
        """Create a response message."""
        return cls(type=MessageType.RESPONSE.value, id=msg_id, result=result)
    
    @classmethod
    def create_error(cls, error: str, msg_id: str):
        """Create an error message."""
        return cls(type=MessageType.ERROR.value, id=msg_id, error=error)
    
    @classmethod
    def create_output(cls, output: str, msg_type: MessageType):
        """Create stdout/stderr output message."""
        return cls(type=msg_type.value, output=output)
    
    def extract_call_args(self):
        """Extract args from simple list format."""
        if hasattr(self, 'args'):
            return tuple(self.args)
        return tuple()  # Return empty tuple instead of args, kwargs
    
    def to_dict(self) -> dict:
        """Convert to plain dict."""
        return {key: value for key, value in self.__dict__.items() 
                if not key.startswith('_')}
    
    def pack(self) -> bytes:
        """Pack message for ZeroMQ transmission."""
        return msgpack.packb(self.to_dict())
    
    @classmethod
    def unpack(cls, data: bytes) -> 'ComlinkMessage':
        """Unpack message from ZeroMQ transmission with validation."""
        try:
            # Validate input
            if not isinstance(data, bytes):
                raise ValueError(f"Expected bytes, got {type(data)}")
                
            if len(data) == 0:
                raise ValueError("Empty message data")
                
            # Unpack with msgpack
            unpacked = msgpack.unpackb(data, raw=False)
            
            # Validate it's a dict
            if not isinstance(unpacked, dict):
                raise ValueError(f"Expected dict from msgpack, got {type(unpacked)}")
            
            # Create message object
            return cls(**unpacked)
            
        except msgpack.exceptions.ExtraData as e:
            raise msgpack.exceptions.ExtraData(f"Message contains extra data: {e}")
        except msgpack.exceptions.UnpackException as e:
            raise msgpack.exceptions.UnpackException(f"Failed to unpack message: {e}")
        except Exception as e:
            raise ValueError(f"Failed to create ComlinkMessage: {e}")


class RemoteCallError(Exception):
    """Exception for failures that occurred on the remote worker/child."""



# Parent controller 
class ParentWorker:
    @dataclass
    class CallbackInfo:
        callback: callable
        is_async: bool
        timeout: Optional[float] = None
        created_at: float = field(default_factory=time.time)

    def __init__(self, script_path, executable=None, port=None, thread_pool_size=10, 
                 auto_restart=False, max_restart_attempts=5):
        self.APP_NAME = APP_NAME
        self.script_path = script_path
        self.script_name = script_path.split('/')[-1]
        self.executable = executable if executable else sys.executable
        self.auto_restart = auto_restart
        self.max_restart_attempts = max_restart_attempts
        self.restart_count = 0
        self.startup_failed = False
        self.port = port or self._find_free_port()


        self.pending_requests = {}
        self.lock = threading.Lock()
        self.thread_pool = ThreadPoolExecutor(max_workers=thread_pool_size)
        
        # ZeroMQ setup
        self.context = zmq.Context()
        self.socket = None
        self.process = None
        self.running = False
        
        # Single thread for ZeroMQ communication
        self.zmq_thread = None
        
        # Proxy-like method handler
        self.call = self.create_sync_proxy()
        self.acall = self.create_async_proxy()

        # Register cleanup on exit
        self._closed = False
        atexit.register(self._emergency_cleanup)
        # Also handle common signals
        signal.signal(signal.SIGINT, self._signal_handler)   # Ctrl+C
        signal.signal(signal.SIGTERM, self._signal_handler)  # Termination


    def _find_free_port(self):
        """Find a free port for ZeroMQ communication."""
        import socket
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('', 0))
            s.listen(1)
            port = s.getsockname()[1]
        return port

    def start(self):
        """Start the parent worker and child process."""
        # Setup ZeroMQ socket
        self._setup_socket()
        
        # Start child process
        env = os.environ.copy()
        env['COMLINK_ZMQ_PORT'] = str(self.port)
        
        self.process = subprocess.Popen(
            [self.executable, self.script_path],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        # Check if process started successfully
        time.sleep(0.5)  # Give it a moment
        if self.process.poll() is not None:
            # Process already died
            self.startup_failed = True
            raise Exception(f"Child process failed to start (exit code: {self.process.returncode})")
        
        self.running = True
        
        # Single thread for all ZeroMQ operations (messages, stdout, stderr, monitoring)
        self.zmq_thread = threading.Thread(target=self._zmq_event_loop)
        self.zmq_thread.daemon = True
        self.zmq_thread.start()
        
        # Wait a bit for child to connect
        time.sleep(1)

    async def astart(self):
        """Async version of start."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.start)


    def _setup_socket(self):
        try:
            self.context = zmq.Context()
            self.socket = self.context.socket(zmq.PAIR)
            
            endpoint = f"tcp://*:{self.port}"
            self.socket.bind(endpoint)
            
            # Set timeouts
            self.socket.setsockopt(zmq.RCVTIMEO, 100)
            self.socket.setsockopt(zmq.SNDTIMEO, 100)
            
        except zmq.ZMQError as e:
            if e.errno == zmq.EADDRINUSE:
                raise Exception(f"Port {self.port} is already in use")
            raise Exception(f"Failed to setup ZMQ socket: {e}")


    def create_sync_proxy(self):
        """Creates a sync proxy for calling remote methods synchronously."""
        class SyncRemoteProxy:
            def __init__(self, controller):
                self._controller = controller
                self._pending_options = {}

            def withOptions(self, **options):
                """Configure options for next method call."""
                self._pending_options = options
                return self

            def __getattr__(self, name):
                def remote_method(*args, **kwargs):
                    if (len(kwargs)):
                        print(f'Passed kwargs into function call {name}, will not be accessible on the other side!')
                    # Use pending options then clear them
                    timeout = self._pending_options.get('timeout')
                    namespace = self._pending_options.get('namespace', 'default')
                    self._pending_options = {}  # Reset after use
                    
                    return self._controller._call_function(name, args, timeout, namespace)
                return remote_method
        
        return SyncRemoteProxy(self)

    def create_async_proxy(self):
        class AsyncRemoteProxy:
            def __init__(self, controller):
                self._controller = controller
                self._pending_options = {}

            def withOptions(self, **options):
                """Configure options for next method call."""
                self._pending_options = options
                return self

            def __getattr__(self, name):
                def remote_method(*args, **kwargs):
                    if (len(kwargs)):
                        print(f'Passed kwargs into function call {name}, will not be accessible on the other side!')
                    # Use pending options then clear them
                    timeout = self._pending_options.get('timeout')
                    namespace = self._pending_options.get('namespace', 'default')
                    self._pending_options = {}  # Reset after use
                    
                    return self._controller._acall_function(name, args, timeout, namespace)
                return remote_method
        return AsyncRemoteProxy(self)
    

    def _call_function(self, func_name: str, args: tuple,
                      timeout: Optional[float] = None, namespace: str = 'default'):
        
        
        if not self.running or self.process.poll() is not None:
            raise Exception("Child process is not running")
        
        request_id = str(uuid.uuid4())
        result_container = {'result': None, 'error': None, 'done': False}
        event = threading.Event()

        def sync_callback(res, err):
            result_container['result'] = res
            result_container['error'] = err
            result_container['done'] = True
            event.set()

        # Store callback
        callback_info = self.CallbackInfo(sync_callback, is_async=False, timeout=timeout)
        
        with self.lock:
            self.pending_requests[request_id] = callback_info

        # Create and send message DIRECTLY
        message = ComlinkMessage.create_call(
            function=func_name, args=args, namespace=namespace, msg_id=request_id
        )

        try:
            # : Direct send, no queue
            with self.lock:  # Protect socket from concurrent access
                self.socket.send(message.pack(), zmq.NOBLOCK)
        except zmq.Again:
            with self.lock:
                self.pending_requests.pop(request_id, None)
            raise Exception("Failed to send request: socket busy")
        except Exception as e:
            with self.lock:
                self.pending_requests.pop(request_id, None)
            raise Exception(f"Failed to send request: {e}")

        # Wait for response
        if not event.wait(timeout=timeout):
            with self.lock:
                self.pending_requests.pop(request_id, None)
            raise TimeoutError(f"Function '{func_name}' timed out")

        if result_container['error']:
            raise RemoteCallError(result_container['error'])
        return result_container['result']


    async def _acall_function(self, func_name, args, timeout=None, namespace='default'):
        
        if not self.running or self.process.poll() is not None:
            raise Exception("Child process is not running")
        
        request_id = str(uuid.uuid4())
        future = asyncio.Future()
        
        def async_callback(res, err):
            if not future.done():
                loop = asyncio.get_event_loop()
                if err:
                    loop.call_soon_threadsafe(future.set_exception, RemoteCallError(err))
                else:
                    loop.call_soon_threadsafe(future.set_result, res)
        
        callback_info = self.CallbackInfo(async_callback, is_async=True, timeout=timeout)
        
        with self.lock:
            self.pending_requests[request_id] = callback_info
        
        message = ComlinkMessage.create_call(
            function=func_name, args=args, namespace=namespace, msg_id=request_id
        )
        
        try:
            # SIMPLIFIED: Direct send, no queue
            with self.lock:
                self.socket.send(message.pack(), zmq.NOBLOCK)
        except Exception as e:
            with self.lock:
                self.pending_requests.pop(request_id, None)
            raise Exception(f"Failed to send request: {e}")
        
        try:
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            with self.lock:
                self.pending_requests.pop(request_id, None)
            raise TimeoutError(f"Function '{func_name}' timed out")

    def _handle_message(self, message_data):
        
        try:
            message = ComlinkMessage.unpack(message_data)
            
            # Basic validation
            if not hasattr(message, 'app') or message.app != self.APP_NAME:
                return
            if not hasattr(message, 'id') or not message.id:
                return
            
            # Handle different message types
            if message.type in [MessageType.RESPONSE.value, MessageType.ERROR.value]:
                self._handle_response(message)
            elif message.type == MessageType.STDOUT.value:
                output = getattr(message, 'output', '')
                if output:
                    print(f"[{self.script_name} STDOUT]: {output}")
            elif message.type == MessageType.STDERR.value:
                output = getattr(message, 'output', '')
                if output:
                    print(f"[{self.script_name} STDERR]: {output}", file=sys.stderr)
                        
        except Exception as e:
            print(f"ERROR: Failed to process message: {e}")


    def _handle_response(self, message):
        
        # Get callback info
        with self.lock:
            callback_info = self.pending_requests.pop(message.id, None)
        
        if not callback_info:
            return  # Unknown request ID
        
        # Prepare result/error
        if message.type == MessageType.RESPONSE.value:
            result = getattr(message, 'result', None)
            error = None
        else:  # ERROR
            result = None
            error = getattr(message, 'error', 'Unknown error')
        
        # Execute callback based on type
        try:
            if callback_info.is_async:
                # Async: callback handles event loop scheduling
                callback_info.callback(result, error)
            else:
                # Sync: callback just sets event, execute directly
                callback_info.callback(result, error)
        except Exception as e:
            print(f"ERROR: Callback execution failed: {e}")


    def _zmq_event_loop(self):
        while self.running:
            try:
                # SIMPLIFIED: Direct receive with timeout
                try:
                    message_data = self.socket.recv(zmq.NOBLOCK)
                    self._handle_message(message_data)
                except zmq.Again:
                    # No message available
                    pass
                
                # Check child process (fast check)
                if self.process and self.process.poll() is not None:
                    self._handle_child_exit()
                    break
                
                # SIMPLIFIED: Fixed small sleep instead of complex timing
                time.sleep(0.01)
                    
            except Exception as e:
                if self.running:
                    print(f"Error in ZMQ event loop: {e}")
                break


    def _handle_zmq_messages(self):
        """Simplified - we know there's a message waiting"""
        try:

            message_data = self.socket.recv(zmq.NOBLOCK)
            
            try:
                message = ComlinkMessage.unpack(message_data)
                
                # Validate message
                if not hasattr(message, 'app') or message.app != self.APP_NAME:
                    print(f"WARNING: Ignoring message from unknown app")
                    return
                    
                if not hasattr(message, 'id') or not message.id:
                    print(f"WARNING: Ignoring message without ID")
                    return
                
                # Handle responses and errors
                if message.type in [MessageType.RESPONSE.value, MessageType.ERROR.value]:
                    self._handle_callback_message(message)
                elif message.type == MessageType.STDOUT.value:
                    output = getattr(message, 'output', '')
                    if output:
                        print(f"[{self.script_name} STDOUT]: {output}")
                elif message.type == MessageType.STDERR.value:
                    output = getattr(message, 'output', '')
                    if output:
                        print(f"[{self.script_name} STDERR]: {output}", file=sys.stderr)
                        
            except Exception as e:
                print(f"ERROR: Failed to process message: {e}")
        
        except zmq.Again:
            # Poller said there was data but there isn't - rare but possible
            pass
        except Exception as e:
            print(f"ERROR: Failed to receive ZMQ message: {e}")


    def _handle_callback_message(self, message):
        """Handle callback execution based on sync/async mode."""
        callback_info = None
        
        # Remove from pending requests
        with self.lock:
            callback_info = self.pending_requests.pop(message.id, None)
        
        if not callback_info:
            print(f"WARNING: Received response for unknown request ID: {message.id}")
            return
        
        # Prepare callback parameters
        if message.type == MessageType.RESPONSE.value:
            result = getattr(message, 'result', None)
            error = None
        else:  # ERROR
            result = None
            error = getattr(message, 'error', 'Unknown error')
        
        # Execute callback based on mode
        if callback_info.is_async:
            # ASYNC MODE: Execute callback immediately (it schedules on event loop)
            try:
                callback_info.callback(result, error)
            except Exception as e:
                print(f"ERROR: Async callback failed: {e}")
        else:
            # SYNC MODE: Execute callback in thread pool to avoid blocking ZMQ
            try:
                # Submit to thread pool but don't wait - fire and forget
                # The sync callback will just set an event, so it's fast
                future = self.callback_thread_pool.submit(
                    self._safe_callback_execution, 
                    callback_info.callback, 
                    result, 
                    error
                )
                # Don't wait for result - let it execute in background
            except Exception as e:
                print(f"ERROR: Failed to submit sync callback: {e}")
                # Fallback: execute directly (risky but better than losing the response)
                try:
                    callback_info.callback(result, error)
                except Exception as callback_error:
                    print(f"ERROR: Fallback callback execution failed: {callback_error}")


    def _safe_callback_execution(self, callback, result, error):
        """Safely execute callback with error handling."""
        try:
            callback(result, error)
        except Exception as e:
            print(f"ERROR: Callback execution failed: {e}")
            # Don't re-raise - we don't want to crash the thread pool
    
    def _handle_child_exit(self):
        
        print(f"Child process exited with code: {self.process.returncode}")
        
        # Notify all pending requests
        with self.lock:
            pending = list(self.pending_requests.items())
            self.pending_requests.clear()
        
        for request_id, callback_info in pending:
            try:
                callback_info.callback(None, "Child process terminated unexpectedly")
            except Exception as e:
                print(f"ERROR: Failed to notify pending request {request_id}: {e}")
        
        # Handle restart
        if self.auto_restart and self.restart_count < self.max_restart_attempts:
            self._attempt_restart()
        else:
            self.running = False

    def _attempt_restart(self):
        
        try:
            self.restart_count += 1
            print(f"Restarting worker (attempt {self.restart_count}/{self.max_restart_attempts})...")
            
            time.sleep(1)  # Brief pause
            
            # Start new process
            env = os.environ.copy()
            env['COMLINK_ZMQ_PORT'] = str(self.port)
            
            self.process = subprocess.Popen(
                [self.executable, self.script_path],
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            # Quick health check
            time.sleep(0.5)
            if self.process.poll() is not None:
                raise Exception(f"Restart failed (exit code: {self.process.returncode})")
            
            print("Worker restarted successfully")
            self.restart_count = 0  # Reset on success
            
        except Exception as e:
            print(f"Auto-restart failed: {e}")
            if self.restart_count >= self.max_restart_attempts:
                print("Max restart attempts reached")
                self.running = False
    


    def close(self):
        
        if self._closed:
            return
        self._closed = True
        self.running = False
        
        # Cancel pending requests
        with self.lock:
            pending = list(self.pending_requests.items())
            self.pending_requests.clear()
        
        for request_id, callback_info in pending:
            try:
                callback_info.callback(None, "Worker shutting down")
            except Exception:
                pass  # Ignore callback errors during shutdown
        
        # Cleanup resources
        try:
            # Wait for ZMQ thread
            if self.zmq_thread and self.zmq_thread.is_alive():
                self.zmq_thread.join(timeout=2)
            
            # Shutdown thread pool
            self.thread_pool.shutdown(wait=False)
                
            # Close socket
            if self.socket:
                self.socket.close()
                
            # Terminate process
            if self.process:
                self.process.terminate()
                try:
                    self.process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=1)
                    
            # Close context
            if self.context:
                self.context.term()
                
        except Exception as e:
            print(f"Error during cleanup: {e}")
    
    async def aclose(self):
        """Asynchronous version of close."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.close)

    def _signal_handler(self, signum, frame):
        """Handle signals by cleaning up and re-raising."""
        print(f"\nReceived signal {signum}, cleaning up...")
        self.close()
        # Re-raise the signal to maintain normal behavior
        signal.default_int_handler(signum, frame)
        
    def _emergency_cleanup(self):
        """Emergency cleanup called by atexit - only if not already closed."""
        if not self._closed:
            print(f"WARNING: {self.script_name} worker was not properly closed, forcing cleanup...")
            self.close()

    def __enter__(self): 
        return self
    def __del__(self):
        self.close()
    def __exit__(self, exc_type, exc_val, tb): 
        self.close()


# Child base class controller
class ChildWorker:
    """Base class for creating ZeroMQ-enabled worker scripts."""
    namespace = 'default'

    def __init__(self):
        self.APP_NAME = APP_NAME
        self._running = True
        self._thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=10)
        
        # ZeroMQ setup - but don't initialize yet!
        self.context = None
        self.socket = None
        self.port = None
        self.original_stdout = None
        self.original_stderr = None

        
    def _setup_io_redirection(self):
        
        class ZMQWriter:
            def __init__(self, worker, msg_type):
                self.worker = worker
                self.msg_type = msg_type

            def write(self, text):
                if text.strip():  # Only send non-empty text
                    self.worker._send_output(self.msg_type, text.rstrip())

            def flush(self):
                pass  # No buffering, nothing to flush

        # Store original stdout/stderr
        self.original_stdout = sys.stdout
        self.original_stderr = sys.stderr
        
        # Replace with ZMQ writers
        sys.stdout = ZMQWriter(self, MessageType.STDOUT)
        sys.stderr = ZMQWriter(self, MessageType.STDERR)

    
    def _send_output(self, msg_type: MessageType, output: str):
        
        if self.socket:
            try:
                message = ComlinkMessage.create_output(output, msg_type)
                self.socket.send(message.pack(), zmq.NOBLOCK)
            except zmq.Again:
                # Socket busy, skip this output (acceptable for stdout/stderr)
                pass
            except Exception:
                # Ignore output send failures - don't break the worker
                pass
        
    def _setup_zmq(self):
        
        try:
            self.context = zmq.Context()
            
            # Get and validate port
            port_str = os.environ.get('COMLINK_ZMQ_PORT', '5555')
            try:
                self.port = int(port_str)
                if not (1024 <= self.port <= 65535):
                    raise ValueError(f"Port {self.port} out of valid range")
            except ValueError as e:
                print(f"FATAL: Invalid port '{port_str}': {e}", file=sys.stderr)
                sys.exit(1)

            # Create and configure socket  
            self.socket = self.context.socket(zmq.PAIR)
            self.socket.setsockopt(zmq.LINGER, 1000)    # 1s linger on close
            self.socket.setsockopt(zmq.SNDTIMEO, 100)   # Send timeout
            
            # Connect to parent
            endpoint = f"tcp://localhost:{self.port}"
            self.socket.connect(endpoint)
            
            # Setup IO redirection AFTER ZMQ is ready
            self._setup_io_redirection()
            
        except zmq.ZMQError as e:
            error_msg = f"FATAL: ZMQ connection to localhost:{self.port} failed: {e}"
            if e.errno == zmq.ECONNREFUSED:
                error_msg += " (Connection refused - is parent running?)"
            print(error_msg, file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"FATAL: Unexpected error connecting to parent: {e}", file=sys.stderr)
            sys.exit(1)



    def _start(self):
        
        if not hasattr(self, '_running'):
            raise RuntimeError(f"{self.__class__.__name__}.__init__() must call super().__init__()")
        
        self._setup_zmq()
        
        while self._running:
            try:
                # Try to receive message with timeout
                try:
                    message_data = self.socket.recv(zmq.NOBLOCK)
                    self._handle_message(message_data)
                except zmq.Again:
                    # No message available, continue
                    pass
                
                # SIMPLIFIED: Fixed small sleep
                time.sleep(0.01)
                    
            except Exception as e:
                print(f"ERROR: Error in message loop: {e}", file=sys.stderr)
                # Continue running unless it's a fatal error
                if "Context was terminated" in str(e):
                    break
    
    def _handle_message(self, message_data):
        
        try:
            message = ComlinkMessage.unpack(message_data)
            
            # Basic validation
            if not hasattr(message, 'app') or message.app != self.APP_NAME:
                return
            
            # Check namespace match
            if hasattr(message, 'namespace') and message.namespace != self.namespace:
                return
            
            # Handle message types
            if message.type == MessageType.CALL.value:
                self._handle_function_call(message)
            elif message.type == MessageType.SHUTDOWN.value:
                self._running = False
                        
        except Exception as e:
            print(f"ERROR: Failed to process message: {e}", file=sys.stderr)
    


    def _handle_function_call(self, message: ComlinkMessage):
        
        response = None
        
        try:
            # Validate message
            if not hasattr(message, 'function') or not message.function:
                raise ValueError("Message missing 'function' field")
            if not hasattr(message, 'id') or not message.id:
                raise ValueError("Message missing 'id' field")
            
            # Extract args
            args = message.extract_call_args()
            
            # Validate function
            if not hasattr(self, message.function):
                raise AttributeError(f"Function '{message.function}' not found")
                
            func = getattr(self, message.function)
            if not callable(func):
                raise AttributeError(f"'{message.function}' is not callable")
                
            if message.function.startswith('_'):
                raise AttributeError(f"Cannot call private method '{message.function}'")
            
            
            if asyncio.iscoroutinefunction(func):
                # Handle async functions
                try:
                    loop = asyncio.get_event_loop()
                except RuntimeError:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                
                result = loop.run_until_complete(func(*args))
            else:
                # Handle sync functions
                result = func(*args)
            
            # Create success response
            response = ComlinkMessage.create_response(result, message.id)
            
        except Exception as e:
            # Create error response
            error_msg = f"{type(e).__name__}: {str(e)}"
            full_error = f"{error_msg}\n{traceback.format_exc()}"
            response = ComlinkMessage.create_error(full_error, message.id)
        
        
        if response:
            try:
                self.socket.send(response.pack(), zmq.NOBLOCK)
            except zmq.Again:
                # Socket busy - try once more after brief pause
                try:
                    time.sleep(0.001)
                    self.socket.send(response.pack(), zmq.NOBLOCK)
                except Exception:
                    print(f"CRITICAL: Failed to send response for {message.id}", file=sys.stderr)
            except Exception as e:
                print(f"CRITICAL: Failed to send response: {e}", file=sys.stderr)



    def _stop(self):
        
        self._running = False
        
        # Restore stdout/stderr
        if self.original_stdout:
            sys.stdout = self.original_stdout
        if self.original_stderr:
            sys.stderr = self.original_stderr
        
        # Cleanup resources
        try:
            if hasattr(self, '_thread_pool'):
                self._thread_pool.shutdown(wait=False)
            if self.socket:
                self.socket.close()
            if self.context:
                self.context.term()
        except Exception as e:
            print(f"Error during cleanup: {e}", file=sys.stderr)


    def list_functions(self):
        """List available callable public methods (excluding private and base ones)."""
        excluded = set(dir(ChildWorker))  # Exclude inherited/internal methods
        return [
            name for name in dir(self)
            if callable(getattr(self, name))
            and not name.startswith("_")
            and name not in excluded
        ]
            
    
    def _handle_signals(self):
        """Setup signal handlers for graceful shutdown."""
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


