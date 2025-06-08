import * as zmq from "zeromq";
import { spawn, ChildProcess } from "child_process";
import { randomUUID } from "crypto";
import * as net from "net";
import * as msgpack from "msgpackr";

const APP_NAME = "comlink_ipc_v2";

export enum MessageType {
    CALL = "call",
    RESPONSE = "response",
    ERROR = "error",
    STDOUT = "stdout",
    STDERR = "stderr",
    HEARTBEAT = "heartbeat",
    SHUTDOWN = "shutdown",
}

export interface ComlinkMessageData {
    app: string;
    id: string;
    type: string;
    timestamp: number;
    function?: string;
    args?: any[];
    namespace?: string;
    result?: any;
    error?: string;
    output?: string;
}

export class ComlinkMessage {
    public app: string = APP_NAME;
    public id: string;
    public type: string;
    public timestamp: number;
    public function?: string;
    public args?: any[];
    public namespace?: string;
    public result?: any;
    public error?: string;
    public output?: string;

    constructor(data: Partial<ComlinkMessageData> = {}) {
        this.id = data.id || randomUUID();
        this.type = data.type || "";
        this.timestamp = data.timestamp || Date.now() / 1000;

        Object.assign(this, data);
    }

    static createCall(
        functionName: string,
        args: any[] = [],
        namespace: string = "default",
        msgId?: string,
    ): ComlinkMessage {
        return new ComlinkMessage({
            type: MessageType.CALL,
            id: msgId || randomUUID(),
            function: functionName,
            args,
            namespace,
        });
    }

    static createResponse(result: any, msgId: string): ComlinkMessage {
        return new ComlinkMessage({
            type: MessageType.RESPONSE,
            id: msgId,
            result,
        });
    }

    static createError(error: string, msgId: string): ComlinkMessage {
        return new ComlinkMessage({
            type: MessageType.ERROR,
            id: msgId,
            error,
        });
    }

    static createOutput(output: string, msgType: MessageType): ComlinkMessage {
        return new ComlinkMessage({
            type: msgType,
            output,
        });
    }

    toDict(): ComlinkMessageData {
        const result: ComlinkMessageData = {
            app: this.app,
            id: this.id,
            type: this.type,
            timestamp: this.timestamp,
        };

        if (this.function !== undefined) result.function = this.function;
        if (this.args !== undefined) result.args = this.args;
        if (this.namespace !== undefined) result.namespace = this.namespace;
        if (this.result !== undefined) result.result = this.result;
        if (this.error !== undefined) result.error = this.error;
        if (this.output !== undefined) result.output = this.output;

        return result;
    }

    pack(): Buffer {
        return msgpack.encode(this.toDict());
    }

    static unpack(data: Buffer): ComlinkMessage {
        try {
            const decoded = msgpack.decode(data);
            return new ComlinkMessage(decoded);
        } catch (error) {
            throw new Error(`Failed to unpack message: ${error}`);
        }
    }
}

export class RemoteCallError extends Error {
    constructor(message: string) {
        super(message);
        this.name = "RemoteCallError";
    }
}

interface PendingRequest {
    resolve: (value: any) => void;
    reject: (error: Error) => void;
    timeout?: NodeJS.Timeout;
}

export class ParentWorker {
    private readonly scriptPath: string;
    private readonly executable: string;
    private readonly port: number;
    private socket?: zmq.Pair;
    private process?: ChildProcess;
    private running: boolean = false;
    private readonly pendingRequests: Map<string, PendingRequest> = new Map();

    public readonly acall: AsyncRemoteProxy;

    constructor(scriptPath: string, executable: string = "node", port?: number) {
        this.scriptPath = scriptPath;
        this.executable = executable;
        this.port = port || this.findFreePort();
        this.acall = new AsyncRemoteProxy(this);
    }

    private findFreePort(): number {
        const server = net.createServer();
        server.listen(0);
        const address = server.address();
        const port = address && typeof address === "object" ? address.port : 5555;
        server.close();
        return port;
    }

    async start(): Promise<void> {
        // Setup ZeroMQ socket
        this.socket = new zmq.Pair();
        await this.socket.bind(`tcp://*:${this.port}`);

        // Start child process
        const env = { ...process.env, COMLINK_ZMQ_PORT: this.port.toString() };
        this.process = spawn(this.executable, [this.scriptPath], { env, shell: true });

        // Check if process started successfully
        await new Promise<void>((resolve, reject) => {
            const timeout = setTimeout(() => {
                if (this.process?.exitCode !== null) {
                    reject(new Error(`Child process failed to start (exit code: ${this.process?.exitCode})`));
                } else {
                    resolve();
                }
            }, 1000);

            this.process!.on("exit", code => {
                clearTimeout(timeout);
                if (code !== null && code !== 0) {
                    reject(new Error(`Child process exited with code ${code}`));
                }
            });

            // If we reach here after timeout, process is running
            setTimeout(() => {
                clearTimeout(timeout);
                resolve();
            }, 500);
        });

        this.running = true;
        this.startMessageLoop();
    }

    private async startMessageLoop(): Promise<void> {
        if (!this.socket) return;

        for await (const [message] of this.socket) {
            if (!this.running) break;

            try {
                const comlinkMessage = ComlinkMessage.unpack(message);
                await this.handleMessage(comlinkMessage);
            } catch (error) {
                console.error("Failed to process message:", error);
            }
        }
    }

    private async handleMessage(message: ComlinkMessage): Promise<void> {
        // Validate message
        if (message.app !== APP_NAME || !message.id) {
            console.warn("Ignoring invalid message");
            return;
        }

        if (message.type === MessageType.RESPONSE || message.type === MessageType.ERROR) {
            const pending = this.pendingRequests.get(message.id);
            if (pending) {
                this.pendingRequests.delete(message.id);
                if (pending.timeout) clearTimeout(pending.timeout);

                if (message.type === MessageType.RESPONSE) {
                    pending.resolve(message.result);
                } else {
                    pending.reject(new RemoteCallError(message.error || "Unknown error"));
                }
            }
        } else if (message.type === MessageType.STDOUT) {
            if (message.output) {
                console.log(`[${this.scriptPath} STDOUT]:`, message.output);
            }
        } else if (message.type === MessageType.STDERR) {
            if (message.output) {
                console.error(`[${this.scriptPath} STDERR]:`, message.output);
            }
        }
    }

    async callFunction(
        functionName: string,
        args: any[] = [],
        timeout?: number,
        namespace: string = "default",
    ): Promise<any> {
        if (!this.running || !this.socket) {
            throw new Error("Child process is not running");
        }

        const requestId = randomUUID();
        const message = ComlinkMessage.createCall(functionName, args, namespace, requestId);

        return new Promise((resolve, reject) => {
            let timeoutHandle: NodeJS.Timeout | undefined;

            if (timeout) {
                timeoutHandle = setTimeout(() => {
                    this.pendingRequests.delete(requestId);
                    reject(new Error(`Function '${functionName}' timed out after ${timeout}ms`));
                }, timeout);
            }

            this.pendingRequests.set(requestId, {
                resolve,
                reject,
                timeout: timeoutHandle,
            });

            // Send message
            this.socket!.send(message.pack()).catch(error => {
                this.pendingRequests.delete(requestId);
                if (timeoutHandle) clearTimeout(timeoutHandle);
                reject(new Error(`Failed to send request: ${error}`));
            });
        });
    }

    async close(): Promise<void> {
        this.running = false;

        // Cancel all pending requests
        for (const [id, pending] of this.pendingRequests) {
            if (pending.timeout) clearTimeout(pending.timeout);
            pending.reject(new Error("Worker controller is shutting down"));
        }
        this.pendingRequests.clear();

        // Close socket
        if (this.socket) {
            this.socket.close();
            // Add a small delay to let zmq clean up
            await new Promise(resolve => setTimeout(resolve, 100));
        }

        // Terminate child process
        if (this.process) {
            this.process.kill("SIGTERM");

            // Give it a moment to exit gracefully
            await new Promise<void>(resolve => {
                const timeout = setTimeout(() => {
                    this.process?.kill("SIGKILL");
                    resolve();
                }, 2000);

                this.process!.on("exit", () => {
                    clearTimeout(timeout);
                    resolve();
                });
            });
        }
    }
}

class AsyncRemoteProxy {
    private _controller: ParentWorker;
    private _pendingOptions: { timeout?: number; namespace?: string } = {};

    constructor(controller: ParentWorker) {
        this._controller = controller;

        // Return a Proxy that intercepts all property access
        return new Proxy(this, {
            get(target, prop: string) {
                if (prop === "withOptions") {
                    return (options: { timeout?: number; namespace?: string }) => {
                        target._pendingOptions = options;
                        return target; // Return the same proxy for chaining
                    };
                }

                // For any other property, return a remote method function
                return (...args: any[]) => {
                    // Use pending options then clear them
                    const timeout = target._pendingOptions.timeout;
                    const namespace = target._pendingOptions.namespace || "default";
                    target._pendingOptions = {}; // Reset after use

                    return target._controller.callFunction(prop, args, timeout, namespace);
                };
            },
        });
    }
}

export abstract class ChildWorker {
    protected namespace: string = "default";
    private running: boolean = true;
    private socket?: zmq.Pair;
    private port?: number;

    constructor() {
        // Setup ZMQ connection
        this.setupZmq();
    }

    private setupZmq(): void {
        const portStr = process.env.COMLINK_ZMQ_PORT || "5555";
        this.port = parseInt(portStr);

        if (isNaN(this.port) || this.port < 1024 || this.port > 65535) {
            console.error(`FATAL: Invalid port '${portStr}'`);
            process.exit(1);
        }

        console.error(`DEBUG: Child connecting to parent on port ${this.port}...`);
    }

    async start(): Promise<void> {
        try {
            this.socket = new zmq.Pair();
            await this.socket.connect(`tcp://localhost:${this.port}`);
            console.error(`DEBUG: Connected to tcp://localhost:${this.port}`);

            // Redirect stdout/stderr (simplified - just log to parent)
            const originalConsoleLog = console.log;
            const originalConsoleError = console.error;

            console.log = (...args) => {
                this.sendOutput(MessageType.STDOUT, args.join(" "));
            };

            console.error = (...args) => {
                originalConsoleError(...args); // Still log locally
                this.sendOutput(MessageType.STDERR, args.join(" "));
            };

            await this.messageLoop();
        } catch (error) {
            console.error(`FATAL: ZMQ connection failed: ${error}`);
            process.exit(1);
        }
    }

    private sendOutput(msgType: MessageType, output: string): void {
        if (this.socket) {
            const message = ComlinkMessage.createOutput(output, msgType);
            this.socket.send(message.pack()).catch(() => {
                // Ignore send failures for output
            });
        }
    }

    private async messageLoop(): Promise<void> {
        if (!this.socket) return;

        for await (const [messageData] of this.socket) {
            if (!this.running) break;

            try {
                const message = ComlinkMessage.unpack(messageData);
                // console.error(`DEBUG: Received message: ${JSON.stringify(message.toDict())}`);

                // Validate message
                if (message.app !== APP_NAME) {
                    console.error(`WARNING: Ignoring message from wrong app: ${message.app}`);
                    continue;
                }

                if (message.namespace && message.namespace !== this.namespace) {
                    console.error(`WARNING: Ignoring message for wrong namespace: ${message.namespace}`);
                    continue;
                }

                if (message.type === MessageType.CALL) {
                    await this.handleFunctionCall(message);
                } else if (message.type === MessageType.SHUTDOWN) {
                    console.error("Received shutdown signal");
                    this.running = false;
                    break;
                }
            } catch (error) {
                console.error("ERROR: Failed to process message:", error);
            }
        }
    }

    private async handleFunctionCall(message: ComlinkMessage): Promise<void> {
        let response: ComlinkMessage;

        try {
            if (!message.function || !message.id) {
                throw new Error("Message missing 'function' or 'id' field");
            }

            const args = message.args || [];

            // Check if function exists and is callable
            const func = (this as any)[message.function];
            if (typeof func !== "function") {
                throw new Error(`Function '${message.function}' not found or not callable`);
            }

            // Check if it's a private method
            if (message.function.startsWith("_")) {
                throw new Error(`Cannot call private method '${message.function}'`);
            }

            // Call the function
            const result = await func.apply(this, args);
            response = ComlinkMessage.createResponse(result, message.id);
        } catch (error) {
            const errorMsg = error instanceof Error ? error.message : String(error);
            console.error(`ERROR: Function call failed: ${errorMsg}`);
            console.error(`DEBUG: Function: ${message.function}`);
            console.error(`DEBUG: Args: ${JSON.stringify(message.args)}`);

            response = ComlinkMessage.createError(errorMsg, message.id);
        }

        // Send response
        try {
            if (this.socket) {
                await this.socket.send(response.pack());
            }
        } catch (error) {
            console.error(`CRITICAL: Failed to send response: ${error}`);
        }
    }

    listFunctions(): string[] {
        const excluded = new Set(Object.getOwnPropertyNames(ChildWorker.prototype));
        return Object.getOwnPropertyNames(Object.getPrototypeOf(this)).filter(
            name => typeof (this as any)[name] === "function" && !name.startsWith("_") && !excluded.has(name),
        );
    }

    stop(): void {
        this.running = false;
        if (this.socket) {
            this.socket.close();
        }
    }

    async run(): Promise<void> {
        // Setup signal handlers
        process.on("SIGINT", () => {
            console.error("Received SIGINT, shutting down...");
            this.stop();
        });

        process.on("SIGTERM", () => {
            console.error("Received SIGTERM, shutting down...");
            this.stop();
        });

        try {
            await this.start();
        } finally {
            this.stop();
        }
    }
}
