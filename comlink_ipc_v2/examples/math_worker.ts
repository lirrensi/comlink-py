#!/usr/bin/env node

import { ChildWorker } from "../comlink_v2";

class MathWorker extends ChildWorker {
    add(a: number, b: number): number {
        /** Add two numbers */
        const result = a + b;
        console.log(`Computing ${a} + ${b} = ${result}`);
        return result;
    }

    multiply(a: number, b: number): number {
        /** Multiply two numbers */
        const result = a * b;
        console.log(`Computing ${a} × ${b} = ${result}`);
        return result;
    }

    async factorial(n: number): Promise<number> {
        /** Calculate factorial of n */
        if (n < 0) {
            throw new Error("Factorial is not defined for negative numbers");
        }

        let result = 1;
        for (let i = 2; i <= n; i++) {
            result *= i;
        }

        console.log(`Computing ${n}! = ${result}`);
        return result;
    }

    fibonacci(n: number): number {
        /** Calculate nth Fibonacci number */
        if (n <= 0) {
            return 0;
        } else if (n === 1) {
            return 1;
        }

        let a = 0,
            b = 1;
        for (let i = 2; i <= n; i++) {
            [a, b] = [b, a + b];
        }

        console.log(`Fibonacci(${n}) = ${b}`);
        return b;
    }
}

const worker = new MathWorker();
worker.run();
