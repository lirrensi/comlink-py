#!/usr/bin/env python3

import time
import asyncio
import sys
import os
import math
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from comlink_v2 import ChildWorker


class MathWorker(ChildWorker):
    
    def add(self, a, b):
        """Add two numbers"""
        result = a + b
        print(f"Computing {a} + {b} = {result}")
        return result
    
    def multiply(self, a, b):
        """Multiply two numbers"""
        result = a * b
        print(f"Computing {a} × {b} = {result}")
        return result
    
    def factorial(self, n):
        """Calculate factorial of n"""
        if n < 0:
            raise ValueError("Factorial is not defined for negative numbers")
        result = math.factorial(n)
        print(f"Computing {n}! = {result}")
        return result
    
    async def fibonacci(self, n):
        """Calculate nth Fibonacci number"""
        if n <= 0:
            return 0
        elif n == 1:
            return 1
        
        a, b = 0, 1
        for _ in range(2, n + 1):
            a, b = b, a + b
        
        print(f"Fibonacci({n}) = {b}")
        return b

if __name__ == "__main__":
    worker = MathWorker()
    worker.run()