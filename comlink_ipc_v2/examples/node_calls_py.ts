import { ParentWorker } from "../comlink_v2";

async function main() {
    const worker = new ParentWorker(`../examples/math_worker.py`, "python");
    await worker.start();

    // will run first | blocking
    worker.acall.fibonacci(55).then(result => {
        console.log("fibo number => ", result);
    });

    const fact = await worker.acall.factorial(15);
    console.log("factorial => ", fact);

    await worker.close();
}
main().catch(console.error);
