import { ParentWorker } from "../comlink_v2";

async function main() {
    const worker = new ParentWorker(`../examples/math_worker.ts`, "tsx");
    await worker.start();

    // will run first | blocking
    worker.acall.fibonacci(11).then(result => {
        console.log("fibo number => ", result);
    });

    const fact = await worker.acall.factorial(11);
    console.log("factorial => ", fact);

    await worker.close();
}
main().catch(console.error);
