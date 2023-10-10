// Get the global engine for config
import { getEngine } from "@/sync/tooling/persistence/store";

// Construct a global promiseQueue to be delayed until later (but still complete as part of this sync)
export const promiseQueue: (
  | Promise<unknown>
  | ((stack?: (() => Promise<any>)[]) => Promise<unknown>)
)[] = [];

// promises can be enqueued during handler execution - make sure to handle any errors inside your promise - this queue will only pass once all promises successfully resolve
export const enqueuePromise = (
  promise:
    | Promise<unknown>
    | ((stack?: (() => Promise<any>)[]) => Promise<unknown>)
) => {
  promiseQueue.push(promise);
};

// process all promises added to the queue during handler processing (allowing us to move from sequential to parallel processing after first pass - careful though, many things must be processed sequentially)
export const processPromiseQueue = async (
  queue: (
    | Promise<unknown>
    | ((stack: (() => Promise<any>)[]) => Promise<unknown>)
  )[],
  concurrency?: number,
  cleanup?: boolean
) => {
  // access global engine for config
  const engine = await getEngine();
  // construct a reqStack so that we're only processing x reqs at a time
  const reqStack: (() => Promise<any>)[] = [];

  // iterate the ranges and collect all events in that range
  for (const key of Object.keys(queue)) {
    const promise = queue[key];
    reqStack.push(async function keepTrying() {
      try {
        if (typeof (promise as Promise<unknown>).then === "function") {
          queue[key] = await Promise.resolve(promise);
        } else {
          queue[key] = await (
            promise as (stack: (() => Promise<any>)[]) => Promise<unknown>
          )(reqStack);
        }
      } catch {
        reqStack.push(async () => keepTrying());
      }
    });
  }

  // pull from the reqStack and process...
  while (reqStack.length > 0) {
    // process n requests concurrently and wait for them all to resolve
    const consec = reqStack.splice(0, concurrency ?? engine.concurrency);
    // run through all promises until we come to a stop
    await Promise.all(consec.map(async (fn) => fn()));
  }

  // remove all from queue on cleanup
  if (cleanup) {
    queue.splice(0, queue.length);
  }
};
