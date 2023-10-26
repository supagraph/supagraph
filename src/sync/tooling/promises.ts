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

  // return a promise to resolve the stack
  return new Promise<void>((resolve, reject) => {
    // iterate the queue and place each promise as a response of a fn callback
    for (const key of Object.keys(queue)) {
      // get the promise
      const promise = queue[key];
      // keep trying to process the given promise -- if the handler throws this could cause an infinite load situation limit with retries
      reqStack.push(async function keepTrying(attempts = 0) {
        try {
          // wait for the function/promise to resolve
          if (typeof (promise as Promise<unknown>).then === "function") {
            queue[key] = await Promise.resolve(promise);
          } else if (typeof promise === "function") {
            queue[key] = await (
              promise as (stack: (() => Promise<any>)[]) => Promise<unknown>
            )(reqStack);
          }
        } catch (e) {
          // if theres an error - restack upto 10 times before throwing in outer context
          if (attempts < 3) {
            // print the error
            if (!engine.flags.silent) console.log(e);
            // make another attempt
            reqStack.push(async () => keepTrying(attempts + 1));
          } else {
            // throw the error externally
            throw e;
          }
        }
      });
    }

    // resolve the resolution of the promise stack
    resolve(
      Promise.resolve()
        .then(async () => {
          // pull from the reqStack and process...
          while (reqStack.length > 0) {
            // process n requests concurrently and wait for them all to resolve
            const consec = [];

            // pop up to 'concurrency' number of requests
            for (
              let i = 0;
              // we can't take less than one item from the stack
              i < (concurrency || engine.concurrency || 1) &&
              reqStack.length > 0;
              // incr index to position for loop
              i += 1
            ) {
              // shift from reqStack to consec to process in batches
              consec.push(reqStack.shift());
            }

            // run through all promises until we come to a stop
            await Promise.all(consec.map(async (fn) => fn())).catch((e) => {
              // throw in context, we want to trigger an error in parent and stop processing, no handler should error 10 times.
              reject(e);
            });
          }
        })
        .then(() => {
          // remove all from queue on cleanup
          if (cleanup) {
            queue.length = 0;
          }
        })
    );
  });
};
