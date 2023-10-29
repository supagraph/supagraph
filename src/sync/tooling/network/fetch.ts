import { Event, ethers } from "ethers";
import { Block, TransactionReceipt } from "@ethersproject/abstract-provider";
import { JsonRpcProvider, WebSocketProvider } from "@ethersproject/providers";

import { SyncEvent } from "@/sync/types";

import { getEngine } from "@/sync/tooling/persistence/store";
import { readJSON, saveJSON } from "@/sync/tooling/persistence/disk";
import { getRandomProvider } from "@/sync/tooling/network/providers";

import { processPromiseQueue } from "@/sync/tooling/promises";

import { getBlockByNumber } from "@/sync/tooling/network/blocks";
import { getTransactionReceipt } from "@/sync/tooling/network/transactions";

// Constants used in post requests
const method = "POST";
const headers = {
  "Content-Type": "application/json",
};

// this is only managing about 20k blocks before infura gets complainee
export const attemptFnCall = async <T extends Record<string, any>>(
  syncProviders: Record<number, JsonRpcProvider | WebSocketProvider>,
  type: string,
  dets: {
    type: string;
    data: Event | string | number;
    chainId: number;
    blockNumber: number;
    timestamp?: number;
    from?: string;
  },
  prop: keyof ethers.Event,
  fn: "getBlock" | "getTransactionReceipt",
  resProp: "timestamp" | "from",
  attempts: number
) => {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new Error("Query timeout exceeded::attemptFnCall"));
    }, 120000);

    // select provider to use for call
    const provider =
      attempts === 0
        ? syncProviders[dets.chainId]
        : getRandomProvider(dets.chainId, [
            syncProviders[dets.chainId].connection.url,
          ]);

    // wrap call to get block/receipt from custom handler
    const call: (hash: unknown) => Promise<T> =
      fn === "getTransactionReceipt"
        ? // direct through getTransactionReceipt
          async (hash: unknown) =>
            getTransactionReceipt(
              provider,
              hash as string
            ) as unknown as Promise<T>
        : fn === "getBlock"
        ? // direct through getBlockByNumber
          async (num: unknown) =>
            getBlockByNumber(provider, num as number) as unknown as Promise<T>
        : // noop (unreachable)
          async () => undefined;

    // attempt to get the block from the connected provider first
    call(dets.data[prop])
      .then(async (resData: T) => {
        // if we have data...
        if (resData && resData[resProp]) {
          clearTimeout(timer);
          // write the file
          await saveJSON(
            type,
            `${dets.chainId}-${
              type === "blocks"
                ? parseInt(`${dets.blockNumber}`).toString(10)
                : dets.data[prop]
            }`,
            resData
          );
          // spread the val
          if (resProp === "from") {
            dets.from = resData.from as string;
          } else {
            // set the value by mutation
            dets.timestamp = resData.timestamp as number;
          }
          // resolve the requested data
          resolve(resData);
        } else {
          throw new Error("Missing data");
        }
      })
      .catch((e) => {
        clearTimeout(timer);
        reject(e);
      });
  });
};

// unpack event fn details for prop
export const getFnResultForProp = async <T extends Record<string, any>>(
  events: SyncEvent[],
  filtered: SyncEvent[],
  syncProviders: Record<number, JsonRpcProvider | WebSocketProvider>,
  type: string,
  prop: keyof ethers.Event,
  fn: "getBlock" | "getTransactionReceipt",
  resProp: "timestamp" | "from",
  silent?: boolean
) => {
  // fetch the engine for config
  const engine = await getEngine();
  // count the number of actual collections
  const result = new Set<string>();
  // place the operations into a stack and resolve them x at a time
  const stack: ((reqStack: (() => Promise<any>)[]) => Promise<void>)[] = [];
  // attempt the fn call against the dets (this will always result in an eventual call to processed)
  const makeFnCall = (
    dets: SyncEvent,
    attempts: number,
    resolve: (v: unknown) => void,
    reject: (e: any) => void
  ) =>
    // attempt the appropriate fn call and resolve/catch any errors
    attemptFnCall<T>(syncProviders, type, dets, prop, fn, resProp, attempts)
      .then((respType) => {
        resolve(respType);
      })
      .catch((errType) => {
        reject(errType);
      });
  // once fetched place results hash into the results
  const processed = (val: Block | TransactionReceipt) => {
    // count successfully fetched items
    if (val)
      result.add(
        (val as Block).hash || (val as TransactionReceipt).transactionHash
      );

    return val;
  };

  // iterate the events and attempt to pull the associated block/transaction
  for (const dets of filtered) {
    // push function to place the selected resProp at the requested prop
    stack.push(async (reqStack) => {
      await new Promise<unknown>((resolve, reject) => {
        // first attempt to read the content from disk
        readJSON(
          type,
          `${dets.chainId}-${
            // when dealing with blocks we need to ensure the blockNumber is base 10
            type === "blocks"
              ? +dets.blockNumber
              : typeof dets.data === "object"
              ? dets.data[prop]
              : dets.data
          }`
        )
          .then((data) => {
            if (data) {
              // store the requested resProp against the instance
              dets[resProp as string] = data[
                resProp as string
              ] as (typeof dets)[typeof resProp];
              // convert timestamp to base 10 number
              if (resProp === "timestamp")
                dets[resProp as string] = +dets[resProp as string];
              // resolve the discovered data
              resolve(data);
            } else {
              // make the fn call to get the block/transaction
              throw new Error("Missing block data fetch new...");
            }
          })
          .catch(() => {
            // if anything failed during read, attempt the fn call for the first time (attempts: 0)
            makeFnCall(dets, 0, resolve, reject);
          });
      })
        .catch(
          // set up a routine to catch and retry recursively - we always want to eventually resolve these
          (function retry(attempts) {
            // log if we keep attempting and we're not getting anything...
            if (!silent && attempts % 10 === 0) {
              console.log(
                `Made ${attempts} attempts to get: ${dets.chainId}::${
                  dets.data[prop] || dets.data
                }`
              );
            }

            // return the error handler (recursively stacking against the reqStack to avoid building too much off a callstack)
            return (): void => {
              // push another task to the reqStack (and resolve current task)
              reqStack.push(() =>
                // make the fnCall again and incr the attempts
                makeFnCall(dets, attempts, processed, retry(attempts + 1))
              );
            };
          })(1)
        )
        // finalise first attempt with processed (later attempts are piped through processed() on resolve())
        .then(processed);
    });
  }

  // process all items in the stack (cleanup=true to clean stack after processing)
  await processPromiseQueue(stack, engine.concurrency, true);

  // after resolution of all entries
  if (!silent && events.length)
    console.log(`All ${type} collected (${[...result].length})`);
};

// Method to fetch data via a given rpc url
export async function fetchDataWithRetries<T>(
  url: string,
  rpcMethod: string,
  rpcParams: unknown[],
  maxRetries: number = 3,
  silent: boolean = true
) {
  // keep response outside so we can cancel the req in finally
  let response: Response;

  // attempt to fetch the block data
  for (let retry = 1; retry <= maxRetries; retry += 1) {
    try {
      // set an abort controller to timeout the request
      const controller = new AbortController();

      // timeout after 30seconds by aborting the request
      const timeout = setTimeout(() => controller.abort(), 30e3);

      // use fetch to grab the blockdata directly from the rpcUrl endpoint
      response = await fetch(url, {
        method,
        headers,
        body: JSON.stringify({
          jsonrpc: "2.0",
          method: rpcMethod,
          params: rpcParams,
          id: Math.floor(Math.random() * 1e8),
        }),
        signal: controller.signal,
      });

      // collect the response
      const data = await response.json();

      // clear the abort
      clearTimeout(timeout);

      // return the result
      if (data.result) return data.result as T;
    } catch (error) {
      // if theres an error we'll try again upto maxRetries
      if (!silent)
        console.error(`Error fetching ${rpcMethod} (retry ${retry}):`, error);
      // lets wait a second after a failed fetch before attempting it again
      await new Promise((resolve) => {
        setTimeout(
          resolve,
          // try again in anywhere from 1 to 5 seconds
          Math.floor((Math.random() * (5 - 1) + 1) * 1e3)
        );
      });
    } finally {
      // finally, if we returned or errored, we need to cancel the body to close the resource
      if (response && !response.bodyUsed) response.body.cancel();
    }
  }

  // if we fail to return the result then we should restack the task
  return null;
}
