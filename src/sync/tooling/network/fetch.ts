import { ethers } from "ethers";
import {
  Block,
  BlockWithTransactions,
  TransactionReceipt,
  TransactionResponse,
} from "@ethersproject/abstract-provider";
import { JsonRpcProvider, WebSocketProvider } from "@ethersproject/providers";

import { getEngine } from "@/sync/tooling/persistence/store";
import { withRetries } from "@/utils/withRetries";
import { processPromiseQueue } from "@/sync/tooling/promises";

// All events fit into the SyncEvent type
import { SyncEvent } from "@/sync/types";

// Constants used in post requests
const method = "POST";
const headers = {
  "Content-Type": "application/json",
};

// Call the fn and save the details
export const attemptFnCall = async <T extends Record<string, any>>(
  dets: SyncEvent,
  fnCall: (dets: SyncEvent) => Promise<T>,
  saveDets: (dets: SyncEvent, res: T) => Promise<void> | void
) => {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new Error("Query timeout exceeded::attemptFnCall"));
    }, 120000);

    // attempt to get the block from the connected provider first
    fnCall(dets)
      .then(async (resData: T) => {
        // if we have data...
        if (resData) {
          // clear timeout to kill rejection
          clearTimeout(timer);
          // spread the val
          await saveDets(dets, resData);
          // resolve the requested data
          resolve(resData);
        } else {
          // throw to trigger a retry attempt (in parent context)
          throw new Error("Missing data");
        }
      })
      .catch((e) => {
        // clear timeout to kill hanging resource
        clearTimeout(timer);
        // reject with error
        reject(e);
      });
  });
};

// Unpack event fn details for prop using provided fns to process the operations
export const getFnResultForProp = async <T extends Record<string, any>>(
  events: SyncEvent[],
  filtered: SyncEvent[],
  type: string,
  prop: keyof ethers.Event,
  fnCall: (dets: SyncEvent) => Promise<T>,
  readDets: (dets: SyncEvent) => Promise<T>,
  saveDets: (dets: SyncEvent, res: T) => Promise<void> | void,
  cleanDets: (dets: SyncEvent) => Promise<void> | void,
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
    resolve: (v: unknown) => void,
    reject: (e: any) => void
  ) =>
    // attempt the appropriate fn call and resolve/catch any errors
    attemptFnCall<T>(dets, fnCall, saveDets)
      .then((respType: T) => {
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
        readDets(dets)
          .then(async (data) => {
            if (data) {
              // save the result into dets
              await saveDets(dets, data);
              // convert timestamp to base 10 number
              await cleanDets(dets);
              // resolve the discovered data
              resolve(data);
            } else {
              // make the fn call to get the block/transaction
              throw new Error("Missing data fetch new...");
            }
          })
          .catch(() => {
            // if anything failed during read, attempt the fn call for the first time (attempts: 0)
            return makeFnCall(dets, resolve, reject);
          });
      })
        // finalise first attempt with processed (later attempts are piped through processed() on resolve())
        .then(processed)
        // if any part of the first attempt failed then try again...
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
                makeFnCall(dets, processed, retry(attempts + 1))
              );
            };
          })(1)
        );
    });
  }

  // process all items in the stack (cleanup=true to clean stack after processing)
  await processPromiseQueue(stack, engine.concurrency, true);

  // after resolution of all entries
  if (!silent && events.length)
    console.log(`All ${type} collected (${[...result].length})`);
};

// Method to fetch data via a given rpc url
export const fetchDataWithRetries = async <T>(
  url: string,
  rpcMethod: string,
  rpcParams: unknown[],
  maxRetries: number = 3,
  silent: boolean = true
) => {
  // keep response outside so we can cancel the req in finally
  let response: Response;

  // attempt to fetch the block data with retries
  return withRetries<T>(
    async () => {
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

      // throw to retry/end
      throw new Error("No response");
    },
    (error, retries) => {
      // if theres an error we'll try again upto maxRetries
      if (!silent)
        console.error(`Error fetching ${rpcMethod} (retry ${retries}):`, error);
    },
    () => {
      // finally, if we returned or errored, we need to cancel the body to close the resource
      if (response && !response.bodyUsed) response.body.cancel();
    },
    // number of retry attemps
    maxRetries,
    // dynamically set how long to wait between attempts for random wait time each call
    () => Math.floor((Math.random() * (5 - 1) + 1) * 1e3)
  );
};

// Get a block and all transaction responses with retries
export const getBlockByNumber = async (
  provider: JsonRpcProvider | WebSocketProvider,
  blockNumber: number | "latest",
  useProvider: boolean = false,
  maxRetries: number = 3,
  retryDelay: number = 1e3
): Promise<BlockWithTransactions> => {
  // get the block tag as hex or latest
  const blockTag =
    blockNumber === "latest" ? "latest" : `0x${blockNumber.toString(16)}`;

  // if we're using the provider
  if (useProvider) {
    return withRetries<BlockWithTransactions>(
      async () => {
        // fetch block by hex number passing true to fetch all transaction responses
        const block = await provider.send("eth_getBlockByNumber", [
          blockTag,
          true,
        ]);

        // check the block holds data and return if valid
        if (block?.number && +block.number === +blockNumber) return block;

        // throw error and retry
        throw new Error("No block response");
      },
      undefined,
      undefined,
      maxRetries,
      retryDelay
    );
  }

  // otherwise pass through to fetchWithRetries
  const block = await fetchDataWithRetries<BlockWithTransactions>(
    provider.connection.url,
    "eth_getBlockByNumber",
    [blockTag, true],
    maxRetries,
    true
  );

  // check the block holds data
  if (block?.number && +block.number === +blockNumber) {
    // return discovered block response
    return block;
  }

  // throw error if we didn't get the block data
  throw new Error("No block response");
};

// get a full transaction receipt from hash
export const getTransactionReceipt = async (
  provider: JsonRpcProvider | WebSocketProvider,
  txHash: string | TransactionResponse,
  useProvider: boolean = false,
  maxRetries: number = 3,
  retryDelay: number = 1e3
): Promise<TransactionReceipt & TransactionResponse> => {
  // if we're using the provider
  if (useProvider) {
    return withRetries<TransactionReceipt & TransactionResponse>(
      async () => {
        // get the txResponse if we dont already have it
        const tx = (txHash as TransactionResponse)?.hash
          ? txHash
          : await provider.send("eth_getTransactionByHash", [txHash]);
        // get the receipt for the txHash
        const receipt = await provider.send("eth_getTransactionReceipt", [
          tx.hash,
        ]);

        // if we have all components of the tx + receipt
        if (tx && tx.hash && receipt && receipt.transactionHash)
          return {
            ...tx,
            ...receipt,
          };

        // throw to retry/end
        throw new Error("No transaction response");
      },
      // no catch or finally...
      undefined,
      undefined,
      // pass through maxRetries options
      maxRetries,
      retryDelay
    );
  }

  // get the txResponse if we dont already have it
  const tx = (txHash as TransactionResponse)?.hash
    ? (txHash as TransactionResponse)
    : await fetchDataWithRetries<TransactionResponse>(
        provider.connection.url,
        "eth_getTransactionByHash",
        [txHash],
        maxRetries,
        true
      );
  // get the receipt for the txHash
  const receipt = await fetchDataWithRetries<TransactionReceipt>(
    provider.connection.url,
    "eth_getTransactionReceipt",
    [tx.hash],
    maxRetries,
    true
  );

  // if we discovered the tx and receipt return them
  if (tx && tx.hash && receipt && receipt.transactionHash)
    return {
      ...tx,
      ...receipt,
    };

  // throw to retry/end
  throw new Error("No transaction response");
};
