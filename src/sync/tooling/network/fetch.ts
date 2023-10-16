import { Event, ethers } from "ethers";
import { Block, TransactionReceipt } from "@ethersproject/abstract-provider";
import { JsonRpcProvider, WebSocketProvider } from "@ethersproject/providers";

import { SyncEvent } from "@/sync/types";

import { getEngine } from "@/sync/tooling/persistence/store";
import { readJSON, saveJSON } from "@/sync/tooling/persistence/disk";

import { processPromiseQueue } from "@/sync/tooling/promises";

import { getRandomProvider } from "@/sync/tooling/network/providers";
import { getTransactionReceipt } from "@/sync/tooling/network/transactions";

// this is only managing about 20k blocks before infura gets complainee
export const attemptFnCall = async <T extends Record<string, any>>(
  syncProviders: Record<number, JsonRpcProvider | WebSocketProvider>,
  type: string,
  dets: {
    type: string;
    data: Event;
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

    // wrap call to get receipt from custom handler
    const call: (hash: unknown) => Promise<T> =
      fn === "getTransactionReceipt"
        ? async (hash: unknown) =>
            getTransactionReceipt(
              provider,
              hash as string
            ) as unknown as Promise<T>
        : async (hash: unknown) =>
            provider[fn](hash as string) as unknown as Promise<T>;

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

  // iterate the events and attempt to pull the associated block/transaction
  for (const dets of filtered) {
    // push function to place the selected resProp at the requested prop
    stack.push(async (reqStack) => {
      await new Promise<unknown>((resolve, reject) => {
        readJSON(
          type,
          `${dets.chainId}-${
            type === "blocks"
              ? parseInt(`${dets.blockNumber}`).toString(10)
              : typeof dets.data === "object"
              ? dets.data[prop]
              : dets.data
          }`
        )
          .then((data) => {
            if (data) {
              try {
                // spread the val
                if (resProp === "from") {
                  dets.from = data.from as string;
                } else {
                  // set the value by mutation
                  dets.timestamp = data.timestamp as number;
                }
                resolve(data);
              } catch {
                attemptFnCall<T>(
                  syncProviders,
                  type,
                  dets,
                  prop,
                  fn,
                  resProp,
                  0
                )
                  .then((respType) => {
                    resolve(respType);
                  })
                  .catch((errType) => {
                    reject(errType);
                  });
              }
            } else {
              attemptFnCall<T>(syncProviders, type, dets, prop, fn, resProp, 0)
                .then((respType) => {
                  resolve(respType);
                })
                .catch((errType) => {
                  reject(errType);
                });
            }
          })
          .catch(() => {
            attemptFnCall<T>(syncProviders, type, dets, prop, fn, resProp, 0)
              .then((respType) => {
                resolve(respType);
              })
              .catch((errType) => {
                reject(errType);
              });
          });
      })
        .catch(
          (function retry(attempts) {
            // logging if we keep attempting and we're not getting anything...
            if (!silent && attempts % 10 === 0) {
              console.log(
                `Made ${attempts} attempts to get: ${dets.chainId}::${
                  dets.data[prop] || dets.data
                }`
              );
            }

            // return the error handler (recursive stacking against the reqStack)
            return (): void => {
              reqStack.push(() =>
                attemptFnCall<T>(
                  syncProviders,
                  type,
                  dets,
                  prop,
                  fn,
                  resProp,
                  attempts
                ).catch(retry(attempts + 1))
              );
            };
          })(1)
        )
        .then((val) => {
          // count successfully fetched items
          result.add(
            (val as Block).hash || (val as TransactionReceipt).transactionHash
          );

          return val;
        });
    });
  }

  // process all items in the stack (cleanup=true to clean stack after processing)
  await processPromiseQueue(stack, engine.concurrency, true);

  // after resolution of all entries
  if (!silent && events.length)
    console.log(`All ${type} collected (${[...result].length})`);
};
