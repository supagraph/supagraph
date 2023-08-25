// Import fs to record blocks and transactionReceipts to disk for pre-processing
import fs from "fs";

// Build Event filters with ethers
import { ethers, providers } from "ethers";
import {
  Block,
  TransactionReceipt,
  JsonRpcProvider,
  TransactionResponse,
  // @TODO - check support for wss providers
  WebSocketProvider,
} from "@ethersproject/providers";
import { getAddress } from "ethers/lib/utils";
import { BlockWithTransactions } from "@ethersproject/abstract-provider";

// Import tooling to check bloomFilters
import {
  isTopicInBloom,
  isContractAddressInBloom,
} from "ethereum-bloom-filters";

// Parse and write csv documents (using csv incase we have to move processing into readStreams)
import csvParser from "csv-parser";
import csvWriter from "csv-write-stream";

// Create new entities against the engine via the Store
import { Entity, Store, getEngine } from "./store";

// CWD defined in base db
import { cwd } from "./db";

// Allow for stages to be skipped via config
export enum Stage {
  events = 1,
  blocks,
  transactions,
  sort,
  process,
}

// Defines a sync operation
export type Sync = {
  provider: JsonRpcProvider | WebSocketProvider;
  startBlock: number | "latest";
  eventName: string;
  address?: string;
  eventAbi?: ethers.Contract["abi"];
  opts?: {
    collectBlocks?: boolean;
    collectTxReceipts?: boolean;
  };
  onEvent: HandlerFn;
};

// response type for a sync summary
export type SyncResponse = {
  error?: any;
  syncOps?: number;
  events?: number;
  runTime?: string;
  chainIds?: number[];
  eventsByChain?: Record<number, number>;
  startBlocksByChain?: Record<number, number>;
  latestBlocksByChain?: Record<number, number>;
  close?: (() => Promise<void>) | undefined;
};

// Export the complete supagraph configuration (sync & graph) - we can add everything from Mappings to this config
export type SyncConfig = {
  // set the local engine (true: db || false: mongo)
  dev: boolean;
  // name your supagraph (this will inform mongo table name etc...)
  name: string;
  // boolean to reset server between runs (used to move back to startBlock each run)
  reset: boolean;
  // boolean to start the listening service to keep the chain insync
  listen: boolean;
  // clean up initial sync tmp files
  cleanup: boolean;
  // optionally silence std-out until error
  silent: boolean;
  // global tx/block capture
  collectBlocks: boolean;
  collectTxReceipts: boolean;
  // flag mutable to insert by upsert only (on id field (mutate entities))
  // - otherwise we use _block_number + id to make a unique document and do a distinct groupBy on the id when querying
  //   ie: do everything the immutable way (this can be a lot more expensive)
  mutable: boolean;
  // how often do we want queries to be revalidated?
  revalidate: number;
  staleWhileRevalidate: number;
  // configure providers
  providers: {
    [chainId: number]: {
      rpcUrl: string;
    };
  };
  // register events into named groups
  events: {
    [group: string]: string[];
  };
  // configure available Contracts and their block details
  contracts: {
    [key: string | number]: {
      // for network listeners we only need start and endblock
      startBlock: number | "latest";
      endBlock: number | "latest";
      // Establish all event signatures available on this contract (we could also accept a .sol or .json file here)
      events?: string | string[];
      // use handlers registered against "token" named group in handlers/index.ts
      handlers?: string;
      // set config from env
      chainId?: number;
      address?: string | undefined;
      // we don't receipts here
      collectBlocks?: boolean;
      collectTxReceipts?: boolean;
    };
  };
  // define supagraph schema
  schema: string;
  // define supagraph default query
  defaultQuery: string;
};

// Defines a SyncEvent obj
export type SyncEvent = {
  type: string;
  data: ethers.Event;
  number: number;
  blockNumber: number;
  chainId: number;
  collectTxReceipt: boolean;
  collectBlock: boolean;
  timestamp?: number | undefined;
  from?: string | undefined;
};

// Handlers definition, to map handlers to contracts
export type Handlers = {
  [key: string | number]: {
    [key: string]: HandlerFn;
  };
};

// Handler fn definition
export type HandlerFn = (
  args: any,
  {
    tx,
    block,
    logIndex,
  }: {
    tx: TransactionReceipt & TransactionResponse;
    block: Block;
    logIndex: number;
  }
) => void | Promise<void>;

// Object holder handler onEvents
export type HandlerFns = Record<string, HandlerFn>;

// latestEntity type (for locking the db during syncs)
export type LatestEntity = Record<
  string,
  Entity<{
    id: string;
    latestBlock: number;
    latestBlockTime: number;
    locked: boolean;
    lockedAt: number;
    _block_num: number;
    _block_ts: number;
  }> & {
    id: string;
    locked: boolean;
    lockedAt: number;
    latestBlock: number;
    latestBlockTime: number;
    _block_num: number;
    _block_ts: number;
  }
>;

// how long to lock the db between sync attempts
export const LOCKED_FOR = 10;

// how many ranges to request at a time
export const CONCURRENCY = 100;

// the maximum no. of blocks to be attempted in any one queryFilter call
export const RANGE_LIMIT = 100000;

// sync operations will be appended in the route.ts file using addSync
const syncs: Sync[] = [];

// allow a promise to be delayed until later (but still complete as part of this sync)
const promiseQueue: ((stack?: (() => Promise<any>)[]) => Promise<void>)[] = [];

// list of public rpc endpoints
export const altProviders: Record<number, string[]> = {
  1: [`https://rpc.ankr.com/eth`],
  5: [
    `https://ethereum-goerli.publicnode.com`,
    `https://eth-goerli.public.blastapi.io`,
    `https://rpc.goerli.eth.gateway.fm`,
    `https://rpc.ankr.com/eth_goerli`,
  ],
  5000: [`https://rpc.mantle.xyz`],
  5001: [`https://rpc.testnet.mantle.xyz`],
};

// set the default RPC providers
export const rpcProviders: Record<
  number,
  Record<number, providers.JsonRpcProvider>
> = {
  1: {
    0: new providers.JsonRpcProvider(altProviders[1][0]),
  },
  5: {
    0: new providers.JsonRpcProvider(altProviders[5][0]),
  },
  5000: {
    0: new providers.JsonRpcProvider(altProviders[5000][0]),
  },
  5001: {
    0: new providers.JsonRpcProvider(altProviders[5001][0]),
  },
};

// function to return a provider from the alt list
export const getRandomProvider = (
  chainId: number,
  extraProviders: string[] = []
) => {
  const useProviders = [...altProviders[chainId], ...extraProviders];
  const key = Math.floor(Math.random() * useProviders.length);
  rpcProviders[chainId][key] =
    rpcProviders[chainId][key] ||
    new providers.JsonRpcProvider(useProviders[key]);
  return rpcProviders[chainId][key];
};

// get a block and all transaction responses
const getBlockByNumber = async (
  provider: JsonRpcProvider | WebSocketProvider,
  blockNumber: number
): Promise<BlockWithTransactions & any> => {
  try {
    // fetch block by hex number passing true to fetch all transaction responses
    const block = await provider.send("eth_getBlockByNumber", [
      `0x${blockNumber.toString(16)}`,
      true,
    ]);
    // check the block holds data
    if (!block?.number) {
      throw new Error("No block response");
    }
    return block;
  } catch {
    return getBlockByNumber(provider, blockNumber);
  }
};
// promises can be enqueued during handler execution - make sure to handle any errors inside your promise - this queue will only pass once all promises successfully resolve
export const enqueuePromise = (
  promise: (stack?: (() => Promise<any>)[]) => Promise<void>
) => {
  promiseQueue.push(promise);
};

// process all promises added to the queue during handler processing (allowing us to move from sequential to parallel processing after first pass - careful though, many things must be processed sequentially)
export const processPromiseQueue = async (
  queue: ((stack: (() => Promise<any>)[]) => Promise<void>)[],
  concurrency?: number
) => {
  // construct a reqStack so that we're only processing x reqs at a time
  const reqStack: (() => Promise<any>)[] = [];

  // iterate the ranges and collect all events in that range
  for (const promise of queue) {
    reqStack.push(async function keepTrying() {
      try {
        await promise(reqStack);
      } catch {
        reqStack.push(async () => keepTrying());
      }
    });
  }

  // pull from the reqStack and process...
  while (reqStack.length > 0) {
    // process n requests concurrently and wait for them all to resolve
    const consec = reqStack.splice(0, concurrency || CONCURRENCY);
    // run through all promises until we come to a stop
    await Promise.all(consec.map(async (fn) => fn()));
  }
};

// allow external usage to set the sync operations
export function addSync<T extends Record<string, unknown>>(
  eventNameOrParams:
    | string
    | {
        provider: JsonRpcProvider | WebSocketProvider;
        startBlock: number | "latest";
        eventName: string;
        onEvent: HandlerFn;
        address?: string;
        eventAbi?: ethers.Contract["abi"];
        opts?: {
          collectBlocks?: boolean;
          collectTxReceipts?: boolean;
        };
      },
  provider?: JsonRpcProvider | WebSocketProvider,
  startBlock?: number | "latest",
  address?: string,
  eventAbi?: ethers.Contract["abi"],
  onEvent?: HandlerFn,
  opts?: {
    collectBlocks?: boolean;
    collectTxReceipts?: boolean;
  }
): (
  args: T,
  {
    tx,
    block,
    logIndex,
  }: {
    tx: TransactionReceipt & TransactionResponse;
    block: Block;
    logIndex: number;
  }
) => void {
  let params;

  // hydrate the params
  if (typeof eventNameOrParams === "string") {
    params = {
      address: address!,
      eventAbi: eventAbi!,
      eventName: eventNameOrParams!,
      provider: provider!,
      startBlock: startBlock!,
      opts: opts || { collectTxReceipts: false, collectBlocks: false },
      onEvent: onEvent!,
    };
  } else {
    params = eventNameOrParams;
  }

  // push to the syncs
  syncs.push(params);

  // Return the event handler to constructing context
  return params.onEvent;
}

// set the sync by config & handler object
export const setSyncs = (config: SyncConfig, handlers: Handlers) => {
  // Object of providers by rpcUrl
  const providerCache: { [rpcUrl: string]: providers.JsonRpcProvider } = {};

  // Register each of the syncs from the mapping
  Object.keys(config.contracts).forEach((name) => {
    // extract this syncOp
    const syncOp = config.contracts[name];

    // extract rpc url
    const { rpcUrl } =
      config.providers[
        (syncOp.chainId as keyof typeof config.providers) ||
          (name as unknown as number)
      ];

    // pull the handlers for the registration
    const mapping =
      typeof syncOp.handlers === "string" || typeof syncOp.handlers === "number"
        ? handlers?.[syncOp.handlers || ("" as keyof Handlers)] || {}
        : syncOp.handlers || handlers[name];

    // extract events
    const events =
      typeof syncOp.events === "string" &&
      Object.hasOwnProperty.call(config, "events")
        ? (config as unknown as { events: Record<string, string[]> })?.events[
            syncOp.events
          ]
        : syncOp.events || [];

    // configure JsonRpcProvider for contracts chainId (@TODO: can we switch this out for a WebSocketProvider?)
    providerCache[rpcUrl] =
      providerCache[rpcUrl] || new providers.JsonRpcProvider(rpcUrl);

    // for each handler register a sync
    Object.keys(mapping || []).forEach((eventName) => {
      addSync({
        eventName,
        eventAbi: events,
        address: syncOp.address,
        startBlock: syncOp.startBlock,
        provider: providerCache[rpcUrl],
        opts: {
          collectBlocks:
            syncOp.collectBlocks || eventName === "onBlock" || false,
          collectTxReceipts: syncOp.collectTxReceipts || false,
        },
        onEvent: mapping[eventName],
      });
    });
  });
};

// read the file from disk
const readJSON = async <T extends Record<string, any>>(
  type: string,
  filename: string
): Promise<T> => {
  return new Promise((resolve, reject) => {
    fs.readFile(`${cwd}${type}-${filename}.json`, "utf8", async (err, file) => {
      if (!err && file && file.length) {
        try {
          const data = JSON.parse(file);
          resolve(data as T);
        } catch (e) {
          reject(err);
        }
      } else {
        reject(err);
      }
    });
  });
};

// save a JSON data blob to disk
const saveJSON = async (
  type: string,
  filename: string,
  resData: Record<string, unknown>
): Promise<boolean> => {
  return new Promise((resolve, reject) => {
    // create data dir if missing
    try {
      fs.accessSync(`${cwd}`);
    } catch {
      fs.mkdirSync(`${cwd}`);
    }
    // write the file
    fs.writeFile(
      `${cwd}${type}-${filename}.json`,
      JSON.stringify(resData),
      "utf8",
      async (err) => {
        if (!err) {
          resolve(true);
        } else {
          reject(err);
        }
      }
    );
  });
};

// remove the file from disk
const deleteJSON = async (type: string, filename: string): Promise<boolean> => {
  return new Promise((resolve) => {
    try {
      // delete the file
      fs.rm(`${cwd}${type}-${filename}.json`, async () => resolve(true));
    } catch {
      // already deleted
      resolve(true);
    }
  });
};

// slice the range according to the provided limit
const createBlockRanges = (
  fromBlock: number,
  toBlock: number,
  limit: number
): number[][] => {
  // each current is a tuple containing from and to
  let currentRange: number[] = [fromBlock];
  // we collect the tuples into an array
  const blockRanges: number[][] = [];

  // bracket the from-to to only include the limit number of blocks
  for (let i = fromBlock + 1; i <= toBlock; i += 1) {
    // if we step over the limit push a new entry
    if (i - currentRange[0] >= limit) {
      // -1 so we don't request the same block twice
      currentRange.push(i - 1);
      blockRanges.push(currentRange);
      // start the next range at boundary
      currentRange = [i];
    }
  }

  // push the final toBlock to the current range
  currentRange.push(toBlock);
  // record the final current range
  blockRanges.push(currentRange);

  // return all ranges
  return blockRanges;
};

// pull all transactions in the requested range
const TxsFromRange = async (
  chainId: number,
  provider: JsonRpcProvider | WebSocketProvider,
  from: number,
  to: number,
  collectTxReceipts: boolean,
  reqStack: (() => Promise<any>)[],
  result: Set<{
    hash: string;
    block: number;
  }>,
  silent?: boolean
) => {
  // save to disk for future use
  const fetchAndSaveBlock = async (number: number, block_?: Block) => {
    // fetch the block and transactions via the provider (we dont need raw details ie logBlooms here)
    const block = block_ || (await provider.getBlockWithTransactions(number));
    // if we have a block
    if (block?.number) {
      // save the block to disk to release from mem
      await saveJSON(
        "blocks",
        `${chainId}-${+parseInt(`${block.number}`).toString(10)}`,
        block as unknown as Record<string, unknown>
      );

      // save the transactions
      await Promise.all(
        block.transactions.map(async (tx: string | TransactionResponse) => {
          // add the fetched block to the result
          result.add({
            // allow for the tx to be passed as its hash (we're currently requesting all txResponses from the block so it will never be a string atm)
            hash: typeof tx === "string" ? tx : tx.hash,
            block: +parseInt(`${block.number}`).toString(10),
          });

          // if collectTxReceipts fetch the receipt
          const saveTx =
            !collectTxReceipts && typeof tx !== "string"
              ? tx
              : {
                  // txResponse & txReceipt if collectTxReceipts
                  ...(typeof tx === "string" ? {} : tx),
                  ...(await provider.getTransactionReceipt(
                    typeof tx === "string" ? tx : tx.hash
                  )),
                };

          // save each tx to disk to release from mem
          await saveJSON(
            "transactions",
            `${chainId}-${typeof tx === "string" ? tx : tx.hash}`,
            saveTx as unknown as Record<string, unknown>
          );
        })
      );
    } else {
      // trigger error handler and retry...
      throw new Error("No block response");
    }
  };

  // iterate the ranges and collect all blocks in that range
  while (from <= to) {
    // wait for each item to complete before fetching the next one
    await new Promise<unknown>((resolve, reject) => {
      // carry out the fetch operation
      const doFetch = async (block?: Block) => {
        // do the actual fetch
        return fetchAndSaveBlock(from, block)
          .catch((e) => {
            reject(e);
          })
          .then(() => {
            resolve(true);
          });
      };
      // atempt to pull from disk first
      fs.readFile(
        `${cwd}blocks-${chainId}-${parseInt(`${from}`).toString(10)}.json`,
        "utf8",
        async (err, file) => {
          if (!err && file && file.length) {
            // console.log(file,`${cwd}blocks-${chainId}-${parseInt(`${from}`).toString(10)}.json` )
            try {
              // parse data
              const data = JSON.parse(file);
              // check data holds transactions
              if (
                data.transactions &&
                data.transactions.length &&
                data.transactions[0].hash &&
                // bail out if we're fetching receipts because we're going to have to fetch all of these
                !collectTxReceipts
              ) {
                // add all tx hashes to the result
                await Promise.all(
                  data.transactions.map(async (tx: TransactionResponse) => {
                    result.add({
                      hash: tx.hash,
                      block: from,
                    });
                    // save each tx to disk to release from mem
                    await saveJSON(
                      "transactions",
                      `${chainId}-${typeof tx === "string" ? tx : tx.hash}`,
                      tx as unknown as Record<string, unknown>
                    );
                  })
                );
                // complete the promise
                resolve(true);
              } else {
                // fetch transactions from the provided block
                await doFetch(data);
              }
            } catch {
              // fecth block and transactions from current from
              await doFetch();
            }
          } else {
            // fecth block and transactions from current from
            await doFetch();
          }
        }
      );
    }).catch(
      (function retry(attempts) {
        // logging if we keep attempting and we're not getting anything...
        if (attempts % 10 === 0) {
          if (!silent)
            console.log(
              `Made ${attempts} attempts to get: ${chainId}::block::${from}`
            );
        }

        // return the error handler (recursive stacking against the reqStack)
        return (): void => {
          reqStack.push(() =>
            fetchAndSaveBlock(from).catch(retry(attempts + 1))
          );
        };
      })(1)
    );
    // move from for next tick
    from += 1;
  }
};

// wrap the transactions response to set and to cast type
const wrapTxRes = async (
  entries: {
    hash: string;
    block: number;
  }[],
  provider: JsonRpcProvider | WebSocketProvider,
  toBlock: number
) => {
  return Promise.all(
    entries.map(async (entry) => {
      return {
        type: "onTransaction",
        data: entry.hash,
        blockNumber: entry.block,
        number: toBlock,
        chainId: provider.network.chainId,
        collectTxReceipt: true,
        collectBlock: true,
      };
    })
  );
};

// pull new events from the contract
const getNewTransactions = async (
  fromBlock: number,
  toBlock: number,
  provider: JsonRpcProvider | WebSocketProvider,
  collectTxReceipts: boolean = false,
  silent: boolean = false
) => {
  // collect a list of events then map the event type to each so that we can rebuild all events into a single array later
  const result = new Set<{
    hash: string;
    block: number;
  }>();
  // get the chainId
  const { chainId } = provider.network;
  // create a new eventRange for each 500,000 blocks (to process in parallel)
  const ranges = createBlockRanges(fromBlock, toBlock, 10);
  // construct a reqStack so that we're only processing x reqs at a time
  const stack: ((reqStack: (() => Promise<any>)[]) => Promise<void>)[] = [];

  // iterate the ranges and collect all events in that range
  for (const [from, to] of ranges) {
    stack.push(async (reqStack) => {
      // what if result is just built by ref? then we dont have to await failures etc...
      await TxsFromRange(
        chainId,
        provider,
        from,
        to,
        collectTxReceipts,
        reqStack,
        result,
        silent
      );
    });
  }

  // wait for the promiseQueue to resolve
  await processPromiseQueue(stack);

  // wrap the events with the known type
  return wrapTxRes(Array.from(result), provider, toBlock);
};

// pull all blocks in the requested range
const blocksFromRange = async (
  chainId: number,
  provider: JsonRpcProvider | WebSocketProvider,
  from: number,
  to: number,
  reqStack: (() => Promise<any>)[],
  result: Set<number>,
  silent: boolean
) => {
  // save to disk for future use
  const fetchAndSaveBlock = async (number: number) => {
    // fetch the block and transactions via the provider (we dont need raw details ie logBlooms here)
    const block = await provider.getBlockWithTransactions(number);
    // if we have a block
    if (block?.number) {
      // add the block to the result
      result.add(+parseInt(`${block.number}`).toString(10));
      // save the block to disk to release from mem
      await saveJSON(
        "blocks",
        `${chainId}-${parseInt(`${block.number}`).toString(10)}`,
        block as unknown as Record<string, unknown>
      );
    } else {
      // trigger error handler and retry...
      throw new Error("No block response");
    }
  };

  // iterate the ranges and collect all blocks in that range
  while (+from <= +to) {
    await new Promise<unknown>((resolve, reject) => {
      fs.readFile(
        `${cwd}blocks-${chainId}-${+from}.json`,
        "utf8",
        async (err, file) => {
          if (!err && file && file.length) {
            // store hash for result
            result.add(+from);
            // successfully loaded the result
            resolve(true);
          } else {
            // do the actual fetch
            await fetchAndSaveBlock(+from)
              .then(() => {
                resolve(true);
              })
              .catch((e) => {
                reject(e);
              });
          }
        }
      );
    }).catch(
      (function retry(attempts) {
        // logging if we keep attempting and we're not getting anything...
        if (!silent && attempts % 10 === 0) {
          console.log(`Made 10 attempts to get: ${chainId}::block::${+from}`);
        }

        // return the error handler (recursive stacking against the reqStack)
        return (): void => {
          reqStack.push(() =>
            fetchAndSaveBlock(from).catch(retry(attempts + 1))
          );
        };
      })(1) // start attempts at 1
    );
    // move from for next tick
    from = +from + 1;
  }
};

// wrap the blocks response to set and to cast type
const wrapBlockRes = async (
  entries: number[],
  provider: JsonRpcProvider | WebSocketProvider,
  toBlock: number
) => {
  return Promise.all(
    entries.map(async (entry) => {
      return {
        type: "onBlock",
        data: entry,
        number: toBlock,
        blockNumber: entry,
        chainId: provider.network.chainId,
        collectTxReceipt: false,
        collectBlock: true,
      };
    })
  );
};

// pull new events from the contract
const getNewBlocks = async (
  fromBlock: number,
  toBlock: number,
  provider: JsonRpcProvider | WebSocketProvider,
  silent: boolean
) => {
  // collect a list of block hashes then wrap the response to build a single array of happenings to process
  const result = new Set<number>();
  // get the chainId
  const { chainId } = provider.network;
  // create a new eventRange for each 500,000 blocks (to process in parallel)
  const ranges = createBlockRanges(fromBlock, toBlock, 10);
  // construct a reqStack so that we're only processing x reqs at a time
  const stack: ((reqStack: (() => Promise<any>)[]) => Promise<void>)[] = [];

  // iterate the ranges and collect all events in that range
  for (const [from, to] of ranges) {
    stack.push(async (reqStack) => {
      await blocksFromRange(
        chainId,
        provider,
        from,
        to,
        reqStack,
        result,
        silent
      );
    });
  }

  // wait for the promiseQueue to resolve
  await processPromiseQueue(stack);

  // wrap the events with the known type
  return wrapBlockRes(Array.from(result), provider, toBlock);
};

// collect events from the given range
const eventsFromRange = async (
  chainId: number,
  contract: ethers.Contract,
  event: string,
  fromBlock: number,
  toBlock: number,
  reqStack: (() => Promise<any>)[],
  result: Set<ethers.Event>,
  silent: boolean
) => {
  // use catch to check for error state and collect range by halfing if we exceed 10000 events
  const res: ethers.Event[] = await new Promise<ethers.Event[]>(
    (resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new Error("Query timeout exceeded"));
      }, 120000);

      // run the queryFilter
      contract
        .queryFilter(contract.filters[event](), fromBlock, toBlock)
        .then((results) => {
          clearTimeout(timer);
          resolve(results);
        })
        .catch((e) => {
          clearTimeout(timer);
          reject(e);
        });
    }
  ).catch(async (e) => {
    reqStack.push(async () => {
      // eslint-disable-next-line @typescript-eslint/no-use-before-define
      await cancelAndSplit(
        chainId,
        contract,
        event,
        fromBlock,
        toBlock,
        reqStack,
        result,
        silent
      )(e);
    });
    // split the req and try again
    return [];
  });

  if (res.length) {
    // // log what we've collected on this tick
    // console.log(
    //   `Got ${
    //     res.length
    //   } ${event}::${chainId} events from range: ${JSON.stringify({
    //     fromBlock,
    //     toBlock,
    //   })}`
    // );

    // add each item to the result
    res.forEach((resEvent) => {
      result.add({
        ...resEvent,
      } as ethers.Event);
    });
  }
};

// cancel this frame and split the range in two to reattempt the call
const cancelAndSplit =
  (
    chainId: number,
    contract: ethers.Contract,
    event: string,
    fromBlock: number,
    toBlock: number,
    reqStack: (() => Promise<any>)[],
    result: Set<ethers.Event>,
    silent: boolean
  ) =>
  async (e: any) => {
    // connection timeout - reattempt the same range
    if (e.code === "TIMEOUT" || e.code === "NETWORK_ERROR") {
      // reattempt without splitting...
      await eventsFromRange(
        chainId,
        contract,
        event,
        fromBlock,
        toBlock,
        reqStack,
        result,
        silent
      );
    } else if (
      toBlock - fromBlock > 1 &&
      // check the error code
      ([
        // processing response error
        "-32002",
        // check for 10000 limit (this is infura specific - we have a 10,000 event limit per req)
        "-32005",
        // if there is a server error, its probably because the range is too large
        "SERVER_ERROR",
      ].includes(e.code) ||
        e.message === "Query timeout exceeded")
      // API rate limit exceeded
    ) {
      // split the range in two and try again...
      const middle = Math.round((fromBlock + toBlock) / 2);

      // get events from two ranges and bubble the result (this will combine all results recursively and add blocks)
      await Promise.all([
        eventsFromRange(
          chainId,
          contract,
          event,
          fromBlock,
          middle,
          reqStack,
          result,
          silent
        ),
        eventsFromRange(
          chainId,
          contract,
          event,
          middle + 1,
          toBlock,
          reqStack,
          result,
          silent
        ),
      ]);
    } else {
      if (!silent) console.log("Failed unknown error", e);
      // reattempt without splitting...
      await eventsFromRange(
        chainId,
        contract,
        event,
        fromBlock,
        toBlock,
        reqStack,
        result,
        silent
      );
    }
  };

// wrap the events response to set and to cast type
const wrapEventRes = async (
  event: string,
  entries: ethers.Event[],
  provider: JsonRpcProvider | WebSocketProvider,
  toBlock: number,
  collectTxReceipt: boolean,
  collectBlock: boolean
) => {
  return Promise.all(
    entries.map(async (entry) => {
      return {
        type: event,
        data: entry,
        number: toBlock,
        blockNumber: entry.blockNumber,
        chainId: provider.network.chainId,
        collectTxReceipt,
        collectBlock,
      };
    })
  );
};

// pull new events from the contract
const getNewEvents = async (
  address: string | undefined,
  eventAbi: ethers.Contract["abi"] | undefined,
  eventName: string,
  fromBlock: number,
  toBlock: number,
  provider: JsonRpcProvider | WebSocketProvider,
  collectTxReceipt: boolean,
  collectBlock: boolean,
  silent: boolean
) => {
  // collect a list of events then map the event type to each so that we can rebuild all events into a single array later
  const result = new Set<ethers.Event>();
  // get the chainId
  const { chainId } = provider.network;
  // only proceed if the contract is known...
  if (address && eventAbi) {
    // connect to contract and filter for events to build data-set
    const contract = new ethers.Contract(address, eventAbi, provider);
    // create a new eventRange for each 500,000 blocks (to process in parallel)
    const ranges = createBlockRanges(fromBlock, toBlock, RANGE_LIMIT);
    // construct a reqStack so that we're only processing x reqs at a time
    const stack: ((reqStack: (() => Promise<any>)[]) => Promise<void>)[] = [];

    // iterate the ranges and collect all events in that range
    for (const [from, to] of ranges) {
      stack.push(async (reqStack) => {
        // what if result is just built by ref? then we dont have to await failures etc...
        await eventsFromRange(
          chainId,
          contract,
          eventName,
          from,
          to,
          reqStack,
          result,
          silent
        );
      });
    }

    // wait for the promiseQueue to resolve
    await processPromiseQueue(stack);
  }

  // wrap the events with the known type
  return wrapEventRes(
    eventName,
    Array.from(result),
    provider,
    toBlock,
    collectTxReceipt,
    collectBlock
  );
};

// this is only managing about 20k blocks before infura gets complainee
const attemptFnCall = async <T extends Record<string, any>>(
  syncProviders: Record<number, JsonRpcProvider | WebSocketProvider>,
  type: string,
  dets: {
    type: string;
    data: ethers.Event;
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

    // attempt to get the block from the connected provider first
    (attempts === 0
      ? (syncProviders[dets.chainId][fn](
          dets.data[prop] as string
        ) as unknown as Promise<T>)
      : (getRandomProvider(dets.chainId, [
          syncProviders[dets.chainId].connection.url,
        ])[fn](dets.data[prop] as string) as unknown as Promise<T>)
    )
      .then((resData: T) => {
        // if we have data...
        if (resData && resData[resProp]) {
          clearTimeout(timer);
          // write the file
          saveJSON(
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
const getFnResultForProp = async <T extends Record<string, any>>(
  events: SyncEvent[],
  filtered: SyncEvent[],
  syncProviders: Record<number, JsonRpcProvider | WebSocketProvider>,
  type: string,
  prop: keyof ethers.Event,
  fn: "getBlock" | "getTransactionReceipt",
  resProp: "timestamp" | "from",
  silent?: boolean
) => {
  // count the number of actual collections
  const result = new Set<string>();
  // place the operations into a stack and resolve them x at a time
  const stack: ((reqStack: (() => Promise<any>)[]) => Promise<void>)[] = [];

  // iterate the events and attempt to pull the associated block/transaction
  for (const dets of filtered) {
    // push function to place the selected resProp at the requested prop
    stack.push(async (reqStack) => {
      await new Promise<unknown>((resolve, reject) => {
        fs.readFile(
          `${cwd}${type}-${dets.chainId}-${
            // eslint-disable-next-line no-nested-ternary
            type === "blocks"
              ? parseInt(`${dets.blockNumber}`).toString(10)
              : typeof dets.data === "object"
              ? dets.data[prop]
              : dets.data
          }.json`,
          "utf8",
          async (err, file) => {
            if (!err && file && file.length) {
              try {
                const data = JSON.parse(file);
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
          }
        );
      })
        .catch(
          (function retry(attempts) {
            // logging if we keep attempting and we're not getting anything...
            if (!silent && attempts % 10 === 0) {
              console.log(
                `Made 10 attempts to get: ${dets.chainId}::${
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

  // process all items in the stack
  await processPromiseQueue(stack);

  // after resolution of all entries
  if (!silent && events.length)
    console.log(`All ${type} collected (${[...result].length})`);
};

// read a csv file of events
const readCSV = async (
  filePath: string
): Promise<
  {
    type: string;
    data: ethers.Event;
    chainId: number;
    number: number;
    blockNumber: number;
    collectBlock: boolean;
    collectTxReceipt: boolean;
    timestamp?: number;
    from?: string;
  }[]
> => {
  const results: {
    type: string;
    data: ethers.Event;
    chainId: number;
    number: number;
    blockNumber: number;
    collectBlock: boolean;
    collectTxReceipt: boolean;
    timestamp?: number;
    from?: string;
  }[] = [];

  return new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(csvParser())
      .on("data", (data) => {
        if (data[0] !== "type") {
          results.push({
            type: data.type,
            data: {
              blockNumber: parseInt(data.blockNumber, 10),
              blockHash: data.blockHash,
              transactionIndex: parseInt(data.transactionIndex, 10),
              removed: data.removed === "true",
              address: data.address,
              data: data.data,
              topics: data.topics?.split(",") || [],
              transactionHash: data.transactionHash,
              logIndex: parseInt(data.logIndex, 10),
              event: data.event,
              eventSignature: data.eventSignature,
              args: data.args?.split(",") || [],
            } as ethers.Event,
            collectBlock: data.collectBlock === "true",
            collectTxReceipt: data.collectTxReceipt === "true",
            chainId: parseInt(data.chainId, 10),
            number: data.number,
            blockNumber: data.blockNumber,
            timestamp: data.blockTimestamp,
            from: data.from,
          });
        }
      })
      .on("end", () => {
        resolve(results);
      })
      .on("error", (error) => {
        reject(error);
      });
  });
};

// save events to csv file
const saveCSV = async (
  csvFileName: string,
  events: {
    type: string;
    data: ethers.Event | string;
    chainId: number;
    number: number;
    blockNumber: number;
    collectBlock: boolean;
    collectTxReceipt: boolean;
    timestamp?: number;
    from?: string;
  }[]
) => {
  // createWriteStream to save this run of raw events to a file
  const writableStream = fs.createWriteStream(csvFileName);

  // set up the csv document
  const writer = csvWriter({
    sendHeaders: true,
    headers: [
      "type",
      "collectBlock",
      "collectTxReceipt",
      "blockTimestamp",
      "blockNumber",
      "blockHash",
      "transactionIndex",
      "removed",
      "address",
      "data",
      "topics",
      "transactionHash",
      "logIndex",
      "event",
      "eventSignature",
      "args",
      "number",
      "from",
      "chainId",
    ],
  });

  // write to a new document each run
  const savingProcess = new Promise((resolve) => {
    writer.pipe(writableStream).on("finish", () => resolve(true));
  });

  // stream writes to the csv document
  events.forEach(
    (event: {
      type: string;
      data: ethers.Event | string;
      chainId: number;
      number: number;
      blockNumber: number;
      collectBlock: boolean;
      collectTxReceipt: boolean;
      timestamp?: number;
      from?: string;
    }) => {
      const txData = typeof event.data === "string" ? ({} as any) : event.data;
      // write all values against the headered column
      writer.write({
        type: event.type,
        collectBlock: event.collectBlock,
        collectTxReceipt: event.collectTxReceipt,
        blockTimestamp: event.timestamp,
        blockNumber: txData.blockNumber,
        blockHash: txData.blockHash,
        transactionIndex: txData.transactionIndex,
        removed: txData.removed,
        address: txData.address,
        data: txData.data || event.data,
        topics: txData.topics,
        transactionHash: txData.transactionHash,
        logIndex: txData.logIndex,
        event: txData.event,
        eventSignature: txData.eventSignature,
        args: txData.args,
        number: event.number,
        from: event.from,
        chainId: event.chainId,
      });
    }
  );

  // close the stream
  writer.end();

  // wait for the process to complete
  await savingProcess;
};

// get all networks chainIds
const getNetworks = async () => {
  // chainIds visited in the process
  const chainIds: Set<number> = new Set<number>();
  const syncProviders: Record<number, JsonRpcProvider | WebSocketProvider> = {};

  // get all network descriptions now...
  await Promise.all(
    syncs.map(async (syncOp) => {
      // get the network from the provider
      const { chainId } = await syncOp.provider.getNetwork();
      // record the chainId
      chainIds.add(chainId);
      // record the provider for the chain
      syncProviders[chainId] = syncOp.provider;
    })
  );

  return {
    chainIds,
    syncProviders,
  };
};

// sort the syncs into an appropriate order
const sortSyncs = () => {
  // sort the operations by chainId then eventName (asc then desc)
  return syncs.sort((a, b) => {
    // process lower chainIds first
    return a.provider.network.chainId - b.provider.network.chainId;
  });
};

// retrieve all events from all syncs from last-run to now
const getNewSyncEvents = async (
  eventIfaces: Record<string, ethers.utils.Interface>,
  latestEntity: LatestEntity,
  callbacks: HandlerFns,
  startBlocks: Record<string, number>,
  latestBlocks: Record<number, Block>,
  collectAnyBlocks: boolean,
  collectAnyTxReceipts: boolean,
  collectBlocks: boolean,
  collectTxReceipts: boolean,
  cleanup?: boolean,
  silent?: boolean,
  start?: keyof typeof Stage | false | undefined
) => {
  // record when we started this operation
  const startTime = performance.now();

  // collate the addresses we use in the processing
  const addresses: Record<string, string> = {};

  // retrieve the engine backing the Store
  const engine = await getEngine();

  // pull the sorted syncs
  const syncOps = sortSyncs();

  // boolean to allow the lock to pass
  let clearLock = false;
  let cancelled = false;

  // record locks by chainId
  const locked: Record<number, boolean> = {};
  const blocked: Record<number, number> = {};
  const checked: Record<number, Promise<boolean>> = {};

  // collate all events from all sync operations
  let events: {
    type: string;
    data: ethers.Event;
    chainId: number;
    number: number;
    blockNumber: number;
    collectTxReceipt: boolean;
    collectBlock: boolean;
    timestamp?: number;
    from?: string;
  }[] = [];

  // we're starting the process here then print the discovery
  if (!silent && (!start || (start && Stage[start] < Stage.process))) {
    // detail everything which was loaded in this run
    console.log("\n\nStarting sync by collecting events...\n\n--\n");
  }

  // process all requests at once
  await Promise.all(
    syncOps.map(async (opSync) => {
      // check that the operation hasnt been cancelled
      if (!cancelled) {
        // hydrate the events into this var
        let newEvents: any = [];

        // extract info from the sync operation
        const {
          address,
          eventAbi,
          eventName,
          provider,
          startBlock,
          opts,
          onEvent,
        } = opSync;
        // get this chains id
        const { chainId } = provider.network;

        // check locked state - we only want to check this lock once per chainId
        if (!Object.hasOwnProperty.call(checked, chainId)) {
          // mark as seen
          checked[chainId] = Promise.resolve().then(async () => {
            // set the chainId into the engine
            Store.setChainId(chainId);
            // clear the current block state so we don't disrupt sync timestamps on the entity
            Store.clearBlock();
            // fetch the meta entity from the store (we'll update this once we've committed all changes)
            latestEntity[chainId] = await Store.get<{
              id: string;
              latestBlock: number;
              latestBlockTime: number;
              locked: boolean;
              lockedAt: number;
              _block_num: number;
              _block_ts: number;
            }>("__meta__", `${chainId}`); // if this is null but we have rows in the collection, we could select the most recent and check the lastBlock

            // make sure this lines up with what we expect
            latestEntity[chainId].chainId = chainId;
            latestEntity[chainId].block = undefined;

            // hydrate the locked bool
            locked[chainId] = latestEntity[chainId]?.locked;
            // check when the lock was instated
            blocked[chainId] =
              latestEntity[chainId]?.lockedAt ||
              latestEntity[chainId]?.latestBlock ||
              0;
            // toBlock is always "latest" from when we collected the events
            latestBlocks[chainId] = await provider.getBlock("latest");

            // when the db is locked check if it should be released before ending all operations
            if (
              !engine.newDb &&
              locked[chainId] &&
              // this offset should be chain dependent
              +blocked[chainId] + LOCKED_FOR >
                (+latestBlocks[chainId].number || 0) &&
              // if the lock hasnt been cleared by another prov
              !clearLock &&
              !cancelled
            ) {
              // mark as cancelled
              cancelled = true;

              // mark the operation in the log
              if (!silent)
                console.log("Sync error - chainId ", chainId, " is locked");

              // record when we finished the sync operation
              const endTime = performance.now();

              // print time in console
              if (!silent)
                console.log(
                  `\n===\n\nTotal execution time: ${(
                    Number(endTime - startTime) / 1000
                  ).toPrecision(4)}s\n\n`
                );

              // return error state to caller
              throw new Error("DB is locked");
            }

            // if the blocked frame has passed on this provider we can clear locks for other providers
            if (
              +blocked[chainId] + LOCKED_FOR <=
              (+latestBlocks[chainId].number || 0)
            ) {
              clearLock = true;
            }

            // set the lock (this will be released on successful update)
            latestEntity[chainId].set("locked", true);
            // move the lockedAt to now to prevent adjacent runs unlocking at the same time
            latestEntity[chainId].set(
              "lockedAt",
              +latestBlocks[chainId].number
            );

            // save the new locked state on the chainId (this will save immediately - we're not inside a checkpoint)
            latestEntity[chainId] = await latestEntity[chainId].save();

            // bool on comp
            return true;
          });
        }

        // await the check
        await checked[chainId];

        // record the checksum address
        addresses[address || chainId] =
          (address && getAddress(address)) || chainId.toString();

        // record the eventNames callback to execute on final sorted list
        callbacks[`${addresses[address || chainId]}-${eventName}`] =
          callbacks[`${addresses[address || chainId]}-${eventName}`] || onEvent;

        // when provided an address - we're mapping a contract...
        if (address && eventAbi) {
          // record the event interface so we can reconstruct args to feed to callback
          eventIfaces[`${addresses[address || chainId]}-${eventName}`] =
            eventIfaces[`${addresses[address || chainId]}-${eventName}`] ||
            new ethers.utils.Interface(eventAbi);
        }

        // check if we should be pulling events in this sync or using the tmp cache
        if (
          (!start ||
            Stage[start] === Stage.events ||
            !fs.existsSync(
              `${cwd}events-latestRun-${
                addresses[address || chainId]
              }-${eventName}-${chainId}.csv`
            )) &&
          !cancelled
        ) {
          // start the run from the blocks
          if (start && Stage[start] > Stage.events) {
            start = "blocks";
          }
          // set the block frame (go back a block as a safety check (1 confirmation))
          const toBlock = +latestBlocks[chainId].number - 1;
          const fromBlock =
            +latestEntity[chainId].latestBlock + 1 ||
            (startBlock === "latest"
              ? +latestBlocks[chainId].number - 1
              : startBlock);

          // mark engine as newDb
          if (!latestEntity[chainId].latestBlock) {
            engine.newDb = true;
          }

          // record the startBlock
          startBlocks[chainId] = fromBlock;

          // mark the operation in the log
          if (!silent)
            console.log(
              `Start sync ${eventName}::${provider.network.name}(${chainId})${
                address ? `::${address}` : ``
              } → ${JSON.stringify({ fromBlock, toBlock })}`
            );

          // greater than the prev fromBlock
          if (address && toBlock >= fromBlock) {
            // all new events to be processed in this sync (only make the req if we will be querying a new block)
            newEvents = await getNewEvents(
              address,
              eventAbi,
              eventName,
              fromBlock,
              toBlock,
              provider,
              opts?.collectTxReceipts || collectTxReceipts || false,
              opts?.collectBlocks || collectBlocks || false,
              !!silent
            );
          } else if (eventName === "onTransaction" && toBlock >= fromBlock) {
            // get new events using getBlockAndTransactions - onTransaction will always be triggered first - save the blocks as we process them
            newEvents = await getNewTransactions(
              fromBlock,
              toBlock,
              provider,
              opts?.collectTxReceipts || collectTxReceipts || false,
              !!silent
            );
          } else if (eventName === "onBlock" && toBlock >= fromBlock) {
            // get new event for each block in the range (will use data pulled when doing onTransaction if opSync for onTransaction exists)
            newEvents = await getNewBlocks(
              fromBlock,
              toBlock,
              provider,
              !!silent
            );
          }

          // don't save if we're just going to delete it
          if (!cleanup) {
            // record the entities run so we can step back to this spot
            await saveCSV(
              `${cwd}events-latestRun-${
                addresses[address || chainId]
              }-${eventName}-${chainId}.csv`,
              newEvents
            );
          }
        } else if (
          (!start ||
            Stage[start] < Stage.process ||
            (!fs.existsSync(`${cwd}events-latestRun-allData.csv`) &&
              fs.existsSync(
                `${cwd}events-latestRun-${
                  addresses[address || chainId]
                }-${eventName}-${chainId}.csv`
              ))) &&
          !cancelled
        ) {
          // assume that we're starting a fresh db
          engine.newDb = true;
          // start the run from the blocks
          if (start && Stage[start] >= Stage.process) {
            start = "blocks";
          }
          // read in events from disk
          newEvents = await readCSV(
            `${cwd}events-latestRun-${
              addresses[address || chainId]
            }-${eventName}-${chainId}.csv`
          );
        }

        // append any newly discovered events to the collection
        if (!cancelled) {
          // record the fully resolved discovery
          if (!silent)
            console.log(
              `  End sync ${eventName}::${provider.network.name}(${chainId})${
                address ? `::${address}` : ``
              } → new events:`,
              newEvents.length
            );
          // push all new events to the event log
          events = events.concat(newEvents);
        }
      }
    })
  ).catch((e) => {
    // throw in outer context
    throw e;
  });

  // if we're starting the process here then print the discovery
  if (!silent && (!start || (start && Stage[start] < Stage.process))) {
    // detail everything which was loaded in this run
    console.log(
      "\n--\n\nTotal no. events discovered:",
      events.length,
      (collectAnyBlocks || collectAnyTxReceipts) && events.length
        ? "\n\n--\n"
        : ""
    );
  }

  // return all the events and identified mappings
  return {
    events,
    startBlocks,
    eventIfaces,
    callbacks,
    latestEntity,
  };
};

// retrieve all requested blocks to disk
const getNewSyncEventsBlocks = async (
  events: SyncEvent[],
  syncProviders: Record<number, JsonRpcProvider | WebSocketProvider>,
  collectAnyBlocks?: boolean,
  silent?: boolean,
  start?: keyof typeof Stage | false,
  stop?: keyof typeof Stage | false
) => {
  // check if we're gathering blocks in this sync
  if (
    collectAnyBlocks &&
    (!start || Stage[start] <= Stage.sort) &&
    (!stop || Stage[stop] >= Stage.blocks)
  ) {
    // extract blocks and call getBlock once for each discovered block - then index against blocks number
    await getFnResultForProp<Block>(
      events,
      events,
      syncProviders,
      "blocks",
      "blockNumber",
      "getBlock",
      "timestamp",
      silent
    );
  }
};

// retrieve all requested tx receipts to disk
const getNewSyncEventsTxReceipts = async (
  events: SyncEvent[],
  syncProviders: Record<number, JsonRpcProvider | WebSocketProvider>,
  collectAnyTxReceipts?: boolean,
  silent?: boolean,
  start?: keyof typeof Stage | false,
  stop?: keyof typeof Stage | false
) => {
  // check if we're gathering transactions in this sync
  if (
    collectAnyTxReceipts &&
    (!start || Stage[start] <= Stage.sort) &&
    (!stop || Stage[stop] >= Stage.transactions)
  ) {
    const filtered = events.filter((evt) => !!evt.collectTxReceipt);

    // extract transactions and call getTransactionReceipt once for each discovered tx
    await getFnResultForProp<TransactionReceipt>(
      events,
      filtered,
      syncProviders,
      "transactions",
      "transactionHash",
      "getTransactionReceipt",
      "from",
      silent
    );
  }
};

// sort the events into processing order
const getNewSyncEventsSorted = async (
  events: SyncEvent[],
  collectBlocks?: boolean,
  cleanup?: boolean,
  silent?: boolean,
  start?: keyof typeof Stage | false,
  stop?: keyof typeof Stage | false
) => {
  // open a checkpoint on the db...
  const engine = await getEngine();

  // sorted has same type as events
  let sorted: typeof events = [];

  // check if we're performing a sort in this sync
  if (
    (!start || Stage[start] <= Stage.sort) &&
    (!stop || Stage[stop] >= Stage.sort)
  ) {
    // sort the events by block timestamp then tx order
    sorted = events.sort((a, b) => {
      // sort on blockNumber first
      const blockSort = collectBlocks
        ? (a.timestamp || 0) - (b.timestamp || 0)
        : (a.data.blockNumber || 0) - (b.data.blockNumber || 0);
      // sort blocks/txs to top
      const txSort =
        (a.data?.transactionIndex || 0) - (b.data?.transactionIndex || 0);
      const logSort = (a.data?.logIndex || 0) - (b.data?.logIndex || 0);

      // bail outs to move tx/block handler calls to the top
      if (!blockSort && !txSort && typeof a.data === "string") {
        return -1;
      }
      if (!blockSort && !txSort && typeof b.data === "string") {
        return 1;
      }

      // if blockNumbers match sort on txIndex then logIndex
      // eslint-disable-next-line no-nested-ternary
      return blockSort !== 0 ? blockSort : txSort !== 0 ? txSort : logSort;
    });

    // don't save if we're just going to delete it...
    if (!cleanup) {
      // save all events to disk
      await saveCSV(`${cwd}events-latestRun-allData.csv`, sorted);
    }

    // sorted by timestamp
    if (!silent && sorted.length)
      console.log("\n--\n\nEvents sorted", sorted.length);
  }

  // check if we start on "process" in this sync and pull all the sorted content from the tmp store
  if (start && Stage[start] === Stage.process) {
    // log the start of the operation
    if (!silent)
      console.log(
        "\n\nStarting sync by restoring last-runs sorted events...\n\n--\n"
      );

    // assume that we're starting a fresh db
    engine.newDb = true;

    // restore the sorted collection
    sorted = await readCSV(`${cwd}events-latestRun-allData.csv`);

    // detail everything which was loaded in this run
    if (!silent) console.log("Total no. events discovered:", sorted.length);
  }

  return sorted;
};

// update the pointers to reflect latest sync
const updateSyncPointers = async (
  sorted: SyncEvent[],
  listen: boolean,
  chainIds: Set<number>,
  syncProviders: Record<number, JsonRpcProvider | WebSocketProvider>,
  startBlocks: Record<string, number>,
  latestEntity: LatestEntity,
  chainUpdates: string[]
) => {
  // commit an update for each provider used in the sync (these are normal db.put's to always write to the same entries)
  await Promise.all(
    Array.from(chainIds).map(async (chainId) => {
      // we want to select the latest block this chains events
      const chainsEvents: typeof sorted = sorted.filter(
        (ev) => ev.chainId === chainId
      );
      // get the last entry in this array
      const chainsLatestBlock: (typeof sorted)[0] =
        chainsEvents[chainsEvents.length - 1];

      // extract latest update (only move the latestBlock when fromBlock !== toBlock)
      const latestBlockNumber =
        chainsLatestBlock?.data.blockNumber ||
        chainsLatestBlock?.blockNumber ||
        startBlocks[chainId];
      // if timestamp isnt available default to using blockNumber in its place
      const latestTimestamp = chainsLatestBlock?.timestamp || latestBlockNumber;

      // grab from and to blocks
      const fromBlock = +startBlocks[chainId];
      const toBlock = +(latestBlockNumber || startBlocks[chainId]);

      // log the pointer update we're making with this sync
      chainUpdates.push(
        `Chain pointer update on ${
          syncProviders[chainId].network.name
        } (chainId: ${chainId}) → ${JSON.stringify({
          fromBlock,
          // if we didn't find any events then we can keep the toBlock as startBlock
          toBlock,
          // this will be the starting point for the next block
          nextBlock:
            fromBlock < toBlock ||
            (fromBlock === toBlock && chainsEvents.length > 0)
              ? toBlock + 1
              : toBlock,
        })}`
      );

      // set the chainId into the engine
      latestEntity[chainId].chainId = chainId;

      // don't record a move if we've havent found anything to warrant a move
      if (
        (fromBlock < toBlock || fromBlock === toBlock) &&
        chainsEvents.length
      ) {
        // take a copy of the latest entity to refresh chainId & block pointers
        latestEntity[chainId].block = {
          // we add 1 here so that the next sync starts beyond the last block we processed (handlers are not idempotent - we must make sure we only process each event once)
          number: +latestBlockNumber,
          timestamp: +latestTimestamp,
        } as unknown as Block;

        // set the latest entry
        latestEntity[chainId].set("latestBlock", +latestBlockNumber);
        latestEntity[chainId].set("latestBlockTime", +latestTimestamp);
      } else {
        // clear the block to avoid altering _block_num and _block_ts on this run
        latestEntity[chainId].block = undefined;
      }

      // when listening we can leave this locked until later
      if (!listen) {
        // remove the lock for the next iteration
        latestEntity[chainId].set("locked", false);
      }

      // persist changes into the store
      latestEntity[chainId] = await latestEntity[chainId].save();
    })
  );
};

// reset locks on all chains
const releaseSyncPointerLocks = async (
  chainIds: Set<number>,
  latestEntity?: LatestEntity
) => {
  // ensure latest entity is defined
  if (!latestEntity) {
    // default to obj
    latestEntity = {};
    // fill with latest entities
    await Promise.all(
      Array.from(chainIds).map(async (chainId) => {
        Store.clearBlock();
        Store.setChainId(chainId);
        latestEntity![chainId] = await Store.get<{
          id: string;
          latestBlock: number;
          latestBlockTime: number;
          locked: boolean;
          lockedAt: number;
          _block_num: number;
          _block_ts: number;
        }>("__meta__", `${chainId}`);
      })
    );
  }
  // otherwise release locks because we'e not altering db state
  await Promise.all(
    Array.from(chainIds).map(async (chainId) => {
      // set the chainId against the entity
      latestEntity![chainId].chainId = chainId;
      // clear the block to avoid changing update times
      latestEntity![chainId].block = {} as Block;
      // remove the lock for the next iteration
      latestEntity![chainId].set("locked", false);
      // persist changes into the store
      latestEntity![chainId] = await latestEntity![chainId].save(false);
    })
  );
};

// process the sorted events via the sync handler callbacks
const doCleanup = async (
  events: SyncEvent[],
  silent?: boolean,
  start?: keyof typeof Stage | false,
  stop?: keyof typeof Stage | false
) => {
  // check if we're cleaning up in this sync
  if (
    (!start || Stage[start] <= Stage.process) &&
    (!stop || Stage[stop] >= Stage.process)
  ) {
    // log that we're starting
    if (!silent) process.stdout.write(`Cleanup tmp storage `);

    // iterate the events and delete all blocks and txs from this run
    await Promise.all(
      events.map(async (event) => {
        // make sure we've correctly discovered an iface
        if (typeof event.data === "object" && event.data.address) {
          // delete tx from tmp storage
          await deleteJSON(
            "transactions",
            `${event.chainId}-${event.data.transactionHash}`
          );
          // delete block from tmp storage
          await deleteJSON(
            "blocks",
            `${event.chainId}-${parseInt(`${event.data.blockNumber}`).toString(
              10
            )}`
          );
        } else if (event.type === "onBlock") {
          // delete block from tmp storage
          await deleteJSON(
            "blocks",
            `${event.chainId}-${parseInt(`${event.data}`).toString(10)}`
          );
        } else if (event.type === "onTransaction") {
          // delete tx from tmp storage
          await deleteJSON("transactions", `${event.chainId}-${event.data}`);
          // delete block from tmp storage
          await deleteJSON(
            "blocks",
            `${event.chainId}-${parseInt(`${event.blockNumber}`).toString(10)}`
          );
        }
      })
    );
  }

  // mark as completed in stdout
  if (!silent) process.stdout.write("✔\n");
};

// process the sorted events via the sync handler callbacks
const processEvents = async (
  sorted: SyncEvent[],
  chainIds: Set<number>,
  syncProviders: Record<number, JsonRpcProvider | WebSocketProvider>,
  eventIfaces: Record<string, ethers.utils.Interface>,
  latestEntity: LatestEntity,
  callbacks: HandlerFns,
  startBlocks: Record<string, number>,
  listen: boolean,
  collectBlocks?: boolean,
  collectTxReceipts?: boolean,
  cleanup?: boolean,
  silent?: boolean,
  start?: keyof typeof Stage | false,
  stop?: keyof typeof Stage | false
) => {
  // open a checkpoint on the db...
  const engine = await getEngine();

  // check if we're finalising the process in this sync
  if (
    sorted.length &&
    (!start || Stage[start] <= Stage.process) &&
    (!stop || Stage[stop] >= Stage.process)
  ) {
    // store the updates until we've completed the call
    const chainUpdates: string[] = [];

    // create a checkpoint
    engine?.stage?.checkpoint();

    // log that we're starting
    if (!silent) process.stdout.write(`\n--\n\nEvents processed `);

    // iterate the sorted events and process the callbacks with the given args (sequentially)
    for (const opSorted of sorted) {
      // get an interface to parse the args
      const iface =
        eventIfaces[
          `${
            (opSorted.data.address && getAddress(opSorted.data.address)) ||
            opSorted.chainId
          }-${opSorted.type}`
        ];

      // make sure we've correctly discovered an iface
      if (iface) {
        // extract the args
        const { args } = iface.parseLog({
          topics: opSorted.data.topics,
          data: opSorted.data.data,
        });
        // transactions can be skipped if we don't need the details in our sync handlers
        const tx =
          !collectTxReceipts && !opSorted.collectTxReceipt
            ? ({
                contractAddress: opSorted.data.address,
                transactionHash: opSorted.data.transactionHash,
                transactionIndex: opSorted.data.transactionIndex,
                blockHash: opSorted.data.blockHash,
                blockNumber: opSorted.data.blockNumber,
              } as unknown as TransactionReceipt)
            : await readJSON<TransactionReceipt>(
                "transactions",
                `${opSorted.chainId}-${opSorted.data.transactionHash}`
              ).then(async (file) => {
                if (cleanup) {
                  await deleteJSON(
                    "transactions",
                    `${opSorted.chainId}-${opSorted.data.transactionHash}`
                  );
                }
                return file;
              });
        // block can also be summised from opSorted if were not collecting blocks for this run
        const block =
          !collectBlocks && !opSorted.collectBlock
            ? ({
                hash: opSorted.data.blockHash,
                number: opSorted.data.blockNumber,
                timestamp: opSorted.timestamp || opSorted.data.blockNumber,
              } as unknown as Block)
            : await readJSON<Block>(
                "blocks",
                `${opSorted.chainId}-${parseInt(
                  `${opSorted.data.blockNumber}`
                ).toString(10)}`
              );
        // set the chainId into the engine - this prepares the Store so that any new entities are constructed against these details
        Store.setChainId(opSorted.chainId);
        // set the block for each operation into the runtime engine before we run the handler
        Store.setBlock(
          block && block.timestamp
            ? block
            : ({
                timestamp: opSorted.timestamp || opSorted.data.blockNumber,
                number: opSorted.data.blockNumber,
              } as Block)
        );
        // await the response of the handler before moving on to the next operation in the sorted ops
        await callbacks[
          `${
            (opSorted.data.address && getAddress(opSorted.data.address)) ||
            opSorted.chainId
          }-${opSorted.type}`
        ](
          // pass the parsed args construct
          args,
          // read tx and block from file (this avoids filling the memory with blocks/txs as we collect them - in prod we store into /tmp/)
          {
            tx: tx as TransactionReceipt & TransactionResponse,
            block,
            logIndex: opSorted.data.logIndex,
          }
        );
      } else if (opSorted.type === "onBlock") {
        // get block from tmp storage
        const block = await readJSON<Block>(
          "blocks",
          `${opSorted.chainId}-${parseInt(`${opSorted.blockNumber}`).toString(
            10
          )}`
        );
        // set the chainId into the engine
        Store.setChainId(opSorted.chainId);
        // set the block for each operation into the runtime engine before we run the handler
        Store.setBlock(block);
        // await the response of the handler before moving to the next operation in the sorted ops
        await callbacks[`${opSorted.chainId}-${opSorted.type}`](
          // pass the parsed args construct
          [],
          // read tx and block from file (this avoids filling the memory with blocks/txs as we collect them - in prod we store into /tmp/)
          {
            tx: {} as unknown as TransactionReceipt & TransactionResponse,
            block,
            logIndex: -1,
          }
        );
      } else if (opSorted.type === "onTransaction") {
        // get tx from tmp storage
        const tx = await readJSON<TransactionResponse>(
          "transactions",
          `${opSorted.chainId}-${opSorted.data}`
        );
        // get the tx and block from tmp storage
        const block = await readJSON<Block>(
          "blocks",
          `${opSorted.chainId}-${parseInt(`${opSorted.blockNumber}`).toString(
            10
          )}`
        );
        // set the chainId into the engine
        Store.setChainId(opSorted.chainId);
        // set the block for each operation into the runtime engine before we run the handler
        Store.setBlock(block);
        // await the response of the handler before moving to the next operation in the sorted ops
        await callbacks[
          `${
            (opSorted.data.address && getAddress(opSorted.data.address)) ||
            opSorted.chainId
          }-${opSorted.type}`
        ](
          // pass the parsed args construct
          [],
          // read tx and block from file (this avoids filling the memory with blocks/txs as we collect them - in prod we store into /tmp/)
          {
            tx: tx as TransactionReceipt & TransactionResponse,
            block,
            logIndex: -1,
          }
        );
      }
    }

    // print the number of processed events
    if (!silent) process.stdout.write(`(${sorted.length}) `);

    // await all promises that have been enqueued during execution of callbacks (this will be cleared afterwards ready for the next run)
    await processPromiseQueue(promiseQueue);

    // mark after we end
    if (!silent) process.stdout.write("✔\nEntities stored ");

    // commit the checkpoint on the db...
    await engine?.stage?.commit();

    // no longer a newDB after committing changes
    engine.newDb = false;

    // after commit all events are stored in db
    if (!silent) process.stdout.write("✔\nPointers updated ");

    // update the pointers to reflect the latest sync
    await updateSyncPointers(
      sorted,
      listen,
      chainIds,
      syncProviders,
      startBlocks,
      latestEntity,
      chainUpdates
    );

    // finished after updating pointers
    if (!silent) process.stdout.write("✔\n");

    // do cleanup stuff...
    if (cleanup) {
      // rm the tmp dir between runs
      await doCleanup(sorted, silent, start, stop);
    }

    // place some space before the updates
    if (!silent) console.log("\n--\n");

    // log each of the chainUpdate messages
    if (!silent)
      chainUpdates.forEach((msg) => {
        console.log(msg);
      });
  } else {
    // reset all locks
    await releaseSyncPointerLocks(chainIds, latestEntity);
  }
};

// process a new block as it arrives
const processBlock = async (
  chainId: number,
  validOps: Record<number, Sync[]>,
  blockNumber: number,
  collectBlocks: boolean,
  collectTxReceipts: boolean,
  silent: boolean,
  syncProviders: Record<number, JsonRpcProvider | WebSocketProvider>,
  eventIfaces: Record<string, ethers.utils.Interface>,
  latestEntity: LatestEntity,
  callbacks: HandlerFns,
  queueLength: number,
  asyncParts: Promise<{
    cancelled?: boolean;
    block: BlockWithTransactions & any;
    receipts: Record<string, TransactionReceipt>;
  }>
) => {
  // open a checkpoint on the db...
  const engine = await getEngine();

  // record events before making callbacks
  const events: (Sync & {
    id: string;
    chainId: number;
    timestamp?: number;
    data: {
      blockNumber: number;
    };
    blockNumber: number;
    eventName: string;
    logIndex: number;
    args: ethers.utils.Result;
    tx: TransactionReceipt & TransactionResponse;
    txIndex: number;
  })[] = [];

  // open a new checkpoint whenever we open without one (this will preserve all changes in mem until we get to queueLength === 0)
  if (engine?.stage?.checkpoints.length === 0) {
    engine?.stage?.checkpoint();
  }

  // log that we're syncing the block
  if (!silent)
    console.log(
      `\n--\n\nSyncing block ${blockNumber} (${queueLength} in queue) from ${syncProviders[chainId].network.name} (chainId: ${chainId})`
    );

  // log that we're starting
  if (!silent) process.stdout.write(`\nEvents processed `);

  // await obj containing parts
  const parts = await asyncParts;

  // unpack the async parts  - we need the receipts to access the logBlooms (if we're only doing onBlock/onTransaction we dont need to do this unless collectTxReceipts is true)
  const { block, receipts, cancelled } = parts;

  // set the chainId into the engine
  Store.setChainId(chainId);
  // set the block for each operation into the engine before we run the handler
  Store.setBlock(block);

  // check that we havent cancelled this operation
  if (!cancelled) {
    // run through the ops and extract all events happening in this block to be sorted into logIndex order
    for (const op of validOps[chainId]) {
      // check for block/tx/event by eventName (and check for matching callback)
      if (
        op.eventName === "onBlock" &&
        callbacks[`${chainId}-${op.eventName}`]
      ) {
        // record the event
        events.push({
          ...op,
          id: `${chainId}`,
          chainId,
          timestamp: collectBlocks && block.timestamp,
          data: {
            blockNumber: block.number,
          },
          blockNumber: block.number,
          eventName: op.eventName,
          args: [],
          tx: {} as TransactionReceipt & TransactionResponse,
          // onBlock first
          txIndex: -2,
          logIndex: -2,
        });
      } else if (
        op.eventName === "onTransaction" &&
        callbacks[`${chainId}-${op.eventName}`]
      ) {
        // create a new event for every transaction in the block
        for (const tx of block.transactions) {
          // record the event
          events.push({
            ...op,
            id: `${chainId}`,
            chainId,
            timestamp: collectBlocks && block.timestamp,
            data: {
              blockNumber: block.number,
            },
            blockNumber: block.number,
            eventName: op.eventName,
            args: [],
            tx: {
              ...tx,
              ...(collectTxReceipts || op.opts?.collectTxReceipts
                ? receipts[tx.hash]
                : {}),
            },
            // onTx first out of tx & events
            txIndex: tx.transactionIndex,
            logIndex: -1,
          });
        }
      } else if (
        op.address &&
        callbacks[`${getAddress(op.address)}-${op.eventName}`]
      ) {
        // check for a matching topic in the transactions logBloom
        const iface = eventIfaces[`${getAddress(op.address)}-${op.eventName}`];
        const topic = iface.getEventTopic(op.eventName);
        const hasEvent =
          isTopicInBloom(block.logsBloom, topic) &&
          isContractAddressInBloom(block.logsBloom, op.address);

        // check for logs on the block
        if (hasEvent) {
          // now we need to find the transaction that created this logEvent
          for (const tx of block.transactions) {
            // check if the tx has the event...
            const txHasEvent =
              isTopicInBloom(receipts[tx.hash].logsBloom, topic) &&
              isContractAddressInBloom(receipts[tx.hash].logsBloom, op.address);
            // check for logs on the tx
            if (txHasEvent) {
              // check each log for a match
              for (const log of receipts[tx.hash].logs) {
                if (
                  log.topics[0] === topic &&
                  getAddress(log.address) === getAddress(op.address)
                ) {
                  // find the args for the matching log item
                  const { args } = iface.parseLog({
                    topics: log.topics,
                    data: log.data,
                  });
                  // record the event
                  events.push({
                    ...op,
                    id: `${getAddress(op.address)}`,
                    chainId,
                    timestamp: collectBlocks && block.timestamp,
                    data: {
                      blockNumber: block.number,
                    },
                    blockNumber: block.number,
                    eventName: op.eventName,
                    args,
                    tx: {
                      ...tx,
                      ...(collectTxReceipts || op.opts?.collectTxReceipts
                        ? receipts[tx.hash]
                        : {}),
                    },
                    // order as defined
                    txIndex: tx.transactionIndex,
                    logIndex:
                      typeof log.logIndex === "undefined"
                        ? (log as any).index
                        : log.logIndex,
                  });
                }
              }
            }
          }
        }
      }
    }

    // make sure we haven't been cancelled before we get here
    if (!parts.cancelled) {
      // print number of events in stdout
      if (!silent) process.stdout.write(`(${events.length}) `);

      // sort the events
      const sorted = events.sort((a, b) => {
        // check the transaction order of the block
        const txOrder = a.txIndex - b.txIndex;

        // sort the events by logIndex order (with onBlock and onTransaction coming first)
        return txOrder === 0 ? a.logIndex - b.logIndex : txOrder;
      });

      // checkpoint only these writes
      engine?.stage?.checkpoint();

      // for each of the sync events call the callbacks sequentially
      for (const event of sorted) {
        if (!parts.cancelled) {
          // check for block or tx type handlers
          const isBlockOrTxEv =
            ["onBlock", "onTransaction"].indexOf(event.eventName) === 1;

          // check for block/tx/event by eventName (and check for matching callback)
          if (isBlockOrTxEv && callbacks[`${event.id}-${event.eventName}`]) {
            // process each block/tx event the same way (logIndex: -1)
            await callbacks[`${event.id}-${event.eventName}`](event.args, {
              block,
              tx: event.tx,
              logIndex: -1,
            });
          } else if (
            !isBlockOrTxEv &&
            callbacks[`${event.id}-${event.eventName}`]
          ) {
            // process callBack with its true logIndex
            await callbacks[`${event.id}-${event.eventName}`](event.args, {
              block,
              tx: event.tx,
              logIndex: event.logIndex,
            });
          }
        }
      }

      // commit or revert
      if (!parts.cancelled) {
        // await all promises that have been enqueued during execution of callbacks (this will be cleared afterwards ready for the next run)
        await processPromiseQueue(promiseQueue);
        // move thes changes to the parent checkpoint
        await engine?.stage?.commit();
      } else {
        // clear the promiseQueue
        promiseQueue.slice(0, promiseQueue.length);
        // revert this checkpoint we're not saving it
        engine?.stage?.revert();
      }

      // if we havent been cancelled up to now we can commit this
      if (!parts.cancelled) {
        // only attempt to save changes when the queue is clear (or it has been 15s since we last stored changes)
        if (
          queueLength === 0 ||
          (engine?.lastUpdate || 0) + 15000 <= new Date().getTime()
        ) {
          // mark after we end the processing
          if (!silent) {
            process.stdout.write(`✔\nEntities stored `);
          }
          // commit the checkpoint on the db...
          await engine?.stage?.commit();

          // after all events are stored in db
          if (!silent) process.stdout.write("✔\nPointers updated ");

          // update the lastUpdateTime (we have written everything to db - wait a max of 15s before next update)
          engine.lastUpdate = new Date().getTime();
        }

        // update the pointers to reflect the latest sync
        await updateSyncPointers(
          // these events follow enough to pass as SyncEvents
          sorted as unknown as SyncEvent[],
          true,
          new Set([chainId]),
          syncProviders,
          // this should be startBlocks
          {
            [chainId]: block.number,
          },
          latestEntity,
          []
        );

        // finished after updating pointers
        if (!silent) process.stdout.write(`✔\n`);
      }
    }
  }
};

// attach event listeners (listening for blocks)
const attachListeners = async (
  syncOps: Sync[],
  syncProviders: Record<number, JsonRpcProvider | WebSocketProvider>,
  eventIfaces: Record<string, ethers.utils.Interface>,
  latestEntity: LatestEntity,
  callbacks: HandlerFns,
  startBlocks: Record<string, number>,
  latestBlocks: Record<number, Block>,
  state: {
    inSync: boolean;
    listening?: boolean;
  } = {
    inSync: true,
    listening: true,
  },
  handlers: {
    reject?: (reason?: any) => void;
    resolve?: (value: void | PromiseLike<void>) => void;
  } = {},
  collectBlocks = false,
  collectTxReceipts = false,
  silent = false
) => {
  // array of blocks that need to be processed...
  const blocks: {
    chainId: number;
    number: number;
    asyncParts: Promise<{
      cancelled?: boolean;
      block: BlockWithTransactions;
      receipts: Record<string, TransactionReceipt>;
    }>;
  }[] = [];

  // mark as true when we detect open
  const opened: Record<number, boolean> = {};

  // record the current process so that we can await its completion on close of listener
  let currentProcess: Promise<void>;

  // restructure ops into ops by chainId
  const validOps: Record<number, Sync[]> = Object.keys(syncProviders)
    .map((chainId) => {
      // filter for ops associated with this chainid
      const ops = syncOps.filter(
        (op) =>
          // is for chainId
          op.provider.network.chainId === +chainId
      );

      return {
        chainId: +chainId,
        validOps: ops,
      };
    })
    .reduce((all, op) => {
      return {
        ...all,
        [op.chainId]: op.validOps,
      };
    }, {});

  // pull all block and receipt details (cancel attempt after 30s)
  const getBlockAndReceipts = async (chainId: number, blockNumber: number) => {
    let resolved = false;
    // return the full block and receipts in 30s or cancel
    return Promise.race<{
      cancelled?: boolean;
      block: BlockWithTransactions;
      receipts: Record<string, TransactionReceipt>;
    }>([
      new Promise((resolve) => {
        setTimeout(async () => {
          if (!resolved) {
            resolve({
              cancelled: true,
              block: {} as BlockWithTransactions,
              receipts: {} as Record<string, TransactionReceipt>,
            });
          }
        }, 60000); // max of 60s per block - we'll adjust if needed
      }),
      // fetch all block and receipt details
      Promise.resolve().then(async () => {
        // get the full block details
        const block = await getBlockByNumber(
          syncProviders[+chainId],
          blockNumber
        );

        // get all receipts for the block - we need the receipts to access the logBlooms (if we're only doing onBlock/onTransaction we dont need to do this unless collectTxReceipts is true)
        const receipts = (
          await Promise.all(
            block.transactions.map(async function getReceipt(
              tx: TransactionResponse
            ) {
              // this promise.all is trapped until we resolve all tx receipts in the block
              try {
                // get the receipt
                const fullTx = await syncProviders[
                  +chainId
                ].getTransactionReceipt(tx.hash);
                // try again
                if (!fullTx.transactionHash) throw new Error("Missing hash");

                // return the tx
                return fullTx;
              } catch {
                // attempt to get receipt again on failure
                return getReceipt(tx);
              }
            })
          )
        ).reduce((all, receipt) => {
          // combine all receipts to create an indexed lookup obj
          return {
            ...all,
            [receipt.transactionHash]: receipt,
          };
        }, {});
        // mark as resolved
        resolved = true;

        // return the block and all receipts in the block
        return {
          block,
          receipts,
        };
      }),
    ]);
  };

  // record the block for the given chainId
  const recordBlock = (chainId: number, blockNumber: number) => {
    blocks.push({
      chainId: +chainId,
      number: blockNumber,
      // start fetching these parts now, we will wait for them when we begin processing the blocks...
      asyncParts: getBlockAndReceipts(+chainId, blockNumber),
    });
  };

  // attach a single listener for each provider
  const listeners = await Promise.all(
    Object.keys(syncProviders).map(async (chainId) => {
      // proceed with valid ops
      if (validOps[+chainId].length) {
        // attach a listener to the provider
        const listener = async (blockNumber: number) => {
          // push to the block stack to signal the block is retrievable
          if (state.listening) {
            // console.log("\nPushing block:", blockNumber, "on chainId:", chainId);
            recordBlock(+chainId, blockNumber);
          }
        };
        // attach this listener to onBlock to start collecting blocks
        syncProviders[+chainId].on("block", listener);

        // return unsubscribe method to stop the handler
        return async () => {
          // stop listening for new blocks first
          syncProviders[+chainId].off("block", listener);
          // remove the lock for the next iteration
          latestEntity[chainId].set("locked", false);
          // persist changes into the store
          latestEntity[chainId] = await latestEntity[chainId].save();
          // then wait for the current block to complete before ending
          await currentProcess;
        };
      }
      return false;
    })
  );

  // on the outside, we need to process all events emitted by the handlers to execute procedurally
  Promise.resolve()
    .then(async () => {
      // pull from the reqStack and process...
      while (state.listening) {
        // once the state moves to inSync - we can start taking blocks from the array to process
        if (blocks.length && state.inSync) {
          if (!opened[blocks[0].chainId]) {
            const { number: blockNumber, chainId } = blocks[0];
            if (blockNumber - latestBlocks[+chainId].number > 0) {
              // we need to close this gap
              while (blockNumber - latestBlocks[+chainId].number > 0) {
                // push prev blocks
                recordBlock(
                  chainId,
                  blockNumber - (blockNumber - latestBlocks[+chainId].number)
                );
              }
            }
            // mark as opened so we don't do this again
            opened[blocks[0].chainId] = true;
          }
          // record the current process so that it can be awaited in remove listener logic
          await Promise.resolve().then(async () => {
            // take the next block in the queue
            const [{ number: blockNumber, chainId, asyncParts }] =
              blocks.splice(0, 1);

            // restack this at the top so that we can try again
            const restack = () => {
              blocks.splice(0, 0, {
                number: blockNumber,
                chainId,
                // recreate the async parts to pull everything fresh
                asyncParts: getBlockAndReceipts(chainId, blockNumber),
              });
            };

            // check if this block needs to be processed (check its not included in the catchup-set)
            if (
              startBlocks[+chainId] &&
              blockNumber >= startBlocks[+chainId] &&
              blockNumber >= latestBlocks[+chainId].number
            ) {
              // wrap in a race here so that we never spend too long stuck on a block
              await Promise.race([
                new Promise<void>((resolve) => {
                  // add another 60s to process the block
                  setTimeout(async () => {
                    // set cancelled on the asyncParts obj we're passing through processBlock
                    (await asyncParts).cancelled = true;
                    // resolve to try again
                    resolve();
                  }, 60000); // max of 60s per block - we'll adjust if needed
                }),
                Promise.resolve().then(async () => {
                  try {
                    // attempt to process the block
                    await processBlock(
                      +chainId,
                      validOps,
                      blockNumber,
                      collectBlocks,
                      collectTxReceipts,
                      silent,
                      syncProviders,
                      eventIfaces,
                      latestEntity,
                      callbacks,
                      blocks.length,
                      asyncParts
                    );
                    // record the new number
                    if (!(await asyncParts).cancelled) {
                      latestBlocks[+chainId] = {
                        number: blockNumber,
                      } as unknown as Block;
                    } else {
                      // reattempt the timedout block
                      restack();
                    }
                  } catch {
                    // reattempt the failed block
                    restack();
                  }
                }),
              ]);
            }
          });
        } else {
          // wait 1 second for something to enter the queue
          await new Promise((resolve) => {
            setTimeout(resolve, 1000);
          });
        }
      }
    })
    .catch((e) => {
      // reject propagation and close
      if (handlers?.reject) {
        handlers.reject(e);
      }
    });

  // retun a handler to remove the listener
  return [
    // all valid listeners
    ...listeners.filter((v) => v),
  ];
};

// sync all events since last sync operation
export const sync = async ({
  start = false,
  stop = false,
  collectBlocks = false,
  collectTxReceipts = false,
  listen = false,
  cleanup = false,
  silent = false,
  onError = async (reset) => {
    // reset the locks by default
    await reset();
  },
}: {
  // globally include all blocks/txReceipts for all handlers
  collectBlocks?: boolean;
  collectTxReceipts?: boolean;
  // control how we listen, cleanup and log data
  listen?: boolean;
  cleanup?: boolean;
  silent?: boolean;
  // position which stage we should start and stop the sync
  start?: keyof typeof Stage | false;
  stop?: keyof typeof Stage | false;
  // process errors (should close connection)
  onError?: (close: () => Promise<void>) => Promise<void>;
} = {}) => {
  // record when we started this operation
  const startTime = performance.now();

  // collect each events abi iface
  const eventIfaces: Record<string, ethers.utils.Interface> = {};
  // collect the meta entry for the chain
  const latestEntity: LatestEntity = {};
  // collect callbacks from the syncs
  const callbacks: HandlerFns = {};
  // collect the block we start collecting from
  const startBlocks: Record<number, number> = {};
  // fetch the latest block once per chain
  const latestBlocks: Record<number, Block> = {};

  // keep track of connected listeners in listen mode
  const listeners: (() => void)[] = [];

  // set the control set for all listeners
  const controls = {
    inSync: false,
    listening: true,
  };

  // get all chainIds for defined networks
  const { chainIds, syncProviders } = await getNetworks();

  // attempt to pull the latest data
  try {
    // pull the sorted syncs
    const syncOps = sortSyncs();

    // check if we're globally including, or individually including the blocks
    const collectAnyBlocks = syncOps.reduce((collectBlock, opSync) => {
      return collectBlock || opSync.opts?.collectBlocks || false;
    }, collectBlocks);

    // check if we're globally including, or individually including the txReceipts
    const collectAnyTxReceipts = syncOps.reduce((collectTxReceipt, opSync) => {
      return collectTxReceipt || opSync.opts?.collectTxReceipts || false;
    }, collectTxReceipts);

    // event listener will see all blocks and transactions passing everything to appropriate handlers in block/tx/log order (grouped by type)
    if (listen) {
      // set up a mutable set of handlers to monitor for halting errors so that we can unlock db before exiting
      const errorHandler: {
        resolved?: boolean;
        reject?: (reason?: any) => void;
        resolve?: (value: void | PromiseLike<void>) => void;
      } = {};
      // create promise and apply handlers to obj
      const errorPromise = new Promise<void>((resolve, reject) => {
        errorHandler.reject = reject;
        errorHandler.resolve = resolve;
      }).then(() => {
        // mark as resolved
        errorHandler.resolved = true;
      });
      // do listener stuff...
      listeners.push(
        await Promise.resolve().then(async () => {
          // attach to listerners
          const attached = await attachListeners(
            syncOps,
            syncProviders,
            eventIfaces,
            latestEntity,
            callbacks,
            startBlocks,
            latestBlocks,
            controls,
            errorHandler,
            collectBlocks,
            collectTxReceipts,
            silent
          );

          // return a method to remove all handlers
          const close = async (): Promise<void> => {
            return new Promise((resolve) => {
              // place in the next call-frame
              setTimeout(async () => {
                // notify in stdout that we're closing the connection
                if (!silent) console.log("\nClosing listeners\n\n===\n");
                // close the loop
                controls.listening = false;
                // await removal of listeners and current block
                await Promise.all(
                  attached.map(
                    async (listener) => listener && (await listener())
                  )
                );
                // mark error as resolved - we won't use the promise again
                if (!errorHandler.resolved && errorHandler.resolve) {
                  errorHandler.resolve();
                }
                // give other watchers chance to see this first - but kill the process on error (this gives implementation oppotunity to restart)
                setTimeout(() => process.exit(1));
                // resolve the promise
                resolve();
              });
            });
          };

          // attach errors and pass to handler to close the connection
          errorPromise.catch(async () => onError(close));

          // return close to allow external closure
          return close;
        })
      );
    }

    // pull all syncs to this point
    const { events } = await getNewSyncEvents(
      eventIfaces,
      latestEntity,
      callbacks,
      startBlocks,
      latestBlocks,
      collectAnyBlocks,
      collectAnyTxReceipts,
      collectBlocks,
      collectTxReceipts,
      cleanup,
      silent,
      start
    ).catch((e) => {
      // throw in outer context
      throw e;
    });

    // get all new blocks and txReceipts associated with events
    await getNewSyncEventsBlocks(
      events,
      syncProviders,
      collectAnyBlocks,
      silent,
      start,
      stop
    );
    await getNewSyncEventsTxReceipts(
      events,
      syncProviders,
      collectAnyTxReceipts,
      silent,
      start,
      stop
    );

    // sort the events into processing order
    const sorted = await getNewSyncEventsSorted(
      events,
      collectBlocks,
      cleanup,
      silent,
      start,
      stop
    );

    // process the results
    await processEvents(
      sorted,
      chainIds,
      syncProviders,
      eventIfaces,
      latestEntity,
      callbacks,
      startBlocks,
      listen,
      collectBlocks,
      collectTxReceipts,
      cleanup,
      silent,
      start,
      stop
    );

    // record when we finished the sync operation
    const endTime = performance.now();

    // print time in console
    if (!silent)
      console.log(
        `\n===\n\nTotal execution time: ${(
          Number(endTime - startTime) / 1000
        ).toPrecision(4)}s\n\n`
      );

    // place in the microtask queue to open listeners after we return the sync summary and close fn
    if (listen) {
      setTimeout(() => {
        // open the listener queue for resolution
        controls.inSync = true;
        // print that we're opening the listeners
        if (!silent) console.log("\n===\n\nProcessing listeners...");
      });
    }

    // return a summary of the operation to the caller (and the close() fn if we called sync({...}) with listen: true)
    return {
      syncOps: syncOps.length,
      events: sorted.length,
      runTime: Number((endTime - startTime) / 1000).toPrecision(4),
      chainIds: Array.from(chainIds),
      eventsByChain: Array.from(chainIds).reduce(
        (all, chainId) => ({
          ...all,
          [chainId]: sorted.filter((vals) => vals.chainId === chainId).length,
        }),
        {}
      ),
      startBlocksByChain: Object.keys(startBlocks).reduce(
        (all, chainId) => ({
          ...all,
          [chainId]: +startBlocks[+chainId],
        }),
        {}
      ),
      latestBlocksByChain: Object.keys(latestEntity).reduce(
        (all, chainId) => ({
          ...all,
          [chainId]: +latestEntity[+chainId].latestBlock,
        }),
        {}
      ),
      ...((listen && {
        // if we're attached in listen mode, return a method to close the listeners (all reduced into one call)
        close: async () => listeners[0](),
      }) ||
        {}),
    } as SyncResponse;
  } catch (e: any) {
    // process error by releasing the locks
    if (e.toString() !== "DB is locked") {
      await onError(async () => {
        await releaseSyncPointerLocks(chainIds);
      });
    }

    // there was an error...
    return {
      error: e.toString(),
    } as SyncResponse;
  }
};
