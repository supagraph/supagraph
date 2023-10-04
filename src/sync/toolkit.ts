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
import {
  Sync,
  SyncOp,
  SyncEvent,
  SyncStage,
  SyncConfig,
  SyncResponse,
  Handlers,
  HandlerFn,
  Migration,
} from "./types";
import { TypedMapEntry } from "./typedMapEntry";

// how long to lock the db between sync attempts
export const LOCKED_FOR = 10;

// how many ranges to request at a time
export const CONCURRENCY = 100;

// the maximum no. of blocks to be attempted in any one queryFilter call
export const RANGE_LIMIT = 100000;

// sync operations will be appended in the route.ts file using addSync
const syncs: Sync[] = [];

// allow a promise to be delayed until later (but still complete as part of this sync)
const promiseQueue: (
  | Promise<unknown>
  | ((stack?: (() => Promise<any>)[]) => Promise<unknown>)
)[] = [];

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
> = {};

// function to return a provider from the alt list
export const getRandomProvider = (
  chainId: number,
  extraProviders: string[] = []
) => {
  const useProviders = [...extraProviders, ...altProviders[chainId]];
  const key = Math.floor(Math.random() * useProviders.length);
  // ensure record exists for rpcProviders on chainId
  rpcProviders[chainId] = rpcProviders[chainId] || {};
  rpcProviders[chainId][key] =
    rpcProviders[chainId][key] ||
    new providers.JsonRpcProvider(useProviders[key]);
  return rpcProviders[chainId][key];
};

// function to return a provider from the alt list
export const getProvider = async (chainId: number) => {
  const engine = await getEngine();

  return engine.providers?.[chainId]?.[0] || false;
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

// get a full transaction receipt from hash
const getTransactionReceipt = async (
  provider: JsonRpcProvider | WebSocketProvider,
  txHash: string | TransactionResponse
): Promise<BlockWithTransactions & any> => {
  try {
    // use response as given or fetch complete
    const tx = (txHash as TransactionResponse)?.hash
      ? txHash
      : await provider.send("eth_getTransactionByHash", [txHash]);
    // fetch receipt (from tx.hash which will always be present on response)
    const receipt = await provider.send("eth_getTransactionReceipt", [tx.hash]);
    // check the result holds data
    if (!receipt?.transactionHash || !tx.hash) {
      throw new Error("No tx response");
    }

    // this will combine both for all the details
    return {
      ...tx,
      ...receipt,
    };
  } catch {
    return getTransactionReceipt(provider, txHash);
  }
};

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
    const consec = reqStack.splice(0, concurrency || CONCURRENCY);
    // run through all promises until we come to a stop
    await Promise.all(consec.map(async (fn) => fn()));
  }

  // remove all from queue on cleanup
  if (cleanup) {
    queue.splice(0, queue.length);
  }
};

// allow external usage to set the sync operations
export async function addSync<T extends Record<string, unknown>>(
  chainIdOrParams:
    | number
    | {
        chainId: number;
        handlers: string;
        eventName: string;
        startBlock: number | "latest";
        endBlock?: number | "latest";
        onEvent?: HandlerFn;
        provider?: JsonRpcProvider | WebSocketProvider;
        opts?: {
          mode?: string;
          collectBlocks?: boolean;
          collectTxReceipts?: boolean;
        };
        address?: string;
        eventAbi?: string | ethers.Contract["abi"];
        against?: Sync[];
      },
  startBlock?: number | "latest",
  endBlock?: number | "latest",
  handlers?: string,
  eventName?: string,
  onEvent?: HandlerFn,
  provider?: JsonRpcProvider | WebSocketProvider,
  opts?: {
    mode?: string;
    collectBlocks?: boolean;
    collectTxReceipts?: boolean;
  },
  address?: string,
  eventAbi?: string | ethers.Contract["abi"],
  against?: Sync[]
): Promise<
  (
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
  ) => void
> {
  // fill params with everything as we go
  let params;
  // pull engine for latest syncs record
  const engine = await getEngine();
  // push to the provided collection or global syncs
  const collection = against || engine.syncs || syncs;

  // hydrate the params
  if (typeof chainIdOrParams === "number") {
    params = {
      chainId: chainIdOrParams!,
      address: address!,
      eventAbi: eventAbi!,
      eventName: eventName!,
      provider: provider!,
      startBlock: startBlock!,
      endBlock: endBlock!,
      opts: {
        mode: "config",
        collectBlocks: false,
        collectTxReceipts: false,
        // overide defaults with provided opts
        ...(opts || {}),
      },
      handlers: handlers!,
      onEvent: onEvent!,
    };
  } else {
    params = chainIdOrParams;
  }

  // no provider - but we have the chainId
  if (!params.provider) {
    // use the current provider
    params.provider = (await getProvider(
      params.chainId
    )) as providers.JsonRpcProvider;
  }

  // set to latest if block markers are absent
  if (!params.startBlock) {
    params.startBlock = "latest";
  }
  if (!params.endBlock) {
    params.endBlock = "latest";
  }

  // copy the eventAbis into place
  if (typeof params.eventAbis === "string") {
    params.eventAbis = engine.eventAbis[params.eventAbis];
  }

  // get the checksum address
  const cbAddress =
    (params.address &&
      `${params.chainId.toString()}-${getAddress(params.address)}`) ||
    params.chainId.toString();

  // with handlers defined, place handling
  if (engine.handlers) {
    // record the eventNames callback to execute on final sorted list
    engine.callbacks[`${cbAddress}-${params.eventName || "false"}`] =
      engine.handlers[params.handlers][params.eventName] || onEvent;
  } else {
    // record the eventNames callback to execute on final sorted list
    engine.callbacks[`${cbAddress}-${params.eventName || "false"}`] = onEvent;
  }

  // if already defined and we're syncing then delete to clear matching events before recapture...
  if (engine.syncing && engine.opSyncs[`${cbAddress}-${params.eventName}`]) {
    // delete the sync if it exists
    await delSync({
      chainId: params.chainId,
      eventName: params.eventName,
      address: params.address,
    });
  }

  // make sure we have a valid setup...
  if (
    engine.callbacks[`${cbAddress}-${params.eventName || "false"}`] &&
    params.provider
  ) {
    // store the new sync into indexed collection
    engine.opSyncs[`${cbAddress}-${params.eventName}`] = params;

    // push to the syncs (should we be pushing?)
    collection.push(params);
  } else {
    throw new Error(
      "Supagraph: You must provide a valid config inorder to set a handler"
    );
  }

  // default the mode to config
  params.opts.mode = params.opts.mode ?? "config";

  // if appendEvents is defined, then we're inside a sync op
  if (engine.syncing && typeof engine.appendEvents === "function") {
    // mark that this was added at runtime if not marked in some other way
    params.opts.mode =
      params.opts.mode !== "config" ? params.opts.mode : "runtime";
    // associate chainId with the store incase we're associating cross-chain
    Store.setChainId(params.chainId);

    // pull all syncs to this point
    const { events } = await getNewSyncEvents(
      [params],
      engine.flags?.collectBlocks || params.opts?.collectBlocks,
      engine.flags?.collectTxReceipts || params.opts?.collectTxReceipts,
      params.opts?.collectBlocks,
      params.opts?.collectTxReceipts,
      engine.flags?.cleanup,
      engine.flags?.start,
      true
    ).catch((e) => {
      // throw in outer context
      throw e;
    });

    // append and sort the pending events
    await engine.appendEvents(events, true);
  }

  // Return the event handler to constructing context
  return params.onEvent;
}

// remove a sync - this will remove the sync from the engines recorded syncs
export const delSync = async (
  chainIdOrParams:
    | number
    | {
        chainId: number;
        eventName: string;
        address?: string;
      },
  chainId?: number,
  eventName?: string,
  address?: string
) => {
  let params;
  // pull engine for latest syncs record
  const engine = await getEngine();

  // hydrate the params
  if (typeof chainIdOrParams === "number") {
    params = {
      chainId: chainId!,
      address: address!,
      eventName: eventName!,
    };
  } else {
    params = chainIdOrParams;
  }

  // get the checksum address
  const cbAddress =
    (params.address &&
      `${params.chainId.toString()}-${getAddress(params.address)}`) ||
    params.chainId.toString();

  // filter from collection
  engine.syncs = engine.syncs.filter((sync) => {
    return !(
      ((params.address &&
        getAddress(sync.address) === getAddress(params.address)) ||
        !params.address) &&
      sync.chainId === params.chainId &&
      sync.eventName === params.eventName
    );
  });

  // mark the sync as ended
  delete engine.opSyncs[`${cbAddress}-${params.eventName}`];
  delete engine.callbacks[`${cbAddress}-${params.eventName}`];
  delete engine.eventIfaces[`${cbAddress}-${params.eventName}`];

  // remove from events if present
  if (engine.syncing && engine.events.length) {
    // remove all events which match
    let i = engine.events.length;
    // eslint-disable-next-line no-plusplus
    while (i--) {
      if (
        ((params.address &&
          getAddress(engine.events[i].address) ===
            getAddress(params.address)) ||
          !params.address) &&
        engine.events[i].type === params.eventName &&
        engine.events[i].chainId === params.chainId
      ) {
        engine.events.splice(i, 1);
      }
    }
  }
};

// set the sync by config & handler object
export const setSyncs = async (
  config: SyncConfig,
  handlers: Handlers,
  against?: Sync[]
) => {
  // identify target for sync ops
  const collection = against || syncs;

  // load the engine
  const engine = await getEngine();

  // set against the engine
  engine.syncs = collection;
  engine.handlers = handlers;
  engine.eventAbis = config.events || {};
  engine.providers = {};

  // should we clear the against on a setSyncs?
  collection.splice(0, collection.length);

  // Register each of the syncs from the mapping
  await Promise.all(
    Object.keys(config.contracts).map(async (name) => {
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
        typeof syncOp.handlers === "string" ||
        typeof syncOp.handlers === "number"
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
      engine.providers[syncOp.chainId] = engine.providers[syncOp.chainId] || {};

      // stash the events for later use as an established mapping
      if (typeof syncOp.events === "string") {
        engine.eventAbis[syncOp.events] = events;
      }

      // set the rpc provider if undefined for this chainId
      engine.providers[syncOp.chainId][0] =
        engine.providers[syncOp.chainId][0] ||
        new providers.JsonRpcProvider(rpcUrl);

      // for each handler register a sync
      await Promise.all(
        Object.keys(mapping || []).map(async (eventName) => {
          await addSync({
            chainId: syncOp.chainId ?? (name as unknown as number),
            eventName,
            eventAbi: events,
            address: syncOp.address,
            startBlock: syncOp.startBlock,
            endBlock: syncOp.endBlock,
            provider: engine.providers[syncOp.chainId][0],
            opts: {
              mode: syncOp.mode || "config",
              collectBlocks:
                syncOp.collectBlocks || eventName === "onBlock" || false,
              collectTxReceipts: syncOp.collectTxReceipts || false,
            },
            handlers: syncOp.handlers ?? (name as unknown as string),
            onEvent: mapping[eventName],
            against: collection,
          });
        })
      );
    })
  );
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
const txsFromRange = async (
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
                  // replace this with a fetch to the rpc to get the full tx receipt (including gas details)
                  ...(await getTransactionReceipt(provider, tx)),
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
      await txsFromRange(
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
  await processPromiseQueue(stack, CONCURRENCY, true);

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
          console.log(
            `Made ${attempts} attempts to get: ${chainId}::block::${+from}`
          );
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
  await processPromiseQueue(stack, CONCURRENCY, true);

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
          middle,
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
  address: string,
  entries: ethers.Event[],
  provider: JsonRpcProvider | WebSocketProvider,
  toBlock: number,
  collectTxReceipt: boolean,
  collectBlock: boolean
) => {
  return Promise.all(
    entries.map(async (entry) => {
      return {
        address,
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
    // can only add events if they exist on the contract
    if (contract.filters[eventName]) {
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
      await processPromiseQueue(stack, CONCURRENCY, true);
    }
  }

  // dedupe on hash and logIndex
  const all = Array.from(
    new Map(
      Array.from(result).map((val) => [
        `${val.transactionHash}-${val.logIndex}`,
        val,
      ])
    )
  ).map((v) => v[1]);

  // wrap the events with the known type
  return wrapEventRes(
    eventName,
    address,
    all,
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

    // select provider to use for call
    const provider =
      attempts === 0
        ? syncProviders[dets.chainId]
        : getRandomProvider(dets.chainId, [
            syncProviders[dets.chainId].connection.url,
          ]);

    // wrap call to get receipt from custom handler
    const call =
      fn === "getTransactionReceipt"
        ? async (hash: unknown) =>
            getTransactionReceipt(provider, hash as string)
        : async (hash: unknown) => provider[fn](hash as string);

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

  // process all items in the stack
  await processPromiseQueue(stack, CONCURRENCY, true);

  // after resolution of all entries
  if (!silent && events.length)
    console.log(`All ${type} collected (${[...result].length})`);
};

// read a csv file of events
const readLatestRunCapture = async (
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
const saveLatestRunCapture = async (
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
const getNetworks = async (useSyncs?: Sync[]) => {
  // chainIds visited in the process
  const chainIds: Set<number> = new Set<number>();
  const syncProviders: Record<number, JsonRpcProvider | WebSocketProvider> = {};

  // retrieve the engine backing the Store
  const engine = await getEngine();

  // get all network descriptions now...
  await Promise.all(
    (useSyncs || engine.syncs).map(async (syncOp) => {
      if (syncOp.provider?.network?.chainId) {
        // record the chainId
        chainIds.add(syncOp.provider.network.chainId);
        // record the provider for the chain
        syncProviders[syncOp.provider.network.chainId] = syncOp.provider;
      } else {
        // get the network from the provider
        const { chainId } = await syncOp.provider.getNetwork();
        // record the chainId
        chainIds.add(chainId);
        // record the provider for the chain
        syncProviders[chainId] = syncOp.provider;
      }
    })
  );

  return {
    chainIds,
    syncProviders,
  };
};

// sort the syncs into an appropriate order
const sortSyncs = (against?: Sync[]) => {
  // sort the operations by chainId then eventName (asc then desc)
  return (against || syncs).sort((a, b) => {
    // process lower chainIds first
    return a.chainId - b.chainId;
  });
};

// retrieve all events from all syncs from last-run to now
const getNewSyncEvents = async (
  syncOps: Sync[],
  collectAnyBlocks: boolean,
  collectAnyTxReceipts: boolean,
  collectBlocks: boolean,
  collectTxReceipts: boolean,
  cleanup?: boolean,
  start?: keyof typeof SyncStage | false | undefined,
  silent?: boolean
) => {
  // collate the addresses we use in the processing
  const addresses: Record<string, string> = {};

  // retrieve the engine backing the Store
  const engine = await getEngine();

  // get all chainIds for defined networks
  const { syncProviders } = await getNetworks();

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
  if (!silent && (!start || (start && SyncStage[start] < SyncStage.process))) {
    // detail everything which was loaded in this run
    console.log("\n\nStarting sync by collecting events...\n\n--\n");
  }

  // process all requests at once
  await Promise.all(
    syncOps.map(async (opSync) => {
      // hydrate the events into this var
      let newEvents: any = [];

      // extract info from the sync operation
      const {
        chainId,
        address,
        eventAbi,
        eventName,
        provider,
        startBlock,
        endBlock,
        opts,
        onEvent,
      } = opSync;

      // record the checksum address
      addresses[address || chainId] =
        (address && `${chainId.toString()}-${getAddress(address)}`) ||
        chainId.toString();

      // record the eventNames callback to execute on final sorted list
      engine.callbacks[`${addresses[address || chainId]}-${eventName}`] =
        engine.callbacks[`${addresses[address || chainId]}-${eventName}`] ||
        onEvent;

      // when provided an address - we're mapping a contract...
      if (address && eventAbi) {
        // record the event interface so we can reconstruct args to feed to callback
        engine.eventIfaces[`${addresses[address || chainId]}-${eventName}`] =
          engine.eventIfaces[`${addresses[address || chainId]}-${eventName}`] ||
          new ethers.utils.Interface(eventAbi);
      }

      // check if we should be pulling events in this sync or using the tmp cache
      if (
        !start ||
        SyncStage[start] === SyncStage.events ||
        !fs.existsSync(
          `${cwd}events-latestRun-${
            addresses[address || chainId]
          }-${eventName}-${chainId}.csv`
        )
      ) {
        // start the run from the blocks
        if (start && SyncStage[start] > SyncStage.events) {
          engine.flags.start = start = "blocks";
        }
        // set the block frame (go back a block as a safety check (1 confirmation))
        const toBlock = !Number.isNaN(+endBlock)
          ? +endBlock
          : +engine.latestBlocks[chainId].number - 1;
        // starting from the last block we synced to or from latestBlock (if startBlock===latest), or from given startBlock if newDB
        const fromBlock =
          startBlock === "latest"
            ? +engine.latestBlocks[chainId].number - 1
            : (engine.latestEntity[chainId]?.latestBlock &&
                +engine.latestEntity[chainId].latestBlock + 1) ||
              startBlock;

        // mark engine as newDb
        if (!engine.latestEntity[chainId]?.latestBlock) {
          engine.newDb = true;
        }

        // record the startBlock
        engine.startBlocks[chainId] = fromBlock;

        // mark the operation in the log
        if (!silent && eventName !== "withPromises")
          console.log(
            `Start sync ${eventName}::${
              syncProviders[chainId].network.name
            }(${chainId})${address ? `::${address}` : ``} → ${JSON.stringify({
              fromBlock,
              toBlock,
            })}`
          );

        // greater than the prev fromBlock
        if (address && toBlock >= fromBlock && eventName !== "withPromises") {
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
          await saveLatestRunCapture(
            `${cwd}events-latestRun-${
              addresses[address || chainId]
            }-${eventName}-${chainId}.csv`,
            newEvents
          );
        }
      } else if (
        !start ||
        SyncStage[start] < SyncStage.process ||
        (!fs.existsSync(`${cwd}events-latestRun-allData.csv`) &&
          fs.existsSync(
            `${cwd}events-latestRun-${
              addresses[address || chainId]
            }-${eventName}-${chainId}.csv`
          ))
      ) {
        // assume that we're starting a fresh db
        engine.newDb = true;
        // start the run from the blocks
        if (start && SyncStage[start] >= SyncStage.process) {
          start = "blocks";
        }
        // read in events from disk
        newEvents = await readLatestRunCapture(
          `${cwd}events-latestRun-${
            addresses[address || chainId]
          }-${eventName}-${chainId}.csv`
        );
      }

      // append any newly discovered events to the collection
      if (!silent && eventName !== "withPromises")
        console.log(
          `  End sync ${eventName}::${
            syncProviders[chainId].network.name
          }(${chainId})${address ? `::${address}` : ``} → new events:`,
          newEvents.length
        );
      // push all new events to the event log
      events = events.concat(newEvents);
    })
  ).catch((e) => {
    // throw in outer context
    throw e;
  });

  // if we're starting the process here then print the discovery
  if (!silent && (!start || (start && SyncStage[start] < SyncStage.process))) {
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
  };
};

// retrieve all requested blocks to disk
const getNewSyncEventsBlocks = async (
  events: SyncEvent[],
  syncProviders: Record<number, JsonRpcProvider | WebSocketProvider>,
  collectAnyBlocks?: boolean,
  silent?: boolean,
  start?: keyof typeof SyncStage | false,
  stop?: keyof typeof SyncStage | false
) => {
  // check if we're gathering blocks in this sync
  if (
    collectAnyBlocks &&
    (!start || SyncStage[start] <= SyncStage.sort) &&
    (!stop || SyncStage[stop] >= SyncStage.blocks)
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
  start?: keyof typeof SyncStage | false,
  stop?: keyof typeof SyncStage | false
) => {
  // check if we're gathering transactions in this sync
  if (
    collectAnyTxReceipts &&
    (!start || SyncStage[start] <= SyncStage.sort) &&
    (!stop || SyncStage[stop] >= SyncStage.transactions)
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
  start?: keyof typeof SyncStage | false,
  stop?: keyof typeof SyncStage | false
) => {
  // open a checkpoint on the db...
  const engine = await getEngine();

  // sorted has same type as events
  let sorted: typeof events = [];

  // check if we're performing a sort in this sync
  if (
    (!start || SyncStage[start] <= SyncStage.sort) &&
    (!stop || SyncStage[stop] >= SyncStage.sort)
  ) {
    // sort the events by block timestamp then tx order
    sorted = events.sort((a, b) => {
      // sort on blockNumber first
      const blockSort = collectBlocks
        ? (a.timestamp || 0) - (b.timestamp || 0)
        : (a.blockNumber || a.data.blockNumber || 0) -
          (b.blockNumber || b.data.blockNumber || 0);
      // sort blocks/txs to top
      const txSort =
        (a.txIndex || a.data?.transactionIndex || 0) -
        (b.txIndex || b.data?.transactionIndex || 0);
      const logSort =
        (a.logIndex || a.data?.logIndex || 0) -
        (b.logIndex || b.data?.logIndex || 0);

      // bail outs to move tx/block handler calls to the top
      if (!blockSort && !txSort && typeof a.data === "string") {
        return -1;
      }
      if (!blockSort && !txSort && typeof b.data === "string") {
        return 1;
      }

      // if blockNumbers match sort on txIndex then logIndex
      return blockSort !== 0 ? blockSort : txSort !== 0 ? txSort : logSort;
    });

    // don't save if we're just going to delete it...
    if (!cleanup) {
      // save all events to disk
      await saveLatestRunCapture(`${cwd}events-latestRun-allData.csv`, sorted);
    }

    // sorted by timestamp
    if (!silent && sorted.length)
      console.log("\n--\n\nEvents sorted", sorted.length);
  }

  // check if we start on "process" in this sync and pull all the sorted content from the tmp store
  if (start && SyncStage[start] === SyncStage.process) {
    // log the start of the operation
    if (!silent)
      console.log(
        "\n\nStarting sync by restoring last-runs sorted events...\n\n--\n"
      );

    // assume that we're starting a fresh db
    engine.newDb = true;

    // restore the sorted collection
    sorted = await readLatestRunCapture(`${cwd}events-latestRun-allData.csv`);

    // detail everything which was loaded in this run
    if (!silent) console.log("Total no. events discovered:", sorted.length);
  }

  return sorted;
};

// update the pointers to reflect latest sync
const updateSyncPointers = async (
  sorted: SyncEvent[],
  listen: boolean,
  chainUpdates: string[]
) => {
  // get the engine
  const engine = await getEngine();

  // get all chainIds for defined networks
  const { chainIds, syncProviders } = await getNetworks();

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
        engine.startBlocks[chainId];
      // if timestamp isnt available default to using blockNumber in its place
      const latestTimestamp = chainsLatestBlock?.timestamp || latestBlockNumber;

      // grab from and to blocks
      const fromBlock = +engine.startBlocks[chainId];
      const toBlock = +(latestBlockNumber || engine.startBlocks[chainId]);

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
      engine.latestEntity[chainId].chainId = chainId;

      // don't record a move if we've havent found anything to warrant a move
      if (
        (fromBlock < toBlock || fromBlock === toBlock) &&
        chainsEvents.length
      ) {
        // take a copy of the latest entity to refresh chainId & block pointers
        engine.latestEntity[chainId].block = {
          // we add 1 here so that the next sync starts beyond the last block we processed (handlers are not idempotent - we must make sure we only process each event once)
          number: +latestBlockNumber,
          timestamp: +latestTimestamp,
        } as unknown as Block;

        // set the latest entry
        engine.latestEntity[chainId].set("latestBlock", +latestBlockNumber);
        engine.latestEntity[chainId].set("latestBlockTime", +latestTimestamp);
      } else {
        // clear the block to avoid altering _block_num and _block_ts on this run
        engine.latestEntity[chainId].block = undefined;
      }

      // when listening we can leave this locked until later
      if (!listen) {
        // remove the lock for the next iteration
        engine.latestEntity[chainId].set("locked", false);
      }

      // persist changes into the store
      engine.latestEntity[chainId] = await engine.latestEntity[chainId].save();
    })
  );
};

// reset locks on all chains
const releaseSyncPointerLocks = async (chainIds: Set<number>) => {
  // use global store
  const engine = await getEngine();
  // ensure latest entity is defined
  if (!engine.latestEntity) {
    // default to obj
    engine.latestEntity = {};
    // fill with latest entities
    await Promise.all(
      Array.from(chainIds).map(async (chainId) => {
        Store.clearBlock();
        Store.setChainId(chainId);
        engine.latestEntity![chainId] = await Store.get<{
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
      engine.latestEntity![chainId].chainId = chainId;
      // clear the block to avoid changing update times
      engine.latestEntity![chainId].block = {} as Block;
      // remove the lock for the next iteration
      engine.latestEntity![chainId].set("locked", false);
      // persist changes into the store
      engine.latestEntity![chainId] = await engine.latestEntity![chainId].save(
        false
      );
    })
  );
};

// process the sorted events via the sync handler callbacks
const doCleanup = async (
  events: SyncEvent[],
  silent?: boolean,
  start?: keyof typeof SyncStage | false,
  stop?: keyof typeof SyncStage | false
) => {
  // check if we're cleaning up in this sync
  if (
    (!start || SyncStage[start] <= SyncStage.process) &&
    (!stop || SyncStage[stop] >= SyncStage.process)
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
  chainIds: Set<number>,
  listen: boolean,
  cleanup?: boolean,
  silent?: boolean,
  start?: keyof typeof SyncStage | false,
  stop?: keyof typeof SyncStage | false
) => {
  // open a checkpoint on the db...
  const engine = await getEngine();

  // copy prcessed events here
  const processed: SyncEvent[] = [];

  // check if we're finalising the process in this sync
  if (
    engine.events.length &&
    (!start || SyncStage[start] <= SyncStage.process) &&
    (!stop || SyncStage[stop] >= SyncStage.process)
  ) {
    // store the updates until we've completed the call
    const chainUpdates: string[] = [];

    // create a checkpoint
    engine?.stage?.checkpoint();

    // log that we're starting
    if (!silent) process.stdout.write(`\n--\n\nEvents processed `);

    // iterate the sorted events and process the callbacks with the given args (sequentially)
    while (engine.events.length) {
      // take the first item from the sorted array
      const opSorted = engine.events.splice(0, 1)[0];

      // create a checkpoint
      engine?.stage?.checkpoint();

      // wrap in try catch so that we don't leave any checkpoints open
      try {
        // process the callback for the given type
        await processCallback(
          opSorted,
          {
            cancelled: false,
            block: false,
            receipts: {},
          },
          processed
        );
      } catch (e) {
        // log any errors from handlers - this should probably halt execution
        console.log(e);
      }

      // commit the checkpoint on the db...
      await engine?.stage?.commit();
    }

    // await all promises that have been enqueued during execution of callbacks (this will be cleared afterwards ready for the next run)
    await processPromiseQueue(promiseQueue);

    // iterate on the syncs and call withPromises
    for (const group of Object.keys(engine.handlers)) {
      for (const eventName of Object.keys(engine.handlers[group])) {
        if (eventName === "withPromises") {
          // check if we have any postProcessing callbacks to handle (each is handled the same way and is supplied the full promiseQueue)
          await engine.handlers[group][eventName](
            promiseQueue,
            {} as unknown as {
              tx: TransactionReceipt & TransactionResponse;
              block: Block;
              logIndex: number;
            }
          );
        }
      }
    }

    // print the number of processed events
    if (!silent) process.stdout.write(`(${processed.length}) `);

    // clear the promiseQueue for next iteration
    promiseQueue.splice(0, promiseQueue.length);

    // mark after we end
    if (!silent) process.stdout.write("✔\nEntities stored ");

    // commit the checkpoint on the db...
    await engine?.stage?.commit();

    // no longer a newDB after committing changes
    engine.newDb = false;

    // after commit all events are stored in db
    if (!silent) process.stdout.write("✔\nPointers updated ");

    // update the pointers to reflect the latest sync
    await updateSyncPointers(processed, listen, chainUpdates);

    // finished after updating pointers
    if (!silent) process.stdout.write("✔\n");

    // do cleanup stuff...
    if (cleanup) {
      // rm the tmp dir between runs
      await doCleanup(processed, silent, start, stop);
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
    await releaseSyncPointerLocks(chainIds);
  }

  // return number of processed events
  return processed.length;
};

// process an events callback
const processCallback = async (
  event: SyncEvent,
  parts: {
    cancelled?: boolean;
    block: BlockWithTransactions & any;
    receipts: Record<string, TransactionReceipt>;
  },
  processed: SyncEvent[]
) => {
  const engine = await getEngine();

  // ensure we havent cancelled the operation...
  if (!parts.cancelled) {
    // get an interface to parse the args
    const iface =
      engine.eventIfaces[
        `${
          (event.data.address &&
            `${event.chainId.toString()}-${getAddress(event.data.address)}`) ||
          event.chainId
        }-${event.type}`
      ];
    // make sure we've correctly discovered an iface
    if (iface) {
      // extract the args
      const { args } = iface.parseLog({
        topics: event.data.topics,
        data: event.data.data,
      });
      // transactions can be skipped if we don't need the details in our sync handlers
      const tx =
        event.tx ||
        (!engine.flags.collectTxReceipts && !event.collectTxReceipt
          ? ({
              contractAddress: event.data.address,
              transactionHash: event.data.transactionHash,
              transactionIndex: event.data.transactionIndex,
              blockHash: event.data.blockHash,
              blockNumber: event.data.blockNumber,
            } as unknown as TransactionReceipt)
          : await readJSON<TransactionReceipt>(
              "transactions",
              `${event.chainId}-${event.data.transactionHash}`
            ));

      // block can also be summised from event if were not collecting blocks for this run
      const block =
        (parts.block &&
          +event.blockNumber === +parts.block.number &&
          parts.block) ||
        (!engine.flags.collectBlocks && !event.collectBlock
          ? ({
              hash: event.data.blockHash,
              number: event.data.blockNumber,
              timestamp: event.timestamp || event.data.blockNumber,
            } as unknown as Block)
          : await readJSON<Block>(
              "blocks",
              `${event.chainId}-${+event.data.blockNumber}`
            ));

      // set the chainId into the engine - this prepares the Store so that any new entities are constructed against these details
      Store.setChainId(event.chainId);
      // set the block for each operation into the runtime engine before we run the handler
      Store.setBlock(
        block && block.timestamp
          ? block
          : ({
              timestamp: event.timestamp || event.data.blockNumber,
              number: event.data.blockNumber,
            } as Block)
      );

      // index for the callback and opSync entry
      const cbIndex = `${
        (event.data.address &&
          `${event.chainId}-${getAddress(event.data.address)}`) ||
        event.chainId
      }-${event.type}`;

      // skip the block if the sync has ended
      if (
        !(
          engine.opSyncs[cbIndex].endBlock !== "latest" &&
          (engine.opSyncs[cbIndex].endBlock === -1 ||
            +engine.opSyncs[cbIndex].endBlock <= block.number)
        )
      ) {
        // await the response of the handler before moving on to the next operation in the sorted ops
        await engine.callbacks[cbIndex]?.(
          // pass the parsed args construct
          args,
          // read tx and block from file (this avoids filling the memory with blocks/txs as we collect them - in prod we store into /tmp/)
          {
            tx: tx as TransactionReceipt & TransactionResponse,
            block,
            logIndex: event.data.logIndex,
          }
        );
        // processed given event
        processed.push(event);
      }
    } else if (event.type === "migration") {
      // get block from tmp storage
      const block =
        (parts.block &&
          +event.blockNumber === +parts.block.number &&
          parts.block) ||
        (await readJSON<Block>(
          "blocks",
          `${event.chainId}-${+event.blockNumber}`
        ));
      // set the chainId into the engine
      Store.setChainId(event.chainId);
      // set the block for each operation into the runtime engine before we run the handler
      Store.setBlock(block);
      // await the response of the handler before moving to the next operation in the sorted ops
      await (event.onEvent && typeof event.onEvent === "function"
        ? event.onEvent
        : (engine.callbacks[
            `${event.chainId}-${event.type}-${event.blockNumber}-${
              (event.data as unknown as { entityName?: string })?.entityName ||
              "false"
            }-${(event as unknown as { migrationKey: number }).migrationKey}`
          ] as unknown as Migration["handler"]))?.(
        block.number,
        event.chainId,
        (event.data as unknown as { entity?: Entity<{ id: string }> })?.entity
      );
      // processed given event
      processed.push(event);
    } else if (event.type === "onBlock") {
      // get block from tmp storage
      const block =
        (parts.block &&
          +event.blockNumber === +parts.block.number &&
          parts.block) ||
        (await readJSON<Block>(
          "blocks",
          `${event.chainId}-${+event.blockNumber}`
        ));
      // set the chainId into the engine
      Store.setChainId(event.chainId);
      // set the block for each operation into the runtime engine before we run the handler
      Store.setBlock(block);
      // await the response of the handler before moving to the next operation in the sorted ops
      await engine.callbacks[`${event.chainId}-${event.type}`]?.(
        // pass the parsed args construct
        [],
        // read tx and block from file (this avoids filling the memory with blocks/txs as we collect them - in prod we store into /tmp/)
        {
          tx: {} as unknown as TransactionReceipt & TransactionResponse,
          block,
          logIndex: -1,
        }
      );
      // processed given event
      processed.push(event);
    } else if (event.type === "onTransaction") {
      // get tx from tmp storage
      const tx =
        event.tx ||
        (await readJSON<TransactionResponse>(
          "transactions",
          `${event.chainId}-${event.data}`
        ));
      // get the tx and block from tmp storage
      const block =
        (parts.block &&
          +event.blockNumber === +parts.block.number &&
          parts.block) ||
        (await readJSON<Block>(
          "blocks",
          `${event.chainId}-${+event.blockNumber}`
        ));
      // set the chainId into the engine
      Store.setChainId(event.chainId);
      // set the block for each operation into the runtime engine before we run the handler
      Store.setBlock(block);
      // await the response of the handler before moving to the next operation in the sorted ops
      await engine.callbacks[
        `${
          (event.data.address &&
            `${event.chainId}-${getAddress(event.data.address)}`) ||
          event.chainId
        }-${event.type}`
      ]?.(
        // pass the parsed args construct
        [],
        // read tx and block from file (this avoids filling the memory with blocks/txs as we collect them - in prod we store into /tmp/)
        {
          tx: tx as TransactionReceipt & TransactionResponse,
          block,
          logIndex: -1,
        }
      );
      // processed given event
      processed.push(event);
    }
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
  migrations: Record<string, Migration[]>,
  migrationEntities: Record<string, Record<number, Promise<{ id: string }[]>>>,
  queueLength: number,
  asyncParts: Promise<{
    cancelled?: boolean;
    block: BlockWithTransactions & any;
    receipts: Record<string, TransactionReceipt>;
    syncOpsEntity: {
      current:
        | (Entity<{
            id: string;
            syncOps: SyncOp[];
          }> & {
            id: string;
            syncOps: SyncOp[];
          })
        | false;
    };
  }>
) => {
  // open a checkpoint on the db...
  const engine = await getEngine();

  // get all chainIds for defined networks
  const { syncProviders } = await getNetworks();

  // record events before making callbacks
  const events: (
    | Sync
    | SyncEvent
    | {
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
      }
  )[] = [];

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

  // await obj containing parts (keeping this then unpacking to check .cancelled by ref in async flow)
  const parts = await asyncParts;

  // unpack the async parts  - we need the receipts to access the logBlooms (if we're only doing onBlock/onTransaction we dont need to do this unless collectTxReceipts is true)
  const { block, receipts, cancelled, syncOpsEntity } = parts;

  // set the chainId into the engine
  Store.setChainId(chainId);
  // set the block for each operation into the engine before we run the handler
  Store.setBlock(block);

  // check if any migration is relevant in this block
  if (migrations[`${chainId}-${+block.number}`]) {
    // start collecting entities for migration now (this could be expensive - track by index to associate migrationEntities)
    for (const migrationKey of Object.keys(
      migrations[`${chainId}-${+block.number}`]
    )) {
      const migration = migrations[`${chainId}-${+block.number}`][migrationKey];
      // check for entity
      if (migration.entity) {
        // pull all entities
        const entities = await migrationEntities[`${chainId}-${+block.number}`][
          migrationKey
        ];
        // push a new event for each entity in the migration
        entities.forEach((entity) => {
          // record this migration event with this entity
          events.push({
            id: `${chainId}`,
            chainId,
            type: "migration",
            provider: undefined as unknown as JsonRpcProvider,
            name: `migration-${migration.chainId}-${migration.blockNumber}`,
            startBlock: migration.blockNumber,
            onEvent: migration.handler as unknown as Migration["handler"],
            // @ts-ignore
            data: {
              blockNumber: migration.blockNumber,
              entity: new Entity<typeof entity>(migration.entity, entity.id, [
                ...Object.keys(entity).map((key) => {
                  return new TypedMapEntry(
                    key as keyof typeof entity,
                    entity[key]
                  );
                }),
              ]),
              // set entityName for callback recall
              entityName: migration.entity,
            } as unknown as Event,
            blockNumber: +block.number,
            eventName: "migration",
            args: [],
            tx: {} as TransactionReceipt & TransactionResponse,
            // onBlock first
            txIndex: -2,
            logIndex: -2,
          });
        });
        // delete the migrations entities after constructing the events
        delete migrationEntities[`${chainId}-${+block.number}`][migrationKey];
      } else {
        // push a version without entities if entities is false
        events.push({
          id: `${chainId}`,
          type: "migration",
          chainId,
          provider: await getProvider(chainId),
          name: `migration-${migration.chainId}-${migration.blockNumber}`,
          startBlock: migration.blockNumber,
          onEvent: migration.handler as unknown as Migration["handler"],
          // @ts-ignore
          data: {
            blockNumber: migration.blockNumber,
          } as unknown as Event,
          blockNumber: +block.number,
          eventName: "migration",
          args: [],
          tx: {} as TransactionReceipt & TransactionResponse,
          // onBlock first
          txIndex: -2,
          logIndex: -2,
        });
      }
    }
    // clean up migrations after adding events
    delete migrations[`${chainId}-${+block.number}`];
  }

  // check that we havent cancelled this operation
  if (!cancelled) {
    // run through the ops and extract all events happening in this block to be sorted into logIndex order
    for (const op of validOps[chainId]) {
      // make sure endblock is respected
      if (op.endBlock === "latest" || op.endBlock <= block.blockNumber) {
        // check for block/tx/event by eventName (and check for matching callback)
        if (
          op.eventName === "onBlock" &&
          engine.callbacks[`${chainId}-${op.eventName}`]
        ) {
          // record the event
          events.push({
            ...op,
            type: "onBlock",
            id: `${chainId}`,
            chainId,
            timestamp: collectBlocks && block.timestamp,
            data: {
              blockNumber: block.number,
            } as Event & { blockNumber: number },
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
          engine.callbacks[`${chainId}-${op.eventName}`]
        ) {
          // create a new event for every transaction in the block
          for (const tx of block.transactions as TransactionResponse[]) {
            // record the event
            events.push({
              ...op,
              type: "onTransaction",
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
                  : ({} as unknown as TransactionReceipt)),
              },
              // onTx first out of tx & events
              txIndex: receipts[tx.hash].transactionIndex,
              logIndex: -1,
            });
          }
        } else if (
          op.address &&
          op.eventName !== "withPromises" &&
          engine.callbacks[
            `${op.chainId}-${getAddress(op.address)}-${op.eventName}`
          ]
        ) {
          // check for a matching topic in the transactions logBloom
          const iface =
            engine.eventIfaces[
              `${op.chainId}-${getAddress(op.address)}-${op.eventName}`
            ];
          const topic = iface.getEventTopic(op.eventName);
          const hasEvent =
            isTopicInBloom(block.logsBloom, topic) &&
            isContractAddressInBloom(block.logsBloom, getAddress(op.address));

          // check for logs on the block
          if (hasEvent) {
            // now we need to find the transaction that created this logEvent
            for (const tx of block.transactions as TransactionResponse[]) {
              // check if the tx has the event...
              const txHasEvent =
                isTopicInBloom(receipts[tx.hash].logsBloom, topic) &&
                isContractAddressInBloom(
                  receipts[tx.hash].logsBloom,
                  getAddress(op.address)
                );
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
                      id: `${op.chainId}-${getAddress(op.address)}`,
                      type: op.eventName,
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
                          : ({} as unknown as TransactionReceipt)),
                      },
                      // order as defined
                      txIndex: receipts[tx.hash].transactionIndex,
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
    }

    // make sure we haven't been cancelled before we get here
    if (!parts.cancelled) {
      // print number of events in stdout
      if (!silent) process.stdout.write(`(${events.length}) `);

      // sort the events (this order might yet change if we add new syncs in any events)
      const sorted = events.sort((a, b) => {
        // check the transaction order of the block
        const txOrder = (a as SyncEvent).txIndex - (b as SyncEvent).txIndex;

        // sort the events by logIndex order (with onBlock and onTransaction coming first)
        return txOrder === 0
          ? (a as SyncEvent).logIndex - (b as SyncEvent).logIndex
          : txOrder;
      });

      // checkpoint only these writes
      engine.stage?.checkpoint();
      // place sorted as events - this way we can extend the event state with addSyncs during execution
      engine.events = sorted as SyncEvent[];

      // temp record what has been processed
      const processed: SyncEvent[] = [];

      // for each of the sync events call the callbacks sequentially
      while (engine.events.length > 0) {
        // take one at a time from the events
        const event = engine.events.splice(0, 1)[0];
        // process each callback type
        await processCallback(event, parts, processed);
      }

      // commit or revert
      if (!parts.cancelled) {
        // move thes changes to the parent checkpoint
        await engine?.stage?.commit();
      } else {
        // clear the promiseQueue
        promiseQueue.splice(0, promiseQueue.length);
        // revert this checkpoint we're not saving it
        engine?.stage?.revert();
      }

      // if we havent been cancelled up to now we can commit this
      if (!parts.cancelled) {
        // only attempt to save changes when the queue is clear (or it has been 15s since we last stored changes)
        if (
          queueLength === 0 ||
          ((engine?.lastUpdate || 0) + 15000 <= new Date().getTime() &&
            queueLength < 1000)
        ) {
          // await all promises that have been enqueued during execution of callbacks (this will be cleared afterwards ready for the next run)
          await processPromiseQueue(promiseQueue);
          // iterate on the syncs and call withPromises
          for (const group of Object.keys(engine.handlers)) {
            for (const eventName of Object.keys(engine.handlers[group])) {
              if (eventName === "withPromises") {
                // check if we have any postProcessing callbacks to handle
                await engine.handlers[group][eventName](
                  promiseQueue,
                  {} as unknown as {
                    tx: TransactionReceipt & TransactionResponse;
                    block: Block;
                    logIndex: number;
                  }
                );
              }
            }
          }
          // clear the promiseQueue for next iteration
          promiseQueue.splice(0, promiseQueue.length);
          // mark after we end the processing
          if (!silent) {
            process.stdout.write(`✔\nEntities stored `);
          }
          // update with any new syncOps added in the sync
          if (engine.handlers && syncOpsEntity.current) {
            // record the new syncs to db (this will replace the current entry)
            syncOpsEntity.current = await recordSyncsOpsMeta(
              syncOpsEntity.current,
              engine.syncs
            );
          }

          // commit the checkpoint on the db...
          await engine?.stage?.commit();

          // after all events are stored in db
          if (!silent) process.stdout.write("✔\nPointers updated ");

          // update the lastUpdateTime (we have written everything to db - wait a max of 15s before next update)
          engine.lastUpdate = new Date().getTime();
        }

        // update the startBlock
        engine.startBlocks[chainId] = block.number;

        // update the pointers to reflect the latest sync
        await updateSyncPointers(
          // these events follow enough to pass as SyncEvents
          processed as unknown as SyncEvent[],
          true,
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
  state: {
    inSync: boolean;
    listening?: boolean;
  } = {
    inSync: true,
    listening: true,
  },
  errorHandlers: {
    reject?: (reason?: any) => void;
    resolve?: (value: void | PromiseLike<void>) => void;
  } = undefined,
  migrations: Migration[] = [],
  syncOpsEntity:
    | (Entity<{
        id: string;
        syncOps: SyncOp[];
      }> & {
        id: string;
        syncOps: SyncOp[];
      })
    | false = false,
  collectBlocks = false,
  collectTxReceipts = false,
  silent = false
) => {
  // get all chainIds for defined networks
  const { syncProviders } = await getNetworks();

  // array of blocks that need to be processed...
  const blocks: {
    chainId: number;
    number: number;
    asyncParts: Promise<{
      cancelled?: boolean;
      block: BlockWithTransactions;
      receipts: Record<string, TransactionReceipt>;
      syncOpsEntity: { current: typeof syncOpsEntity };
    }>;
    asyncEntities: Record<string, Record<number, Promise<{ id: string }[]>>>;
  }[] = [];

  // index all migrations
  const indexedMigrations = migrations.reduce((index, migration) => {
    // start arr to keep migrations for chainId at blockNumber
    index[`${migration.chainId}-${migration.blockNumber}`] =
      index[`${migration.chainId}-${migration.blockNumber}`] || [];

    // store the migration
    index[`${migration.chainId}-${migration.blockNumber}`].push(migration);

    // return the index
    return index;
  }, {} as Record<string, Migration[]>);

  // mark as true when we detect open
  const opened: Record<number, boolean> = {};

  // retrieve engine
  const engine = await getEngine();

  // record the current process so that we can await its completion on close of listener
  let currentProcess: Promise<void>;

  // place inside a wrapper to update by mutations
  const syncOpsEntityWrapper = {
    current: syncOpsEntity,
  };

  // restructure ops into ops by chainId
  const validOps: () => Promise<Record<number, Sync[]>> = async () => {
    // get all chainIds for defined networks
    const networks = await getNetworks();
    return Object.keys(networks.syncProviders)
      .map((chainId) => {
        // filter for ops associated with this chainid
        const ops = engine.syncs.filter(
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
  };

  // pull all block and receipt details (cancel attempt after 30s)
  const getBlockAndReceipts = async (chainId: number, blockNumber: number) => {
    let resolved = false;
    // return the full block and receipts in 30s or cancel
    return Promise.race<{
      cancelled?: boolean;
      block: BlockWithTransactions;
      receipts: Record<string, TransactionReceipt>;
      syncOpsEntity: { current: typeof syncOpsEntity };
    }>([
      new Promise((resolve) => {
        setTimeout(async () => {
          if (!resolved) {
            resolve({
              cancelled: true,
              block: {} as BlockWithTransactions,
              receipts: {} as Record<string, TransactionReceipt>,
              syncOpsEntity: syncOpsEntityWrapper,
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
                const fullTx = await getTransactionReceipt(
                  syncProviders[+chainId],
                  tx
                );
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
          syncOpsEntity: syncOpsEntityWrapper,
        };
      }),
    ]);
  };

  // record the block for the given chainId
  const recordBlock = (chainId: number, blockNumber: number) => {
    // store all in sparse array (as obj)
    const asyncMigrationEntities: Record<
      string,
      Record<number, Promise<{ id: string }[]>>
    > = {};

    // check if any migration is relevant in this block
    if (indexedMigrations[`${chainId}-${blockNumber}`]) {
      // start collecting entities for migration now (this could be expensive)
      indexedMigrations[`${chainId}-${blockNumber}`].forEach(
        (migration, key) => {
          asyncMigrationEntities[`${chainId}-${blockNumber}`] =
            asyncMigrationEntities[`${chainId}-${blockNumber}`] || {};
          asyncMigrationEntities[`${chainId}-${blockNumber}`][key] =
            new Promise((resolve) => {
              resolve(
                migration.entity &&
                  (engine.db.get(migration.entity) as Promise<{ id: string }[]>)
              );
            });
        }
      );
    }

    // record the new block
    blocks.push({
      chainId: +chainId,
      number: blockNumber,
      // start fetching these parts now, we will wait for them when we begin processing the blocks...
      asyncParts: getBlockAndReceipts(+chainId, blockNumber),
      // record migration entities on the block
      asyncEntities: asyncMigrationEntities,
    });
  };

  // attach a single listener for each provider
  const listeners = await Promise.all(
    Object.keys(syncProviders).map(async (chainId) => {
      // proceed with valid ops
      if ((await validOps())[+chainId].length) {
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
          engine.latestEntity[chainId].set("locked", false);
          // persist changes into the store
          engine.latestEntity[chainId] = await engine.latestEntity[
            chainId
          ].save();
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
            if (blockNumber - engine.latestBlocks[+chainId].number > 0) {
              // we need to close this gap
              while (blockNumber - engine.latestBlocks[+chainId].number > 0) {
                // push prev blocks
                recordBlock(
                  chainId,
                  blockNumber -
                    (blockNumber - engine.latestBlocks[+chainId].number)
                );
              }
            }
            // mark as opened so we don't do this again
            opened[blocks[0].chainId] = true;
          }
          // record the current process so that it can be awaited in remove listener logic
          await Promise.resolve().then(async () => {
            // take the next block in the queue
            const [
              { number: blockNumber, chainId, asyncParts, asyncEntities },
            ] = blocks.splice(0, 1);

            // restack this at the top so that we can try again
            const restack = () => {
              blocks.splice(0, 0, {
                number: blockNumber,
                chainId,
                // recreate the async parts to pull everything fresh
                asyncParts: getBlockAndReceipts(chainId, blockNumber),
                // this shouldn't fail (but it could be empty)
                asyncEntities,
              });
            };

            // check if this block needs to be processed (check its not included in the catchup-set)
            if (
              engine.startBlocks[+chainId] &&
              blockNumber >= engine.startBlocks[+chainId] &&
              blockNumber >= engine.latestBlocks[+chainId].number
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
                      await validOps(),
                      blockNumber,
                      collectBlocks,
                      collectTxReceipts,
                      silent,
                      indexedMigrations,
                      asyncEntities,
                      blocks.length,
                      asyncParts
                    );
                    // record the new number
                    if (!(await asyncParts).cancelled) {
                      engine.latestBlocks[+chainId] = {
                        number: blockNumber,
                      } as unknown as Block;
                    } else {
                      // reattempt the timedout block
                      restack();
                    }
                  } catch (e) {
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
      if (errorHandlers?.reject) {
        errorHandlers.reject(e);
      }
    });

  // retun a handler to remove the listener
  return [
    // all valid listeners
    ...listeners.filter((v) => v),
  ];
};

// retrieve the latest sync data from mongo
const getLatestSyncMeta = async () => {
  const engine = await getEngine();

  // get all chainIds for defined networks
  const { syncProviders } = await getNetworks();

  engine.latestEntity = {};
  engine.latestBlocks = {};

  // record locks by chainId
  const locked: Record<number, boolean> = {};
  const blocked: Record<number, number> = {};

  // return all entities associated with chainIds
  await Promise.all(
    Object.keys(syncProviders).map(async (chainId) => {
      // set the chainId into the engine
      Store.setChainId(+chainId);
      // clear the current block state so we don't disrupt sync timestamps on the entity
      Store.clearBlock();
      // fetch the meta entity from the store (we'll update this once we've committed all changes)
      engine.latestEntity[chainId] = await Store.get<{
        id: string;
        latestBlock: number;
        latestBlockTime: number;
        locked: boolean;
        lockedAt: number;
        _block_num: number;
        _block_ts: number;
      }>("__meta__", `${chainId}`); // if this is null but we have rows in the collection, we could select the most recent and check the lastBlock

      // make sure this lines up with what we expect
      engine.latestEntity[+chainId].chainId = +chainId;
      engine.latestEntity[chainId].block = undefined;

      // hydrate the locked bool
      locked[chainId] = engine.latestEntity[chainId]?.locked;
      // check when the lock was instated
      blocked[chainId] =
        engine.latestEntity[chainId]?.lockedAt ||
        engine.latestEntity[chainId]?.latestBlock ||
        0;
      // toBlock is always "latest" from when we collect the events
      engine.latestBlocks[chainId] = await syncProviders[chainId].getBlock(
        "latest"
      );

      // bool on comp
      return true;
    })
  );

  // return all details of latest load
  return {
    locked,
    blocked,
  };
};

// retrieve the syncOps from storage
const getSyncsOpsMeta = async () => {
  // from the __meta__ table we need to collect opSync operations - these can be saved as a normalised doc in the collection
  // fetch the meta entity from the store (we'll update this once we've committed all changes)
  const latestOpSyncs = await Store.get<{
    id: string;
    syncOps: SyncOp[];
  }>("__meta__", `syncOps`);

  // we will only ever record this set of values once
  return latestOpSyncs;
};

// record the full set of syncOps as provided - these needs to be called after initial sync and based on any changes we make during runtime
const recordSyncsOpsMeta = async (
  syncOpsEntity: Entity<{
    id: string;
    syncOps: SyncOp[];
  }> & {
    id: string;
    syncOps: SyncOp[];
  },
  against?: Sync[]
) => {
  // pull engine for syncs
  const engine = await getEngine();
  // set the changes against the syncOp
  syncOpsEntity.set(
    "syncOps",
    // map from the provided set
    (against || engine.syncs || syncs).map((sync) => {
      return {
        name: sync.name,
        chainId: sync.chainId,
        handlers: sync.handlers,
        eventName: sync.eventName,
        events: sync.eventAbi,
        address: sync.address,
        mode: sync.opts?.mode || "config",
        collectBlocks: sync.opts?.collectBlocks || false,
        collectTxReceipts: sync.opts?.collectTxReceipts || false,
        startBlock: sync.startBlock,
        endBlock: sync.endBlock,
      };
    })
  );
  // we don't need to record meta on the syncOps entry
  syncOpsEntity.chainId = null;
  syncOpsEntity.block = null;

  // save changes
  return syncOpsEntity.save();
};

// sync all events since last sync operation
export const sync = async ({
  config = undefined,
  handlers = undefined,
  migrations = undefined,
  start = false,
  stop = false,
  collectBlocks = false,
  collectTxReceipts = false,
  listen = false,
  cleanup = false,
  silent = false,
  onError = async (_, reset) => {
    // reset the locks by default
    await reset();
  },
}: {
  // with the provided config...
  config?: SyncConfig;
  handlers?: Handlers;
  // allow migrations to be injected at blockHeights
  migrations?: Migration[];
  // globally include all blocks/txReceipts for all handlers
  collectBlocks?: boolean;
  collectTxReceipts?: boolean;
  // control how we listen, cleanup and log data
  listen?: boolean;
  cleanup?: boolean;
  silent?: boolean;
  // position which stage we should start and stop the sync
  start?: keyof typeof SyncStage | false;
  stop?: keyof typeof SyncStage | false;
  // process errors (should close connection)
  onError?: (e: unknown, close: () => Promise<void>) => Promise<void>;
} = {}) => {
  // record when we started this operation
  const startTime = performance.now();

  // load the engine
  const engine = await getEngine();

  // lock the engine for syncing
  engine.syncing = true;
  // no error yet...
  engine.error = false;
  // collect the block we start collecting from
  engine.startBlocks = {};
  // assign the mutable promiseQueue to engine directly
  engine.promiseQueue = promiseQueue;

  // collect each events abi iface
  engine.eventIfaces = engine.eventIfaces ?? {};
  // default providers
  engine.providers = engine.providers ?? {};

  // pointer to the meta document containing current set of syncs
  let syncOpsMeta: Entity<{
    id: string;
    syncOps: SyncOp[];
  }> & {
    id: string;
    syncOps: SyncOp[];
  };

  // keep track of connected listeners in listen mode
  const listeners: (() => void)[] = [];

  // set the control set for all listeners
  const controls = {
    inSync: false,
    listening: true,
  };

  // set the initial sync object
  if (
    Object.keys(handlers || {}).length ||
    Object.keys(engine.handlers || {}).length
  ) {
    // if config is provided, we can source our syncs from db
    syncOpsMeta = await getSyncsOpsMeta();

    // set the syncs into the engines syncs config
    if (syncOpsMeta.syncOps && syncOpsMeta.syncOps.length) {
      // place the handlers into the engine
      engine.handlers =
        !handlers && Object.keys(engine.handlers).length
          ? engine.handlers
          : handlers || {};

      // set config defined syncs first then merge runtime added syncs preserving config edits
      if (config && engine.handlers) {
        // start a new syncs object
        engine.syncs = [];
        // set the config against engine.syncs
        await setSyncs(config, engine.handlers, engine.syncs);
      }

      // assign db stored syncs in to the engine so long as they hold a "runtime" mode opt
      await Promise.all(
        syncOpsMeta.syncOps.map(async (syncOp) => {
          // get the checksum address
          const cbAddress =
            (syncOp.address &&
              `${syncOp.chainId.toString()}-${getAddress(syncOp.address)}`) ||
            syncOp.chainId.toString();
          // so long as the handler is defined and the item was added in "runtime" mode...
          if (
            handlers[syncOp.handlers][syncOp.eventName] &&
            // ie; only restore syncs which havent been hardcoded into the config
            !engine.opSyncs[`${cbAddress}-${syncOp.eventName}`] &&
            // if we've restarted back to startBlock then we can replace this sync at runtime again with an addSync in the same place (if we need to remove all of associations we can change the handler refs)
            syncOp.mode === "runtime"
          ) {
            // default the provider container
            engine.providers[syncOp.chainId] =
              engine.providers[syncOp.chainId] || [];
            // set the rpc provider if undefined for this chainId
            engine.providers[syncOp.chainId][0] =
              engine.providers[syncOp.chainId][0] ||
              new providers.JsonRpcProvider(
                config.providers[syncOp.chainId].rpcUrl
              );
            // construct the provider for this chainId
            const provider = (await getProvider(
              syncOp.chainId
            )) as providers.JsonRpcProvider;
            // construct the opSync object
            const opSync = {
              provider,
              eventName: syncOp.eventName,
              name: syncOp.name as string,
              chainId: syncOp.chainId,
              startBlock: syncOp.startBlock,
              endBlock: syncOp.endBlock,
              address: syncOp.address,
              eventAbi: syncOp.events,
              handlers: syncOp.handlers,
              opts: {
                mode: syncOp.mode,
                collectBlocks: syncOp.collectBlocks,
                collectTxReceipts: syncOp.collectTxReceipts,
              },
              onEvent: handlers[syncOp.handlers][syncOp.eventName],
            };
            // assign into engine
            engine.opSyncs[`${cbAddress}-${syncOp.eventName}`] = opSync;
            // and record into syncs
            engine.syncs.push(opSync);
          }
        })
      );
    } else if (config && handlers) {
      // start a new syncs object
      engine.syncs = [];
      // set the config against engine.syncs
      await setSyncs(config, handlers, engine.syncs);
    }

    // record the new syncs to db (this will replace the current entry)
    syncOpsMeta = await recordSyncsOpsMeta(syncOpsMeta, engine.syncs);
  } else {
    // assign global syncs to engines syncs (we have no config in the sync call)
    engine.syncs = syncs;
  }

  // sort the syncs - this is what we're searching for in this run and future "listens"
  engine.syncs = sortSyncs(engine.syncs);

  // get all chainIds for defined networks
  const { chainIds } = await getNetworks();

  // allow options to be set by config instead of by top level if supplied
  listen = config ? config.listen ?? listen : listen;
  silent = config ? config.silent ?? silent : silent;
  cleanup = config ? config.cleanup ?? cleanup : cleanup;
  collectBlocks = config
    ? config.collectBlocks ?? collectBlocks
    : collectBlocks;
  collectTxReceipts = config
    ? config.collectTxReceipts ?? collectTxReceipts
    : collectTxReceipts;

  // set the run flags into the engine
  engine.flags = {
    listen,
    cleanup,
    silent,
    start,
    stop,
    collectBlocks,
    collectTxReceipts,
  };

  // attempt to pull the latest data
  try {
    // this collects the meta regarding when we last ran the sync
    const { locked, blocked } = await getLatestSyncMeta();

    // if any are locked we can escape...
    const anyLocked = [...chainIds].reduce(
      (isLocked, chainId) => isLocked || locked[chainId],
      false
    );

    // check if any of our connections are locked/blocked (only proceeds when all chains exceed LOCKED_FOR)
    if (anyLocked) {
      // close connection or release if blocked time has ellapsed
      for (const chainId of chainIds) {
        // when the db is locked check if it should be released before ending all operations
        if (
          !engine.newDb &&
          locked[chainId] &&
          // this offset should be chain dependent
          +blocked[chainId] + LOCKED_FOR >
            (+engine.latestBlocks[chainId].number || 0)
        ) {
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
      }
    }

    // check if we're globally including, or individually including the blocks
    const collectAnyBlocks = engine.syncs.reduce((collectBlock, opSync) => {
      return collectBlock || opSync.opts?.collectBlocks || false;
    }, collectBlocks);

    // check if we're globally including, or individually including the txReceipts
    const collectAnyTxReceipts = engine.syncs.reduce(
      (collectTxReceipt, opSync) => {
        return collectTxReceipt || opSync.opts?.collectTxReceipts || false;
      },
      collectTxReceipts
    );

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
            controls,
            errorHandler,
            migrations,
            syncOpsMeta,
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
                // close the lock
                engine.syncing = false;
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
          errorPromise.catch(async (e) => {
            // assign error for just-in-case
            engine.error = e;
            // chain error through user handler
            return onError(e, close);
          });

          // return close to allow external closure
          return close;
        })
      );
    }

    // pull all syncs to this point (supply engine.syncs directly to gather full list of events)
    const { events } = await getNewSyncEvents(
      engine.syncs,
      collectAnyBlocks,
      collectAnyTxReceipts,
      collectBlocks,
      collectTxReceipts,
      cleanup,
      start,
      silent
    ).catch((e) => {
      // throw in outer context
      throw e;
    });

    // check for migrations - we should insert an event for every item in the entity collection to run the callback
    if (migrations) {
      let migrationCount = 0;
      // check if we have any migrations to run in this range
      for (const migrationKey of Object.keys(migrations)) {
        // locate the migration
        const migration = migrations[migrationKey];
        // set the migrationKey into the migration
        migration.migrationKey = migrationKey;
        // set the latest blockNumber in place and move to the end
        if (migration.blockNumber === "latest") {
          migration.blockNumber = engine.latestBlocks[migration.chainId].number;
        }
        // check this will be triggered here...
        if (
          migration.blockNumber >= engine.startBlocks[migration.chainId] &&
          migration.blockNumber <= engine.latestBlocks[migration.chainId].number
        ) {
          // assign the callback (masquarade this as valid for the context)
          engine.callbacks[
            `${migration.chainId}-migration-${migration.blockNumber}-${
              migration.entity || "false"
            }-${migration.migrationKey}`
          ] = migration.handler as unknown as HandlerFn;
          // if we're basing this on entity, then push an event for each entity
          if (migration.entity) {
            // this migration needs to be ran within this sync - start gathering the query results now
            const allEntries =
              migration.entity &&
              ((await engine.db.get(migration.entity)) as ({
                id: string;
              } & Record<string, unknown>)[]);

            // push a migration entry
            allEntries.forEach((entity) => {
              events.push({
                // insert as migration type
                type: "migration",
                // @ts-ignore
                data: {
                  entity: new Entity<typeof entity>(
                    migration.entity,
                    entity.id,
                    [
                      ...Object.keys(entity).map((key) => {
                        return new TypedMapEntry(key, entity[key]);
                      }),
                    ]
                  ),
                  // set the name for callback recall
                  entityName: migration.entity,
                  blockNumber: migration.blockNumber as number,
                } as unknown as Event,
                chainId: migration.chainId,
                blockNumber: migration.blockNumber as number,
                migrationKey: migration.migrationKey,
                collectBlock: true,
                collectTxReceipt: false,
              });
              // incr by one
              migrationCount += 1;
            });
          } else {
            events.push({
              // insert as migration type
              type: "migration",
              // @ts-ignore
              data: {
                blockNumber: migration.blockNumber as number,
              } as unknown as Event,
              chainId: migration.chainId,
              blockNumber: migration.blockNumber as number,
              migrationKey: migration.migrationKey,
              collectBlock: true,
              collectTxReceipt: false,
            });
            // incr by one
            migrationCount += 1;
          }
        }
      }
      // if we're starting the process here then print the discovery
      if (
        !silent &&
        (!start || (start && SyncStage[start] < SyncStage.process)) &&
        migrationCount > 0
      ) {
        // detail everything which was loaded in this run
        console.log(
          "Total no. migrations in range:",
          migrationCount,
          "\n\n--\n"
        );
      }
    }

    // set the appendEvents against the engine to allow events to be appended during runtime (via addSync)
    engine.appendEvents = async (prcEvents: SyncEvent[], opSilent: boolean) => {
      // get all chainIds for defined networks
      const { syncProviders: currentSyncProviders } = await getNetworks();

      // get all new blocks and txReceipts associated with events
      await getNewSyncEventsBlocks(
        prcEvents,
        currentSyncProviders,
        collectAnyBlocks || engine.flags.collectBlocks,
        opSilent,
        engine.flags.start,
        engine.flags.stop
      );
      await getNewSyncEventsTxReceipts(
        prcEvents,
        currentSyncProviders,
        collectAnyTxReceipts || engine.flags.collectTxReceipts,
        opSilent,
        engine.flags.start,
        engine.flags.stop
      );

      // add new events to the current set (by mutation)
      engine.events.push(...prcEvents);

      // sort the events into processing order (no changes to engine.events after this point)
      engine.events = await getNewSyncEventsSorted(
        engine.events,
        engine.flags.collectBlocks,
        engine.flags.cleanup,
        opSilent,
        engine.flags.start,
        engine.flags.stop
      );
    };

    // sort the pending events
    await engine.appendEvents(events, engine.flags.silent ?? false);

    // process the results
    const newEventsTotal = await processEvents(
      chainIds,
      listen,
      cleanup,
      silent,
      start,
      stop
    );
    // update with any new syncOps added in the sync
    if (engine.handlers && syncOpsMeta) {
      // record the new syncs to db (this will replace the current entry)
      syncOpsMeta = await recordSyncsOpsMeta(syncOpsMeta, engine.syncs);
    }

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
      // attach close mech to engine
      engine.close = async () => listeners[0]();
      // open after a tick to start after returning catchup summary response
      setTimeout(() => {
        // open the listener queue for resolution
        controls.inSync = true;
        // print that we're opening the listeners
        if (!silent) console.log("\n===\n\nProcessing listeners...");
      });
    } else {
      // set syncing to false
      engine.syncing = false;
    }

    // return a summary of the operation to the caller (and the close() fn if we called sync({...}) with listen: true)
    return {
      syncOps: engine.syncs.length,
      events: newEventsTotal,
      runTime: Number((endTime - startTime) / 1000).toPrecision(4),
      chainIds: Array.from(chainIds),
      eventsByChain: Array.from(chainIds).reduce(
        (all, chainId) => ({
          ...all,
          [chainId]: events.filter((vals) => vals.chainId === chainId).length,
        }),
        {}
      ),
      startBlocksByChain: Object.keys(engine.startBlocks).reduce(
        (all, chainId) => ({
          ...all,
          [chainId]: +engine.startBlocks[+chainId],
        }),
        {}
      ),
      latestBlocksByChain: Object.keys(engine.latestEntity).reduce(
        (all, chainId) => ({
          ...all,
          [chainId]: +engine.latestEntity[+chainId].latestBlock,
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
    // assign error to engine
    engine.error = e;

    // process error by releasing the locks
    if (e.toString() !== "DB is locked") {
      await onError(e, async () => {
        await releaseSyncPointerLocks(chainIds);
      });
    }

    // there was an error...
    return {
      error: e.toString(),
    } as SyncResponse;
  }
};
