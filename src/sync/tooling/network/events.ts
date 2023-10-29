import {
  Block,
  JsonRpcProvider,
  TransactionReceipt,
  WebSocketProvider,
} from "@ethersproject/providers";
import { ethers } from "ethers";

import { cwd } from "@/utils";
import { toEventData } from "@/utils/toEventData";

import { getEngine } from "@/sync/tooling/persistence/store";
import { processPromiseQueue } from "@/sync/tooling/promises";

import { createBlockRanges, getNewBlocks } from "@/sync/tooling/network/blocks";
import {
  exists,
  readLatestRunCapture,
  saveLatestRunCapture,
} from "@/sync/tooling/persistence/disk";
import { Sync, SyncStage, SyncEvent } from "@/sync/types";
import { getAddress } from "ethers/lib/utils";
import { getFnResultForProp } from "./fetch";
import { getNetworks } from "./providers";
import { getNewTransactions } from "./transactions";

// the maximum no. of blocks to be attempted in any one queryFilter call
export const RANGE_LIMIT = 100000;

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
        txIndex: +entry.transactionIndex,
        logIndex: +entry.logIndex,
      };
    })
  );
};

// pull new events from the contract
export const getNewEvents = async (
  address: string | undefined,
  events: ethers.Contract["abi"] | undefined,
  eventName: string,
  fromBlock: number,
  toBlock: number,
  provider: JsonRpcProvider | WebSocketProvider,
  collectTxReceipt: boolean,
  collectBlock: boolean,
  silent: boolean
) => {
  // get the global engine
  const engine = await getEngine();
  // collect a list of events then map the event type to each so that we can rebuild all events into a single array later
  const result = new Set<ethers.Event>();
  // get the chainId
  const { chainId } = provider.network;
  // only proceed if the contract is known...
  if (address && events) {
    // connect to contract and filter for events to build data-set
    const contract = new ethers.Contract(address, events, provider);
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
      await processPromiseQueue(stack, engine.concurrency, true);
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

// retrieve all events from all syncs from last-run to now
export const getNewSyncEvents = async (
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
  let events: SyncEvent[] = [];

  // we're starting the process here then print the discovery
  if (!silent && (!start || (start && SyncStage[start] < SyncStage.process))) {
    // detail everything which was loaded in this run
    console.log("\nStarting sync by collecting events...\n\n--\n");
  }

  // process all requests at once
  await Promise.all(
    syncOps.map(async (opSync) => {
      // hydrate the events into this var
      let newEvents: SyncEvent[] = [];

      // extract info from the sync operation
      const {
        chainId,
        address,
        eventName,
        provider,
        startBlock,
        endBlock,
        opts,
        events: eventsAbi,
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
      if (address && eventsAbi) {
        // record the event interface so we can reconstruct args to feed to callback
        engine.eventIfaces[`${addresses[address || chainId]}-${eventName}`] =
          engine.eventIfaces[`${addresses[address || chainId]}-${eventName}`] ||
          new ethers.utils.Interface(eventsAbi);
      }

      // check if we should be pulling events in this sync or using the tmp cache
      if (
        !start ||
        SyncStage[start] === SyncStage.events ||
        !exists(
          "events",
          `latestRun-${
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
            ? +engine.latestBlocks[chainId].number + 1
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
            eventsAbi,
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
        (!exists("events", "latestRun-allData.csv") &&
          exists(
            "events",
            `latestRun-${
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
export const getNewSyncEventsBlocks = async (
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
    const filtered = events.filter((evt) => !!evt.collectBlock);

    // extract blocks and call getBlock once for each discovered block - then index against blocks number
    await getFnResultForProp<Block>(
      events,
      filtered,
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
export const getNewSyncEventsTxReceipts = async (
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
export const getNewSyncEventsSorted = async (
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
        : (a.blockNumber || toEventData(a.data).blockNumber || 0) -
          (b.blockNumber || toEventData(b.data).blockNumber || 0);
      // sort blocks/txs to top
      const txSort =
        (a.txIndex || toEventData(a.data).transactionIndex || 0) -
        (b.txIndex || toEventData(b.data).transactionIndex || 0);
      const logSort =
        (a.logIndex || toEventData(a.data).logIndex || 0) -
        (b.logIndex || toEventData(b.data).logIndex || 0);

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
