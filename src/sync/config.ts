import {
  TransactionReceipt,
  TransactionResponse,
  Block,
} from "@ethersproject/abstract-provider";
import { JsonRpcProvider, WebSocketProvider } from "@ethersproject/providers";
import { ethers, providers } from "ethers";
import { getAddress } from "ethers/lib/utils";

import { getEngine, Store } from "@/sync/tooling/persistence/store";
import { getNewSyncEvents } from "@/sync/tooling/network/events";
import { getProvider } from "@/sync/tooling/network/providers";

import { Sync, HandlerFn, SyncConfig, Handlers, CronSchedule } from "./types";

// sync operations will be appended in the route.ts file using addSync
export const syncs: Sync[] = [];

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
        events?: string | ethers.Contract["abi"];
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
  events?: string | ethers.Contract["abi"],
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
      events: events!,
      handlers: handlers!,
      onEvent: onEvent!,
    };
  } else {
    // hydrate the params and excluse against
    params = {
      chainId: chainIdOrParams.chainId!,
      address: chainIdOrParams.address!,
      eventName: chainIdOrParams.eventName!,
      provider: chainIdOrParams.provider!,
      startBlock: chainIdOrParams.startBlock!,
      endBlock: chainIdOrParams.endBlock!,
      opts: {
        mode: "config",
        collectBlocks: false,
        collectTxReceipts: false,
        // overide defaults with provided opts
        ...(chainIdOrParams.opts || {}),
      },
      events: chainIdOrParams.events!,
      handlers: chainIdOrParams.handlers!,
      onEvent: chainIdOrParams.onEvent!,
    };
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
  if (typeof params.events === "string") {
    params.events = engine.eventAbis[params.events];
  }

  // get the checksum address
  const cbAddress =
    (params.address &&
      `${params.chainId.toString()}-${getAddress(params.address)}`) ||
    params.chainId.toString();

  // when provided an address - we're mapping a contract...
  if (params.address && params.events) {
    // record the event interface so we can reconstruct args to feed to callback
    engine.eventIfaces[`${cbAddress}-${params.eventName}`] =
      engine.eventIfaces[`${cbAddress}-${params.eventName}`] ||
      new ethers.utils.Interface(params.events);
  }

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
    const { events: syncEvents } = await getNewSyncEvents(
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
    await engine.appendEvents(syncEvents, true);
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
        engine.events.pop();
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
  collection.length = 0;

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
        new providers.JsonRpcProvider({
          fetchOptions: {
            referrer: "https://supagraph.xyz",
          },
          url: rpcUrl,
        });

      // for each handler register a sync
      await Promise.all(
        Object.keys(mapping || []).map(async (eventName) => {
          await addSync({
            chainId: syncOp.chainId ?? (name as unknown as number),
            events,
            eventName,
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

// sort the syncs into an appropriate order
export const sortSyncs = (against?: Sync[]) => {
  // sort the operations by chainId then eventName (asc then desc)
  return (against || syncs).sort((a, b) => {
    // process lower chainIds first
    return a.chainId - b.chainId;
  });
};

// set the cron schedule to fire timebound events as we process blocks...
export const setSchedule = async (
  schedule: CronSchedule[],
  against?: CronSchedule[]
) => {
  // get engine to place the schedule
  const engine = await getEngine();

  // default cronSchedule and conbine with provided values
  engine.cronSchedule =
    against || engine.cronSchedule
      ? [...(against || engine.cronSchedule), ...(schedule || [])]
      : [...(schedule || [])];

  // return the schedule
  return engine.cronSchedule;
};
