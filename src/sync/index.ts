// Import types for common constructs
import {
  Handlers,
  Migration,
  SyncEvent,
  SyncStage,
  SyncConfig,
  SyncResponse,
} from "@/sync/types";

// Create new entities against the engine via the Store
import {
  getEngine,
  checkLocks,
  promiseQueue,
  attachListeners,
  applyMigrations,
  getNewSyncEvents,
  getNewSyncEventsBlocks,
  getNewSyncEventsSorted,
  getNewSyncEventsTxReceipts,
  processEvents,
  releaseSyncPointerLocks,
  getNetworks,
  restoreSyncOps,
  updateSyncsOpsMeta,
} from "@/sync/tooling";

// Import sortSyncs method from config
import { sortSyncs } from "@/sync/config";

// Export multicall contract factory and call wrapper
export {
  MULTICALL_ABI,
  getMulticallContract,
  callMulticallContract,
} from "@/sync/tooling/network/multicall";

// Export root level persistence tooling
export { DB } from "@/sync/tooling/persistence/db";
export { Mongo } from "@/sync/tooling/persistence/mongo";

// Export level-db entity store to handlers via engine
export {
  Entity,
  Store,
  getEngine,
  setEngine,
} from "@/sync/tooling/persistence/store";

// Export promise handling
export {
  promiseQueue,
  enqueuePromise,
  processPromiseQueue,
} from "@/sync/tooling/promises";

// Export sync config tooling
export { addSync, delSync, setSyncs, sortSyncs } from "@/sync/config";

// Export all shared types
export type {
  Engine,
  Handlers,
  HandlerFn,
  HandlerFns,
  Migration,
  LatestEntity,
  Sync,
  SyncOp,
  SyncConfig,
  SyncResponse,
  SyncEvent,
  SyncStage,
} from "@/sync/types";

// sync all events since last sync operation and optionally start block handling daemon
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
  readOnly = false,
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
  readOnly?: boolean;
  // position which stage we should start and stop the sync
  start?: keyof typeof SyncStage | false;
  stop?: keyof typeof SyncStage | false;
  // process errors (should close connection)
  onError?: (e: unknown, close: () => Promise<void>) => Promise<void>;
} = {}) => {
  // record when we started this operation
  const startTime = new Date().getTime();

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
  // set readOnly option against the engine immediately
  engine.readOnly = config.readOnly ?? readOnly;
  // set concurrency according to config/fallback to 100
  engine.concurrency = config.concurrency ?? 100;
  // collect each events abi iface
  engine.eventIfaces = engine.eventIfaces ?? {};
  // default providers
  engine.providers = engine.providers ?? {};
  // pointer to the meta document containing current set of syncs
  engine.syncOps = await restoreSyncOps(config, handlers);
  // sort the syncs - this is what we're searching for in this run and future "listens"
  engine.syncs = sortSyncs(engine.syncOps.syncs);

  // get all chainIds for defined networks
  const { chainIds } = await getNetworks();

  // keep track of connected listeners in listen mode
  const listeners: (() => void)[] = [];

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

  // set the runtime flags into the engine
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
    // await the lock check
    await checkLocks(chainIds, startTime);

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

    // set the control set for all listeners (to open / close the handlers)
    const controls = {
      // this lets the handler invokation start taking blocks from the queue (we open this after initial sync completes)
      inSync: false,
      // this lets the provider onBlock handler know that it should be collecting blocks (this is open from start and closes on .close())
      listening: true,
    };

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
      // do listener opening stuff...
      listeners.push(
        await Promise.resolve().then(async () => {
          // attach controls to listerner and start collecting new blocks
          const attached = await attachListeners(
            controls,
            migrations,
            errorHandler
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
                  attached.map(async (detach) => detach && (await detach()))
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

    // apply any migrations that fit the event range
    await applyMigrations(migrations, config, events);

    // set the appendEvents against the engine to allow events to be appended during runtime (via addSync)
    engine.appendEvents = async (prcEvents: SyncEvent[], opSilent: boolean) => {
      // get all chainIds for current set of defined networks
      const { syncProviders: currentSyncProviders } = await getNetworks();

      // get all new blocks and txReceipts associated with events (blocks first followed by tx's to allow us to recycle txResponses from retrieved blocks)
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

      // sort the events into processing order (no changes to engine.events after this point except for processing)
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

    // update storage with any new syncOps added in the sync
    if (engine.handlers && engine.syncOps.meta) {
      // record the new syncs to db (this will replace the current entry)
      engine.syncOps.meta = await updateSyncsOpsMeta(
        engine.syncOps.meta,
        engine.syncs
      );
    }

    // record when we finished the sync operation
    const endTime = new Date().getTime();

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
        // print that we're opening the listeners
        if (!silent) console.log("\n===\n\nProcessing listeners...");
        // open the listener queue for resolution
        controls.inSync = true;
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
