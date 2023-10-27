import { providers } from "ethers";
import { getAddress } from "ethers/lib/utils";
import { Block } from "@ethersproject/abstract-provider";

import { getNetworks, getProvider } from "@/sync/tooling/network/providers";

import { syncs, setSyncs } from "@/sync/config";
import { Sync, SyncOp, SyncEvent, SyncConfig, Handlers } from "@/sync/types";

import { Entity, getEngine, Store } from "@/sync/tooling/persistence/store";

import { toEventData } from "@/utils/toEventData";

// how long to lock the db between sync attempts
export const LOCKED_FOR = 10;

// retrieve the latest sync data from mongo
export const getLatestSyncMeta = async () => {
  const engine = await getEngine();

  // get all chainIds for defined networks
  const { syncProviders } = await getNetworks();

  // clear engine storage
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

// check if the table is locked for any of the chainIds
export const checkLocks = async (chainIds: Set<number>, startTime: number) => {
  // get the engine
  const engine = await getEngine();

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
        if (!engine.flags.silent)
          console.log("Sync error - chainId ", chainId, " is locked");

        // record when we finished the sync operation
        const endTime = new Date().getTime();

        // print time in console
        if (!engine.flags.silent)
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
};

// update the pointers to reflect latest sync
export const updateSyncPointers = async (
  sorted: SyncEvent[],
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
        toEventData(chainsLatestBlock?.data).blockNumber ||
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
      if (!engine.flags.listen) {
        // remove the lock for the next iteration
        engine.latestEntity[chainId].set("locked", false);
      }

      // persist changes into the store
      engine.latestEntity[chainId] = await engine.latestEntity[chainId].save();
    })
  );
};

// reset locks on all chains
export const releaseSyncPointerLocks = async (chainIds: Set<number>) => {
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

// retrieve the syncOps from storage
export const getSyncsOpsMeta = async () => {
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
export const updateSyncsOpsMeta = async (
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
    (against || engine.syncs).map((sync) => {
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

// restore the sync from config & db
export const restoreSyncOps = async (
  config: SyncConfig,
  handlers: Handlers
) => {
  // retrieve the engine
  const engine = await getEngine();

  // get the entity wrapper but dont fetch anything yet...
  let syncOpsMeta: Entity<{
    id: string;
    syncOps: SyncOp[];
  }> & {
    id: string;
    syncOps: SyncOp[];
  } = await Store.get("__meta__", `syncOps`, true);

  // new array for the syncs
  let syncOps = [];

  // set the initial sync object
  if (
    Object.keys(handlers || {}).length ||
    Object.keys(engine.handlers || {}).length
  ) {
    // fetch from the __meta__ table if we have handlers to restore against
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
        // set the config against engine.syncs
        await setSyncs(config, engine.handlers, syncOps);
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
            syncOps.push(opSync);
          }
        })
      );
    } else if (config && handlers) {
      // set the config against engine.syncs
      await setSyncs(config, handlers, syncOps);
    }

    // record the new syncs to db (this will replace the current entry)
    syncOpsMeta = await updateSyncsOpsMeta(syncOpsMeta, syncOps);
  } else {
    // assign global syncs to engines syncs (we have no config in the sync call)
    syncOps = syncs;
  }

  // will always return an entity
  return {
    syncs: syncOps,
    meta: syncOpsMeta,
  };
};
