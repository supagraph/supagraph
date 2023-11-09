// Import required ethers tools and types
import {
  Block,
  BlockWithTransactions,
  TransactionReceipt,
  TransactionResponse,
} from "@ethersproject/abstract-provider";
import { getAddress } from "ethers/lib/utils";

// Bloomfilter handling to check for topics/address on log/tx blooms
import {
  isTopicInBloom,
  isContractAddressInBloom,
} from "ethereum-bloom-filters";

// Import persistence tooling to store entity modifications during processing
import { getEngine, Store, Entity } from "@/sync/tooling/persistence/store";
import {
  updateSyncPointers,
  releaseSyncPointerLocks,
  updateSyncsOpsMeta,
} from "@/sync/tooling/persistence/meta";
import { doCleanup, readJSON } from "@/sync/tooling/persistence/disk";

// Import types used in the process
import {
  SyncStage,
  SyncEvent,
  Migration,
  Sync,
  SyncConfig,
} from "@/sync/types";

// Cast entry.data to ethers.Event
import { toEventData } from "@/utils/toEventData";

// Import provider tooling to gather current network/providers
import { getNetworks } from "@/sync/tooling/network/providers";

// Import promise queue handling to process internal promise queues
import { processGlobalPromiseQueue } from "@/sync/tooling/promises";

// Check if the blockTimestamp will trigger a scheduled event
import { checkSchedule } from "./schedule";
import { applyMigrations } from "./migrations";

// process an events callback
export const processCallback = async (
  event: SyncEvent,
  blockParts: {
    block: BlockWithTransactions & any;
    receipts: Record<string, TransactionReceipt>;
  },
  processed: SyncEvent[]
) => {
  // access globally shared engine
  const engine = await getEngine();

  // cast to eventData once for valid log style events
  const eventData = toEventData(event.data);

  // get an interface to parse the args
  const iface =
    engine.eventIfaces[
      `${
        (eventData.address &&
          `${event.chainId.toString()}-${getAddress(eventData.address)}`) ||
        event.chainId
      }-${event.type}`
    ];

  // make sure we've correctly discovered an events iface
  if (iface) {
    // extract the args by using the iface to parse event topics & data
    const { args } =
      typeof event.args === "object"
        ? event
        : iface.parseLog({
            topics: eventData.topics,
            data: eventData.data,
          });
    // transactions can be simplified if we don't need the details in our sync handlers
    const tx =
      event.tx ||
      (!engine.flags.collectTxReceipts && !event.collectTxReceipt
        ? ({
            contractAddress: eventData.address,
            transactionHash: eventData.transactionHash,
            transactionIndex: eventData.transactionIndex,
            blockHash: eventData.blockHash,
            blockNumber: +eventData.blockNumber,
          } as unknown as TransactionReceipt)
        : await readJSON<TransactionReceipt>(
            "transactions",
            `${event.chainId}-${eventData.transactionHash}`
          ));

    // most of the block can also be inferred from event if were not collecting blocks for this run
    const block =
      (blockParts.block &&
        +event.blockNumber === +blockParts.block.number &&
        blockParts.block) ||
      (!engine.flags.collectBlocks && !event.collectBlock
        ? ({
            hash: eventData.blockHash,
            number: +eventData.blockNumber,
            timestamp: event.timestamp || eventData.blockNumber,
          } as unknown as Block)
        : await readJSON<Block>(
            "blocks",
            `${event.chainId}-${+eventData.blockNumber}`
          ));

    // store the block into Store state with a backup of the block data incase
    setStoreState(event.chainId, block, {
      hash: eventData.blockHash,
      number: +eventData.blockNumber,
      timestamp: event.timestamp || eventData.blockNumber,
    });

    // index for the callback and opSync entry
    const cbIndex = `${
      (eventData.address &&
        `${event.chainId}-${getAddress(eventData.address)}`) ||
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
          logIndex: eventData.logIndex,
        }
      );
      // processed given event
      processed.push(event);
    }
  } else if (event.type === "migration") {
    // get block from tmp storage
    const block =
      (blockParts.block &&
        +event.blockNumber === +blockParts.block.number &&
        blockParts.block) ||
      (await readJSON<Block>(
        "blocks",
        `${event.chainId}-${+event.blockNumber}`
      ));
    // store the block into Store state
    setStoreState(event.chainId, block, undefined);
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
      (blockParts.block &&
        +event.blockNumber === +blockParts.block.number &&
        blockParts.block) ||
      (await readJSON<Block>(
        "blocks",
        `${event.chainId}-${+event.blockNumber}`
      ));
    // store the block into Store state
    setStoreState(event.chainId, block, undefined);
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
      (blockParts.block &&
        +event.blockNumber === +blockParts.block.number &&
        blockParts.block) ||
      (await readJSON<Block>(
        "blocks",
        `${event.chainId}-${+event.blockNumber}`
      ));
    // store the block into Store state
    setStoreState(event.chainId, block, undefined);
    // await the response of the handler before moving to the next operation in the sorted ops
    await engine.callbacks[`${event.chainId}-${event.type}`]?.(
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
};

// Process the sorted events via the sync handler callbacks
export const processEvents = async () => {
  // open a checkpoint on the db...
  const engine = await getEngine();
  // get all chainIds for defined networks
  const { chainIds } = await getNetworks();

  // copy prcessed events here
  const processed: SyncEvent[] = [];

  // check if we're finalising the process in this sync
  if (
    (!engine.flags.start ||
      SyncStage[engine.flags.start] <= SyncStage.process) &&
    (!engine.flags.stop || SyncStage[engine.flags.stop] >= SyncStage.process)
  ) {
    // store the updates until we've completed the call
    const chainUpdates: string[] = [];

    // create a checkpoint
    engine.stage.checkpoint();

    // log that we're starting
    if (!engine.flags.silent) process.stdout.write(`\n--\n\nEvents processed `);

    // iterate the sorted events and process the callbacks with the given args (sequentially - event loop to process all callbacks)
    while (engine.events.length) {
      // take the first item from the sorted array
      const opSorted = engine.events.shift();

      // create a checkpoint
      engine.stage.checkpoint();

      // we don't commit if we revert
      let abortedCallback = false;

      // wrap in try catch so that we don't leave any checkpoints open
      try {
        // process the callback for the given type
        await processCallback(
          opSorted,
          {
            block: false,
            receipts: {},
          },
          processed
        );
      } catch (e) {
        // log any errors from handlers - this should probably halt execution
        console.log(e);
        // mark as aborted
        abortedCallback = true;
        // clear anything added here...
        engine.stage.revert();
        // throw the error again
        throw e;
      } finally {
        // commit the checkpoint on the db...
        if (!abortedCallback) await engine.stage.commit();
      }
    }

    // await all promises that have been enqueued during execution of callbacks (this will be cleared afterwards ready for the next run)
    await processGlobalPromiseQueue(0);

    // print the number of processed events
    if (!engine.flags.silent) process.stdout.write(`(${processed.length}) `);

    // mark after we end
    if (!engine.flags.silent) process.stdout.write("✔\nEntities stored ");

    // make sure we perform all checkpoint updates in this call
    while (engine.stage.isCheckpoint) {
      // commit the checkpoint on the db...
      await engine.stage.commit();
    }

    // no longer a newDB after committing changes
    engine.newDb = false;

    // after commit all events are stored in db
    if (!engine.flags.silent) process.stdout.write("✔\nPointers updated ");

    // update the pointers to reflect the latest sync
    await updateSyncPointers(processed, chainUpdates);

    // finished after updating pointers
    if (!engine.flags.silent) process.stdout.write("✔\n");

    // do cleanup stuff...
    if (engine.flags.cleanup) {
      // rm the tmp dir between runs
      await doCleanup(processed);
    }

    // place some space before the updates
    if (!engine.flags.silent) console.log("\n--\n");

    // log each of the chainUpdate messages
    if (!engine.flags.silent)
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

// process a new block as it arrives
export const processListenerBlock = async (
  config: SyncConfig,
  number: number,
  chainId: number,
  queueLength: number,
  blockParts: {
    block: BlockWithTransactions & any;
    receipts: Record<string, TransactionReceipt>;
  },
  indexedOps: Record<number, Sync[]>,
  collectBlocks: boolean,
  collectTxReceipts: boolean,
  silent: boolean
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
        // onBlock and onTransaction passes only blockNumber as data
        data: {
          blockNumber: number;
        };
      }
  )[] = [];

  // open a new checkpoint whenever we open without one (this will preserve all changes in mem until we get to queueLength === 0)
  if (engine.stage.checkpoints.length === 0) {
    engine.stage.checkpoint();
  }

  // log that we're syncing the block (*note that writing to stdout directly will bypass chromes inspector logs but will avoid risk of console.log leaking)
  if (!silent)
    process.stdout.write(
      `\n--\n\nSyncing block ${number} (${queueLength} in queue) from ${syncProviders[chainId].network.name} (chainId: ${chainId})\n\nEvents processed `
    );

  // unpack the async parts  - we need the receipts to access the logBlooms (if we're only doing onBlock/onTransaction we won't use the content unless collectTxReceipts is true)
  const { block, receipts } = blockParts || {};

  // check that we have everything available for this block...
  if (block && receipts) {
    // store the block into Store state
    setStoreState(chainId, block, undefined);
    // check if any migration is relevant in this block
    if (engine.indexedMigrations[`${chainId}-${+block.number}`]) {
      // apply all migrations for this blockNumber
      await applyMigrations(
        // migrations will be collated into an array for this blockNumber
        engine.indexedMigrations[`${chainId}-${+block.number}`],
        // supply config for access to any newly added providers
        config,
        // pushing events to the current stack
        events as SyncEvent[]
      );
      // delete from migration index
      delete engine.indexedMigrations[`${chainId}-${+block.number}`];
    }

    // run through the ops and extract all events happening in this block to be sorted into logIndex order
    for (const op of indexedOps[chainId]) {
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
            blockNumber: +block.number,
            eventName: op.eventName,
            args: [],
            tx: {} as TransactionReceipt & TransactionResponse,
            // set really big to make sure onBlock is sorted to the end for this block
            txIndex: 999999999999999,
            logIndex: 999999999999999,
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
              blockNumber: +block.number,
              eventName: op.eventName,
              args: [],
              tx: JSON.parse(
                JSON.stringify({
                  ...tx,
                  ...(collectTxReceipts || op.opts?.collectTxReceipts
                    ? receipts[tx?.hash]
                    : ({} as unknown as TransactionReceipt)),
                })
              ),
              // onTx called after all other events for this tx
              txIndex: receipts[tx?.hash].transactionIndex,
              // set really big to make sure onTx is sorted to the end of the tx's events
              logIndex: 999999999999999,
            });
          }
        } else if (
          op.address &&
          op.eventName !== "withPromises" &&
          engine.callbacks[
            `${op.chainId}-${getAddress(op.address)}-${op.eventName}`
          ] &&
          engine.eventIfaces[
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
                    log.topics.indexOf(topic) !== -1 &&
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
                      // pass the full set of data to fill callback tx !op.opts.collectTxReceipts
                      data: {
                        address: op.address,
                        transactionHash: tx.hash,
                        transactionIndex: receipts[tx.hash].transactionIndex,
                        blockHash: block.hash,
                        blockNumber: block.number,
                        topics: log.topics,
                        data: log.data,
                      },
                      blockNumber: +block.number,
                      eventName: op.eventName,
                      args,
                      tx: JSON.parse(
                        JSON.stringify({
                          ...tx,
                          ...(collectTxReceipts || op.opts?.collectTxReceipts
                            ? receipts[tx.hash]
                            : ({} as unknown as TransactionReceipt)),
                        })
                      ),
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

    // sort the events (this order might yet change if we add new syncs in any events)
    const sorted = events.sort((a, b) => {
      // check the order we should sort these events into
      const txOrder = (a as SyncEvent).txIndex - (b as SyncEvent).txIndex;
      const logIndex = (a as SyncEvent).logIndex - (b as SyncEvent).logIndex;
      const migIndex =
        (a as SyncEvent).migrationKey - (b as SyncEvent).migrationKey;

      // sort the events by logIndex order (with onBlock and onTransaction coming first)
      return txOrder === 0 ? (logIndex === 0 ? migIndex : logIndex) : txOrder;
    });

    // record length of promise queue before running handlers
    const promiseQueueLength = engine.promiseQueue.length;

    // place sorted as events - this way we can extend the event state with addSyncs during execution
    engine.events = sorted as SyncEvent[];

    // temp record what has been processed
    const processed: SyncEvent[] = [];

    // for each of the sync events call the callbacks sequentially (event loop to process all callbacks)
    while (engine.events.length > 0) {
      // if we error we skip commit
      let aborted = false;
      // collect the indexes of any promises made during execution for easy revert
      let promiseQueueBeforeEachProcess: number;

      // take the first item from the sorted array
      const event = engine.events.shift();

      // create a checkpoint for these changes
      engine.stage.checkpoint();

      // wrap in try catch so that we don't leave any checkpoints open
      try {
        // position queue marker at new length
        promiseQueueBeforeEachProcess = engine.promiseQueue.length;
        // process the callback for the given type
        await processCallback(event, blockParts, processed);
      } catch (e) {
        // log any errors from handlers - this should probably halt execution
        console.log(e);
        // mark as aborted
        aborted = true;
        // should splice only new messages added in this callback from the message queue
        engine.promiseQueue.length = promiseQueueBeforeEachProcess;
        // clear anything added here...
        engine.stage.revert();
        // throw the error again (end exec - skip commit)
        throw e;
      } finally {
        // commit the checkpoint to parent
        if (!aborted) await engine.stage.commit();
      }
    }

    // print number of events in stdout
    if (!silent) process.stdout.write(`(${processed.length}) `);

    // only attempt to save changes when the queue is clear (or it has been 15s since we last stored changes)
    if (
      queueLength === 0 ||
      ((engine.lastUpdate || 0) + 15000 <= new Date().getTime() &&
        queueLength < 1000)
    ) {
      // attempt to flush the promiseQueue and resolve any withPromise callbacks
      await processGlobalPromiseQueue(promiseQueueLength);

      // mark after we end the processing
      if (!silent) {
        process.stdout.write(`✔\nEntities stored `);
      }

      // update with any new syncOps added in the sync
      if (engine.handlers && engine.syncOps.meta) {
        // record the new syncs to db (this will replace the current entry)
        engine.syncOps.meta = await updateSyncsOpsMeta(
          engine.syncOps.meta,
          engine.syncs
        );
      }

      // make sure we perform all checkpoint updates in this call
      while (engine.stage.isCheckpoint) {
        // commit the checkpoint on the db...
        await engine.stage.commit();
      }

      // after all events are stored in db
      if (!silent) process.stdout.write("✔\nPointers updated ");

      // update the lastUpdateTime (we have written everything to db - wait a max of 15s before next update)
      engine.lastUpdate = new Date().getTime();
    }

    // update the startBlock
    engine.startBlocks[chainId] = block.number;

    // record as new latest after all callbacks are complete
    engine.latestBlocks[+chainId] = {
      number: +block.number,
    } as unknown as Block;

    // update the pointers to reflect the latest sync
    await updateSyncPointers(
      // these events follow enough to pass as SyncEvents
      processed as unknown as SyncEvent[],
      []
    );

    // finished after updating pointers
    if (!silent) process.stdout.write(`✔\n`);

    // check if this block triggers anything in the schedule - run the schedule after processing the block to make sure we start from a complete state
    await checkSchedule(block.timestamp);
  } else {
    // emit error when missing block/receipts
    throw new Error(`Block parts missing ${block.number}`);
  }
};

// Set the chain and block into the stores state
export const setStoreState = (
  chainId: number,
  block: Block,
  altBlock: {
    hash: string;
    number: number;
    timestamp: number;
  }
) => {
  // set the chainId into the engine - this prepares the Store so that any new entities are constructed against these details
  Store.setChainId(chainId);
  // set the block for each operation into the runtime engine before we run the handler
  Store.setBlock(
    block && block.timestamp
      ? {
          hash: block.hash,
          parentHash: block.parentHash,
          number: block.number,
          timestamp: block.timestamp,
          nonce: block.nonce,
          difficulty: block.difficulty,
          _difficulty: block._difficulty,
          gasLimit: block.gasLimit,
          gasUsed: block.gasUsed,
          miner: block.miner,
          extraData: block.extraData,
          baseFeePerGas: block.baseFeePerGas,
          transactions: [
            ...(block.transactions || []).map(
              (blockTx: { hash: any } | string) => {
                // take a copy of each tx to drop assoc
                return typeof blockTx === "string"
                  ? blockTx
                  : JSON.parse(JSON.stringify(blockTx));
              }
            ),
          ],
        }
      : (altBlock as Block)
  );
};
