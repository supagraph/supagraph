// Import required ethers tools and types
import {
  Block,
  BlockWithTransactions,
  TransactionReceipt,
  TransactionResponse,
} from "@ethersproject/abstract-provider";
import { getAddress, Result } from "ethers/lib/utils";
import { JsonRpcProvider } from "@ethersproject/providers";

// Bloomfilter handling to check for topics/address on log/tx blooms
import {
  isTopicInBloom,
  isContractAddressInBloom,
} from "ethereum-bloom-filters";

// Import persistence tooling to store entity modifications during processing
import { TypedMapEntry } from "@/sync/tooling/persistence/typedMapEntry";
import { getEngine, Store, Entity } from "@/sync/tooling/persistence/store";
import {
  updateSyncPointers,
  releaseSyncPointerLocks,
  updateSyncsOpsMeta,
} from "@/sync/tooling/persistence/meta";
import { doCleanup, readJSON } from "@/sync/tooling/persistence/disk";

// Import types used in the process
import { SyncStage, SyncEvent, Migration, Sync } from "@/sync/types";

// Cast entry.data to ethers.Event
import { toEventData } from "@/utils/toEventData";

// Import provider tooling to gather current network/providers
import { getNetworks, getProvider } from "@/sync/tooling/network/providers";

// Import promise queue handling to process internal promise queues
import { processPromiseQueue } from "@/sync/tooling/promises";

// Check if the blockTimestamp will trigger a scheduled event
import { checkSchedule } from "./schedule";

// process an events callback
export const processCallback = async (
  event: SyncEvent,
  blockParts: {
    cancelled?: boolean;
    block: BlockWithTransactions & any;
    receipts: Record<string, TransactionReceipt>;
  },
  processed: SyncEvent[]
) => {
  // access globally shared engine
  const engine = await getEngine();

  // ensure we havent cancelled the operation...
  if (!blockParts.cancelled) {
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
              blockNumber: eventData.blockNumber,
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
              number: eventData.blockNumber,
              timestamp: event.timestamp || eventData.blockNumber,
            } as unknown as Block)
          : await readJSON<Block>(
              "blocks",
              `${event.chainId}-${+eventData.blockNumber}`
            ));

      // set the chainId into the engine - this prepares the Store so that any new entities are constructed against these details
      Store.setChainId(event.chainId);
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
                ...block.transactions.map((blockTx: { hash: any } | string) => {
                  // take a copy of each tx to drop assoc
                  return typeof blockTx === "string"
                    ? blockTx
                    : JSON.parse(JSON.stringify(blockTx));
                }),
              ],
            }
          : ({
              timestamp: event.timestamp || eventData.blockNumber,
              number: eventData.blockNumber,
            } as Block)
      );

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
      // set the chainId into the engine
      Store.setChainId(event.chainId);
      // set the block for each operation into the runtime engine before we run the handler
      Store.setBlock({
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
          ...block.transactions.map((tx: { hash: any }) => {
            // take a copy of each tx to drop assoc
            return tx?.hash ? JSON.parse(JSON.stringify(tx)) : tx;
          }),
        ],
      });
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
      // set the chainId into the engine
      Store.setChainId(event.chainId);
      // set the block for each operation into the runtime engine before we run the handler
      Store.setBlock({
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
          ...block.transactions.map((tx: { hash: any }) => {
            // take a copy of each tx to drop assoc
            return tx?.hash ? JSON.parse(JSON.stringify(tx)) : tx;
          }),
        ],
      });
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
      // set the chainId into the engine
      Store.setChainId(event.chainId);
      // set the block for each operation into the runtime engine before we run the handler
      Store.setBlock({
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
          ...block.transactions.map((blockTx: { hash: any } | string) => {
            // take a copy of each tx to drop assoc
            return typeof blockTx === "string"
              ? blockTx
              : JSON.parse(JSON.stringify(blockTx));
          }),
        ],
      });
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
    engine?.stage?.checkpoint();

    // log that we're starting
    if (!engine.flags.silent) process.stdout.write(`\n--\n\nEvents processed `);

    // iterate the sorted events and process the callbacks with the given args (sequentially - event loop to process all callbacks)
    while (engine.events.length) {
      // take the first item from the sorted array
      const opSorted = engine.events.shift();

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
        // clear anything added here...
        engine.stage.revert();
        // throw the error again
        throw e;
      }

      // commit the checkpoint on the db...
      await engine?.stage?.commit();
    }

    // await all promises that have been enqueued during execution of callbacks (this will be cleared afterwards ready for the next run)
    await processPromiseQueue(engine.promiseQueue);

    // iterate on the syncs and call withPromises
    for (const group of Object.keys(engine.handlers)) {
      for (const eventName of Object.keys(engine.handlers[group])) {
        if (eventName === "withPromises") {
          // check if we have any postProcessing callbacks to handle (each is handled the same way and is supplied the full promiseQueue)
          await engine.handlers[group][eventName](
            engine.promiseQueue,
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
    if (!engine.flags.silent) process.stdout.write(`(${processed.length}) `);

    // clear the promiseQueue for next iteration
    engine.promiseQueue.length = 0;

    // mark after we end
    if (!engine.flags.silent) process.stdout.write("✔\nEntities stored ");

    // commit the checkpoint on the db...
    await engine?.stage?.commit();

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
  number: number,
  chainId: number,
  validOps: Record<number, Sync[]>,
  collectBlocks: boolean,
  collectTxReceipts: boolean,
  silent: boolean,
  queueLength: number,
  migrations: Record<string, Migration[]>,
  migrationEntities: Record<string, Record<number, Promise<{ id: string }[]>>>,
  blockParts: {
    cancelled?: boolean;
    block: BlockWithTransactions & any;
    receipts: Record<string, TransactionReceipt>;
  }
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
        args: Result;
        tx: TransactionReceipt & TransactionResponse;
        txIndex: number;
      }
  )[] = [];

  // open a new checkpoint whenever we open without one (this will preserve all changes in mem until we get to queueLength === 0)
  if (engine?.stage?.checkpoints.length === 0) {
    engine?.stage?.checkpoint();
  }

  // log that we're syncing the block (*note that writing to stdout directly will bypass chromes inspector logs but will avoid risk of console.log leaking)
  if (!silent)
    process.stdout.write(
      `\n--\n\nSyncing block ${number} (${queueLength} in queue) from ${syncProviders[chainId].network.name} (chainId: ${chainId})\n\nEvents processed `
    );

  // unpack the async parts  - we need the receipts to access the logBlooms (if we're only doing onBlock/onTransaction we won't use the content unless collectTxReceipts is true)
  const { block, receipts, cancelled } = blockParts || {};

  // check that we havent cancelled this operation
  if (!cancelled && block && receipts) {
    // set the chainId into the engine
    Store.setChainId(chainId);
    // set the block for each operation into the engine before we run the handler
    Store.setBlock({
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
        ...block.transactions.map((tx: { hash: any }) => {
          // take a copy of each tx to drop assoc to global mem block
          return tx?.hash ? JSON.parse(JSON.stringify(tx)) : tx;
        }),
      ],
    });

    // check if any migration is relevant in this block
    if (migrations[`${chainId}-${+block.number}`]) {
      // start collecting entities for migration now (this could be expensive - track by index to associate migrationEntities)
      for (const migrationKey of Object.keys(
        migrations[`${chainId}-${+block.number}`]
      )) {
        // ref the migration
        const migration =
          migrations[`${chainId}-${+block.number}`][migrationKey];
        // check for entity
        if (migration.entity) {
          // pull all entities
          const entities = await migrationEntities[
            `${chainId}-${+block.number}`
          ][migrationKey];
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
              // onMigration first
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
            // onMigration first
            txIndex: -2,
            logIndex: -2,
          });
        }
      }
      // clean up migrations after adding events
      delete migrations[`${chainId}-${+block.number}`];
    }

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
              blockNumber: block.number,
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
                      blockNumber: block.number,
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

    // make sure we haven't been cancelled before we get here
    if (!blockParts.cancelled) {
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

      // record length before we started
      const promiseQueueBefore = engine.promiseQueue.length - 1;

      // for each of the sync events call the callbacks sequentially (event loop to process all callbacks)
      while (engine.events.length > 0) {
        // take the first item from the sorted array
        const event = engine.events.shift();

        // create a checkpoint
        engine?.stage?.checkpoint();

        // collect the indexes of any promises made during execution for easy revert
        let promiseQueueBeforeEachProcess: number;
        // wrap in try catch so that we don't leave any checkpoints open
        try {
          // position queue marker at new length
          promiseQueueBeforeEachProcess = engine.promiseQueue.length;
          // process the callback for the given type
          await processCallback(event, blockParts, processed);
        } catch (e) {
          // log any errors from handlers - this should probably halt execution
          console.log(e);
          // should splice only new messages added in this callback from the message queue
          engine.promiseQueue.length = promiseQueueBeforeEachProcess;
          // clear anything added here...
          engine.stage.revert();
          // throw the error again (end exec - skip commit)
          throw e;
        }

        // commit the checkpoint on the db...
        await engine?.stage?.commit();
      }

      // commit or revert
      if (!blockParts.cancelled) {
        // move thes changes to the parent checkpoint
        await engine?.stage?.commit();
      } else {
        // should splice only new messages added in this callback from the message queue
        engine.promiseQueue.length = promiseQueueBefore;
      }

      // print number of events in stdout
      if (!silent) process.stdout.write(`(${processed.length}) `);

      // if we havent been cancelled up to now we can commit this
      if (!blockParts.cancelled) {
        // only attempt to save changes when the queue is clear (or it has been 15s since we last stored changes)
        if (
          queueLength === 0 ||
          ((engine?.lastUpdate || 0) + 15000 <= new Date().getTime() &&
            queueLength < 1000)
        ) {
          try {
            // await all promises that have been enqueued during execution of callbacks (this will be cleared afterwards ready for the next run)
            await processPromiseQueue(engine.promiseQueue);
            // iterate on the syncs and call withPromises
            for (const group of Object.keys(engine.handlers)) {
              for (const eventName of Object.keys(engine.handlers[group])) {
                if (eventName === "withPromises") {
                  // check if we have any postProcessing callbacks to handle
                  await engine.handlers[group][eventName](
                    engine.promiseQueue,
                    {} as unknown as {
                      tx: TransactionReceipt & TransactionResponse;
                      block: Block;
                      logIndex: number;
                    }
                  );
                }
              }
            }
          } catch (e) {
            // print any errors from processing the promise queue section
            if (!engine.flags.silent) console.log(e);
          } finally {
            // clear the promiseQueue for next iteration
            engine.promiseQueue.length = 0;
          }

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

          // commit the checkpoint on the db...
          await engine?.stage?.commit();

          // after all events are stored in db
          if (!silent) process.stdout.write("✔\nPointers updated ");

          // update the lastUpdateTime (we have written everything to db - wait a max of 15s before next update)
          engine.lastUpdate = new Date().getTime();
        }

        // update the startBlock
        engine.startBlocks[chainId] = block.number;

        // record as new latest after all callbacks are complete
        engine.latestBlocks[+chainId] = {
          number: block.number,
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
      }
    }
  }
};
