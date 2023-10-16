import { spawn } from "child_process";

import { errors } from "ethers";
import { JsonRpcProvider, WebSocketProvider } from "@ethersproject/providers";
import { TransactionResponse } from "@ethersproject/abstract-provider";

import { getEngine } from "@/sync/tooling/persistence/store";
import { processListenerBlock } from "@/sync/tooling/processing";
import { saveJSON, readJSON, cwd } from "@/sync/tooling/persistence/disk";

import { AsyncBlockParts, Migration, Sync } from "@/sync/types";

import { getNetworks } from "@/sync/tooling/network/providers";
import { getTransactionReceipt } from "@/sync/tooling/network/transactions";

import { saveListenerBlockAndReceipts as saveListenerBlockAndReceiptsProcess } from "@/sync/tooling/network/process/saveBlock";

// create a listener to attach to the provider
export const createListener = (
  state: {
    inSync: boolean;
    listening?: boolean;
  },
  chainId: number
) => {
  return async (number: number) => {
    // console.log("\nPushing block:", blockNumber, "on chainId:", chainId);
    if (state.listening) {
      // push to the block stack to signal the block is retrievable
      await recordListenerBlock(number, +chainId);
    }
  };
};

// construct an error handler to attach to the provider
export const createErrorHandler = (
  errorHandlers: {
    reject?: (reason?: any) => void;
    resolve?: (value: void | PromiseLike<void>) => void;
  } = undefined
) => {
  return async (error: Error & { code: string }) => {
    // handle the error by code...
    switch (error.code) {
      // case errors.NETWORK_ERROR:
      case errors.SERVER_ERROR:
      case errors.UNSUPPORTED_OPERATION:
        // print the error
        console.error("Server Error:", error.message);
        // reject propagation and trigger close (on all listeners)
        if (errorHandlers?.reject) {
          errorHandlers.reject(error);
        }
        break;
      default:
    }
  };
};

// pull the next block form the queue
export const attemptNextBlock = async (state: {
  inSync: boolean;
  listening?: boolean;
  suspended?: boolean;
}) => {
  // access the engine
  const engine = await getEngine();

  // extract the chainId we will be operating on
  const chainId = +engine.blockQueue[0].chainId;

  // check that this block follows the last completed block sequentially
  const latestBlock = engine.latestBlocks[chainId];

  // if the gap is greater than one then...
  if (
    engine.blockQueue[0].number -
      (latestBlock?.number || engine.blockQueue[0].number) >
    1
  ) {
    // start from the latestBlocks blockNumber...
    let latestBlockNumber = +latestBlock.number;
    // ... we need to close it
    while (latestBlockNumber !== +engine.blockQueue[0].number) {
      // push all prev blocks in ascending block order
      await recordListenerBlock(
        // move to the next block each incr
        (latestBlockNumber += 1),
        chainId,
        0 + (+latestBlock.number - latestBlockNumber - 1)
      );
    }
  }
  // take the next block in the queue (this might not be the same block as the old blockQueue[0] but chainId will be the same)
  const block = engine.blockQueue.shift();

  // double check we spliced something
  if (block.number && block.asyncParts) {
    // attempt to process the events in this block (record currentProcess so we can wait for it to complete before closing)...
    engine.currentProcess = processListenerBlockSafely(
      block.number,
      chainId,
      block.asyncParts
    );

    // await the currentProcess
    await engine.currentProcess;

    // delete these components
    delete block.number;
    delete block.chainId;
    delete block.asyncParts;

    // clear the currentProcess
    engine.currentProcess = Promise.resolve();
  }

  // close listener when queue is clean
  if (!engine.blockQueue.length) {
    state.suspended = true;
  }
};

// attach listeners (listening for blocks) to enqueue prior to processing
export const attachListeners = async (
  state: {
    inSync: boolean;
    listening?: boolean;
  } = {
    inSync: true,
    listening: true,
  },
  migrations: Migration[] = [],
  errorHandlers: {
    reject?: (reason?: any) => void;
    resolve?: (value: void | PromiseLike<void>) => void;
  } = undefined
) => {
  // retrieve engine
  const engine = await getEngine();

  // get all chainIds for defined networks
  const { syncProviders } = await getNetworks();

  // array of blocks that need to be processed...
  engine.blockQueue = engine.blockQueue || [];
  // index all migrations
  engine.indexedMigrations = migrations.reduce((index, migration) => {
    // start arr to keep migrations for chainId at blockNumber
    index[`${migration.chainId}-${migration.blockNumber}`] =
      index[`${migration.chainId}-${migration.blockNumber}`] || [];

    // store the migration
    index[`${migration.chainId}-${migration.blockNumber}`].push(migration);

    // return the index
    return index;
  }, {} as Record<string, Migration[]>);

  // store all in sparse array (as obj)
  engine.indexedMigrationEntities = engine.indexedMigrationEntities || {};

  // process these ops once now (then again after every block)
  const ops = await getValidSyncOps();

  // attach a single listener for each provider
  const listeners = await Promise.all(
    Object.keys(syncProviders).map(async (chainId) => {
      // proceed with valid ops
      if (ops[+chainId].length) {
        // construct a listener to attach to the provider
        const listener = createListener(state, +chainId);
        // construct an error handler to attach to the provider
        const handler = createErrorHandler(errorHandlers);

        // attach this listener to onBlock to start collecting blocks
        syncProviders[+chainId].on("block", listener);
        // attach close to error handler
        syncProviders[+chainId].on("error", handler);

        // return detach method to stop the handler - this will be triggered onError
        return async () => {
          // stop listening for new errors and blocks first
          syncProviders[+chainId].off("error", handler);
          syncProviders[+chainId].off("block", listener);

          // wait for the current block to complete
          if (engine.currentProcess) await engine.currentProcess;

          // remove the lock for the next iteration
          engine.latestEntity[chainId].set("locked", false);
          // persist changes into the store
          engine.latestEntity[chainId] = await engine.latestEntity[
            chainId
          ].save();
        };
      }

      // no valid ops
      return false;
    })
  );

  // retun a handler to remove the listener
  return [
    // all valid listeners
    ...listeners.filter((v) => v),
  ];
};

// begin processing blocks
export const attachBlockProcessing = async (
  state: {
    inSync: boolean;
    listening?: boolean;
    suspended?: boolean;
  },
  errorHandlers: {
    reject?: (reason?: any) => void;
    resolve?: (value: void | PromiseLike<void>) => void;
  } = undefined
) => {
  // get the engine
  const engine = await getEngine();

  // catch any errors
  try {
    // pull from the reqStack and process...
    while (state.listening && !state.suspended) {
      // once the state moves to inSync - we can start taking blocks from the array to process
      if (engine.blockQueue.length && state.inSync) {
        // take the next block from the challenge queue (this operation can take up to a max of 30's before it will be cancelled and reattempted)
        await attemptNextBlock(state);
      } else {
        // wait 1 second for something to enter the queue
        await new Promise((resolve) => {
          setTimeout(resolve, 1000);
        });
      }
    }
  } catch (e) {
    // reject propagation and close
    if (errorHandlers?.reject) {
      errorHandlers.reject(e);
    }
  }
};

// restructure ops into ops by chainId
export const getValidSyncOps: () => Promise<
  Record<number, Sync[]>
> = async () => {
  // get the engine
  const engine = await getEngine();
  // get all chainIds for defined networks
  const networks = await getNetworks();

  // return the indexed valid ops
  return Object.keys(networks.syncProviders)
    .map((chainId) => {
      // filter for ops associated with this chainid
      return {
        chainId: +chainId,
        validOps: engine.syncs.filter(
          (op) =>
            // is for chainId
            op.provider.network.chainId === +chainId
        ),
      };
    })
    .reduce((all, op) => {
      return {
        ...all,
        [op.chainId]: op.validOps,
      };
    }, {});
};

// get a receipt for the given details
export const getReceipt = async (
  tx: TransactionResponse,
  chainId: number,
  provider: JsonRpcProvider | WebSocketProvider
) => {
  // retrieve the engine to check flags
  const engine = await getEngine();

  // this promise.all is trapped until we resolve all tx receipts in the block
  try {
    // get the receipt
    const fullTx = await getTransactionReceipt(provider, tx);

    // try again
    if (!fullTx.transactionHash) throw new Error("Missing hash");

    // if we're tmp storing data...
    if (!engine.flags.cleanup) {
      // save each tx to disk to release from mem
      await saveJSON(
        "transactions",
        `${chainId}-${fullTx.transactionHash}`,
        fullTx as unknown as Record<string, unknown>
      );
    }

    // return the tx
    return fullTx;
  } catch {
    // attempt to get receipt again on failure
    return getReceipt(tx, chainId, provider);
  }
};

// get data using a process to move reqs out of blocking path
export async function saveListenerBlockAndReceiptsInSpawnedProcess(
  block: number,
  chainId: number
): Promise<boolean> {
  // retrieve engine
  const engine = await getEngine();
  const { syncProviders } = await getNetworks();

  // spawn a child_process to save the block and transaction receipts
  return new Promise<boolean>((resolve, reject) => {
    // spawn the child process
    const child = spawn(
      "node",
      [
        // absolute path to the child process script
        `${__dirname}/process/saveBlock.js`,
        // pass args as strings (we will resolve them on the otherside)
        block.toString(10),
        chainId.toString(10),
        syncProviders[+chainId].connection.url,
        cwd,
        engine.flags.cleanup.toString(),
      ],
      {
        shell: true, // Use shell to execute the command
        cwd: process.cwd(), // Pass current cwd as workding dir
      }
    );

    // when the handle exits the file is saved
    child.on("exit", (code) => {
      if (code === 0) {
        resolve(true);
      } else {
        reject(new Error(`Child process exited with an error code: ${code}`));
      }
    });
  }).catch(() => {
    // resolve to false (we didn't save the block & receipts)
    return false;
  });
}

// pull all block and receipt details (cancel attempt after 30s)
export const saveListenerBlockAndReceipts = async (
  number: number,
  chainId: number
) => {
  // retrieve engine
  const engine = await getEngine();

  // if we're multithreading the collection use the spaw to complete the collection...
  if (!engine.flags.multithread) {
    // get all chainIds for defined networks
    const { syncProviders } = await getNetworks();

    // if we get any errors here then return false, we'll restack the block when we get to it...
    try {
      // save the block and receipt using the saveBlockProcess but without spawning children to do it...
      return await saveListenerBlockAndReceiptsProcess(
        number,
        chainId,
        syncProviders[+chainId].connection.url,
        cwd,
        engine.flags.cleanup
      );
    } catch {
      return false;
    }
  }

  // return using the spawn process (create a new process to collect each blocks details)
  return saveListenerBlockAndReceiptsInSpawnedProcess(number, chainId);
};

// read a block and its receipt from disk
export const readListenerBlockAndReceipts = async (
  number: number,
  chainId: number
) => {
  try {
    // read block and receipts from the disk stored blockAndReceipts
    const { block, receipts } = await readJSON(
      "blockAndReceipts",
      `${chainId}-${+number}`
    );

    return {
      block,
      receipts,
    } as AsyncBlockParts;
  } catch {
    return false as unknown as AsyncBlockParts;
  }
};

// stop waiting for a read action
const cancelReadWait = () => {
  return new Promise<AsyncBlockParts>((resolve) => {
    setTimeout(() => resolve({} as unknown as AsyncBlockParts), 3000);
  });
};

// check for the current value set in wraper
const cancelOp = async (
  blockAndReceipts: Promise<AsyncBlockParts>,
  resolve: () => void
) => {
  // set cancelled on the asyncParts obj we're passing through processListenerBlock
  if (blockAndReceipts) {
    // await the blockAndReceipt promise
    const vals = await Promise.race([cancelReadWait(), blockAndReceipts]);
    // mark as cancelled
    vals.cancelled = true;
    // delete from wrapper
    delete vals?.block;
    delete vals?.receipts;
  }
  // resolve to end processing
  resolve();
};

// method to cancel the block after a timeout (default of 10s)
const cancelListenerBlockAfterTimeout = async (
  blockAndReceipts: Promise<AsyncBlockParts>,
  timeout: number = 10000,
  cancelRef: {
    timeout?: NodeJS.Timeout;
    resolve?: () => void | Promise<void>;
    resolver?: (
      blockAndReceipts: Promise<AsyncBlockParts>,
      resolve: () => void | Promise<void>
    ) => Promise<void>;
  } = {}
) => {
  // return a promise to resolve the cancelation on the provided vals
  return new Promise<void>((resolve) => {
    // store the resolver
    cancelRef.resolver = cancelOp;
    // attach caller to the ref
    cancelRef.resolve = () => cancelRef.resolver(blockAndReceipts, resolve);
    // add another 60s to process the block
    cancelRef.timeout = setTimeout(cancelRef.resolve, Math.max(timeout, 3000)); // min of 3s per block, default of 10s - we'll adjust if needed
  });
};

// record the block for the given chainId
export const recordListenerBlock = async (
  number: number,
  chainId: number,
  position?: number
) => {
  // access db via the engine
  const engine = await getEngine();

  // check if any migration is relevant in this block
  if (engine.indexedMigrations[`${chainId}-${number}`]) {
    // start collecting entities for migration now (this could be expensive)
    engine.indexedMigrations[`${chainId}-${number}`].forEach(
      (migration, key) => {
        engine.indexedMigrationEntities[`${chainId}-${number}`] =
          engine.indexedMigrationEntities[`${chainId}-${number}`] || {};
        engine.indexedMigrationEntities[`${chainId}-${number}`][key] =
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
  await stackListenerBlock(
    number,
    chainId,
    // position at the end of the queue
    position ?? engine.blockQueue.length
  );
};

// restack this at the top so that we can try again
const stackListenerBlock = async (
  number: number,
  chainId: number,
  position: number
) => {
  // collect engine to insert onto blockQueue
  const engine = await getEngine();
  // construct a new block to splice into the queue
  const block = {
    number,
    chainId,
    // recreate the async parts to pull everything fresh (we can switch betwe)
    asyncParts: saveListenerBlockAndReceipts(number, chainId),
  };
  // position the new block into the blockQueue
  if (position === 0) {
    // start position
    engine.blockQueue.unshift(block);
  } else if (position === engine.blockQueue.length) {
    // last position
    engine.blockQueue.push(block);
  } else {
    // splice at any other pos
    engine.blockQueue.splice(position, 0, block);
  }
};

// begine processing the block
export const startProcessingBlock = async (
  number: number,
  chainId: number,
  blockAndReceipts: Promise<AsyncBlockParts>,
  cancelRef: {
    timeout?: NodeJS.Timeout;
    resolve?: () => void | Promise<void>;
    resolver?: (
      blockAndReceipts: Promise<AsyncBlockParts>,
      resolve: () => void | Promise<void>
    ) => Promise<void>;
  }
) => {
  // using the engine to access flags
  const engine = await getEngine();

  // wrapped so that we can reattempt if we see any errors
  try {
    // attempt to process the block
    await processListenerBlock(
      // blockNumber being processed...
      number,
      // the chainId it belongs to...
      +chainId,
      // process validOps [for this chain] each tick to associate any new syncs (cache and invalidate?)
      await getValidSyncOps(),
      // pass through the config...
      engine.flags.collectBlocks,
      engine.flags.collectTxReceipts,
      engine.flags.silent,
      // pass through the length of the queue for reporting and for deciding if we should be saving or not
      engine.blockQueue.length,
      // helper parts to pass through entities, block & receipts...
      engine.indexedMigrations,
      engine.indexedMigrationEntities,
      // pass through the promise which is reading the block data back from disk
      blockAndReceipts
    );
  } catch (e) {
    // log the error
    console.log(e);
    // mark the failed block as cancelled and cleanup
    blockAndReceipts = blockAndReceipts.then((vals: AsyncBlockParts) => {
      // if theres no place data default to empty obj
      if (!vals) {
        vals = {} as AsyncBlockParts;
      }
      // place by ref incase anything is still ongoing
      vals.cancelled = true;

      // return vals with cancellation in place
      return vals;
    });
  } finally {
    // if any of this cleanup logic throws we don't need to do anything with the error...
    try {
      // final await on the block and receipts
      const vals = await blockAndReceipts;
      // delete all async entities for this blockNum
      delete engine.indexedMigrations[`${chainId}-${+number}`];
      // delete all async entities for this blockNum
      delete engine.indexedMigrationEntities[`${chainId}-${+number}`];
      // clear timeout to prevent cancellation
      if (typeof cancelRef.resolver !== "undefined") {
        // clear the timeout to prevent calling the handler
        clearTimeout(cancelRef.timeout);
        // set the resolver but don't clear the timeout yet because we want it to resolve its promise
        cancelRef.resolver = async (_, resolve) => {
          // mark for g/c
          delete cancelRef.resolve;
          delete cancelRef.resolver;
          delete cancelRef.timeout;
          // resolve the promise to clear ref
          await resolve();
        };
      }
      // restack the request if any parts we're missing
      if (!vals || vals?.cancelled || !vals?.block || !vals?.receipts) {
        // reattempt the timedout block
        await stackListenerBlock(number, chainId, 0);
      } else {
        // delete cancelled marker
        delete vals?.cancelled;
        // delete all receipts
        Object.keys(vals?.receipts || []).forEach((index) => {
          delete vals?.receipts?.[index];
        });
        // delete details from the wrapper
        delete vals?.block;
        delete vals?.receipts;
      }
    } finally {
      // clear the cancelListenerBlockAfterTimeout promise (by resolving it now)
      if (typeof cancelRef.resolver !== "undefined") {
        // call resovle to clear the promise after completing this callback
        setTimeout(cancelRef.resolve);
      }
    }
  }

  // done
  return true;
};

// process the events from a block
export const processListenerBlockSafely = async (
  number: number,
  chainId: number,
  asyncParts: Promise<boolean>
) => {
  // get the engine
  const engine = await getEngine();
  // check if this block needs to be processed (check its not included in the catchup-set)
  if (
    engine.startBlocks[+chainId] &&
    // @TODO: replace these with exact number checks
    number >= engine.startBlocks[+chainId] &&
    number >= engine.latestBlocks[+chainId].number
  ) {
    // track cancellation timeout for cancelling
    const cancelRef = {};
    // await for the write operation to complete
    const saved = await asyncParts;
    // check that we saved it
    if (saved) {
      // read the data back into memory (can this fail? - should we just block here until we read it?)
      const blockAndReceipts = readListenerBlockAndReceipts(number, chainId);

      // wrap in a race here so that we never spend too long stuck on a block
      await Promise.race([
        // place a promise to cancel the block in 10s (configurable?)
        cancelListenerBlockAfterTimeout(blockAndReceipts, 10000, cancelRef),
        // attempt to resolve everything that happened in the block...
        startProcessingBlock(number, chainId, blockAndReceipts, cancelRef),
      ]);
    } else {
      // reattampt the same block
      await stackListenerBlock(number, chainId, 0);
    }
  }
};
