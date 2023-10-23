// Using v8 to measure memory usage between swaps
import * as v8 from "v8";

// Using ethers errors in errorHandler to code check error types
import { errors } from "ethers";

// Use global supagraph engine to associate listeners with ingestor
import { getEngine } from "../persistence/store";
import { getNetworks } from "./providers";

// Supagraph typings
import { Migration, Sync } from "../../types";
import { processListenerBlock } from "../processing";

// Constants used in post requests
const method = "POST";
const headers = {
  "Content-Type": "application/json",
};

// Type definitions used by the class
interface Block {
  number: number;
  chainId: number;
}
interface BlockData {
  number: number;
  transactions: { hash: string }[];
}
interface Tx {
  chainId: number;
  hash: string;
}
interface TxData {
  transactionHash: string;
}
interface LineEntry {
  block: BlockData;
  receipts: TxData[];
  chainId: number;
  number: number;
}

// Class to contain block processing operations
export class Ingestor {
  // Object of RPC urls by chainId
  private rpcUrls: Record<number, string> = {};

  // hold an array for temp storage
  private blockSaveStream1: {
    block: BlockData;
    receipts: TxData[];
    chainId: number;
    number: number;
  }[] = [];

  // hold array for double-buffer storage
  private blockSaveStream2: {
    block: BlockData;
    receipts: TxData[];
    chainId: number;
    number: number;
  }[] = [];

  // Which of the two output files is currently active (double buffer switch)
  private blockSaveStreamSwitch: boolean = false;

  // Blocks which have been fetched and are ready to write to the blockSaveStream
  private blockBuffers: Record<
    number,
    Record<
      number,
      {
        block: BlockData;
        receipts: TxData[];
        chainId: number;
        number: number;
      }
    >
  > = {};

  // A list of blocks which need to be fetched
  private incomingBlockQueue: Block[] = [];

  // A list of blocks we are currently fetching
  private outgoingBlockQueue: Block[] = [];

  // Transactions which have been fetched and are ready to use in the receipts portion on the blockSaveStream
  private transactionBuffers: Record<number, Record<string, TxData>> = {};

  // Track incoming tx requests
  private incomingTransactionQueue: Tx[] = [];

  // Keep track of pending tx requests
  private outgoingTransactionQueue: Tx[] = [];

  // Keep track of the number of lines written / pending
  private pendingBlockCount: number = 0;

  // The pool of workers who are collecting block information
  private blockWorkerPool: Promise<void>[];

  // Another pool for transaction workers
  private transactionWorkerPool: Promise<void>[] = [];

  // Should we be logging to the console?
  private silent: boolean = false;

  // Is the system halted?
  private halted: boolean = true;

  // Is the system in swap mode (prevent writes until complete)
  private swapping: boolean = false;

  // Mark when to start listening for addBlock events
  private listening: boolean = false;

  // The maximum number of retries before throwing an error when fetchign
  private maxRetries: number = 3;

  // The number of workers to use for block data fetching
  private numBlockWorkers: number = 10;

  // The number of workers to use for tx data fetching
  private numTransactionWorkers: number = 22;

  // Method to call with each line we discover
  private withLine: (
    ingestor: Ingestor,
    line: LineEntry
  ) => void | Promise<void>;

  // Construct with options...
  constructor({
    withLine,
    rpcUrls = {},
    silent = false,
    maxRetries = 3,
    numBlockWorkers = 10,
    numTransactionWorkers = 22,
  }: {
    withLine: (ingestor: Ingestor, line: LineEntry) => void | Promise<void>;
    rpcUrls?: Record<number, string>;
    silent?: boolean;
    maxRetries?: number;
    numBlockWorkers?: number;
    numTransactionWorkers?: number;
  }) {
    // copy user supplied options to context
    this.withLine = withLine;
    // extend rpcUrls with user definitions
    this.rpcUrls = { ...this.rpcUrls, ...rpcUrls };
    // record options
    this.maxRetries = maxRetries;
    this.numBlockWorkers = numBlockWorkers;
    this.numTransactionWorkers = numTransactionWorkers;
    // optionally print heap dumps every file switch
    this.silent = silent;
    // initiate the workerPool
    this.blockWorkerPool = new Array(this.numBlockWorkers);
    this.transactionWorkerPool = new Array(this.numTransactionWorkers);
  }

  // Expose the length of the blockQueue
  public get blockQueueLength() {
    // count all places that blocks can be (pendingBlockCount will keep track of what is written to disk)
    return (
      this.pendingBlockCount +
      this.incomingBlockQueue.length +
      this.outgoingBlockQueue.length
    );
  }

  // Get the appropriate RPC url for the chainId
  private getRpcUrl(chainId: number): string {
    return this.rpcUrls[chainId] || "https://default-rpc-url.com";
  }

  // Logic to add a block to the queue
  public addBlock(block: Block) {
    this.incomingBlockQueue.push(block);
  }

  // Logic to start the workers in both pool
  public async startWorkers() {
    // if we havent started the listeners yet
    if (!this.listening) {
      // unhalt the system
      this.halted = false;
      // mark listener open
      this.listening = true;
      // open the workers
      for (let i = 0; i < this.numBlockWorkers; i += 1) {
        this.startBlockWorker(i);
      }
      // open the tx workers
      for (let i = 0; i < this.numTransactionWorkers; i += 1) {
        this.startTransactionWorker(i);
      }
    }
  }

  // Logic to start processing blocks (swapOutputFiles to read pending blocks and to initiate recursive double-buffer processing)
  public async startProcessing() {
    // swap output files to start processing anything added before now
    return this.swapOutputFiles().then(() => this.startWorkers());
  }

  // Logic to stop processing and clear the queue if needed
  public async stopProcessing(clear?: boolean) {
    // mark closed on the instance
    this.halted = true;
    this.listening = false;
    // await current work to finish - as halted is marked we won't replace these workers
    await Promise.all(this.blockWorkerPool.map(async (promise) => promise));
    await Promise.all(
      this.transactionWorkerPool.map(async (promise) => promise)
    );
    // switch the active file to the one we've just been using
    this.blockSaveStreamSwitch = !this.blockSaveStreamSwitch;
    // clear the workers pool
    this.blockWorkerPool.length = 0;
    this.transactionWorkerPool.length = 0;
    // if we're not clearing we should restack any blocks which are partially processed
    if (!clear) {
      // clear the buffer
      this.blockBuffers = {};

      // restack any blocks in the pending queue
      const blocks: Block[] = [];
      await this.processPreviousFile((_, line: Block) => {
        blocks.push(line);
      });

      // restore any blocks from the outgoing queue
      if (this.outgoingBlockQueue.length) {
        while (this.outgoingBlockQueue.length) {
          blocks.push(this.outgoingBlockQueue.shift());
        }
      }

      // if we have blocks then unshift them back into the incoming queue
      if (blocks.length) {
        this.incomingBlockQueue.unshift(...blocks);
      }
    } else {
      // clear all buffers and queues
      this.blockBuffers = {};
      this.incomingBlockQueue.length = 0;
      this.outgoingBlockQueue.length = 0;
    }
    // cleanup txQueue, we don't need to reuse anything here
    this.transactionBuffers = {};
    this.incomingTransactionQueue.length = 0;
    this.outgoingTransactionQueue.length = 0;
  }

  // Logic to process a block and to trigger its buffering
  private async processBlock(block: BlockData, chainId: number) {
    // ensure the buffer exists for the given chain
    if (!this.blockBuffers[chainId]) this.blockBuffers[chainId] = {};

    // fetch and append receipts for each transaction in the block (this will block thread until all transactions are resolved)
    // wonder if we should timeout this op - we would need to clear from queues and buffer
    const blockAndReceipts = await this.fetchAndAppendReceipts(block, chainId);

    // place the block into the buffer
    this.blockBuffers[chainId][+block.number] = {
      ...blockAndReceipts,
      number: +block.number,
      chainId,
    };

    // buffer operation will always write a single block to the saveStream (eventually)
    this.processBuffer();
  }

  // Logic to process blocks in the buffer
  private processBuffer() {
    // when we have items in the outgoing queue
    if (this.outgoingBlockQueue.length) {
      // pick the next one
      const next = this.outgoingBlockQueue[0];
      // check if its buffered entry is ready
      const nextBlock = this.blockBuffers[next.chainId][next.number];

      // when we have the nextBlock and we're not in swapping mode...
      if (!this.swapping && nextBlock) {
        // write to the block stream file for processing on next active switch
        this.writeBlockToStream(this.outgoingBlockQueue.shift(), nextBlock);
      } else {
        // wait a tick and process the buffer again (if we called this then there is a block waiting to be written)
        setImmediate(() => this.processBuffer());
      }
    }
  }

  // Logic to write a block to the stream
  private writeBlockToStream(block: Block, blockData: LineEntry) {
    setImmediate(() => {
      // pushh to the active blockStream
      (this.blockSaveStreamSwitch
        ? this.blockSaveStream1
        : this.blockSaveStream2
      ).push({
        number: block.number,
        chainId: block.chainId,
        block: blockData.block,
        receipts: blockData.receipts,
      });
      // incr the pending line count (decr when we read it)
      this.pendingBlockCount += 1;
      // delete the buffered entry for gc to collect
      delete this.blockBuffers[block.chainId][block.number];
    });
  }

  // Process a transaction (place it in the buffer ready to be recognised by the polling sequence in fetchAndAppendReceipts)
  private async processTransaction(receipt: TxData, chainId: number) {
    // ensure the buffer exists for the given chain
    if (!this.transactionBuffers[chainId]) {
      this.transactionBuffers[chainId] = {};
    }

    // place the transaction into the buffer
    this.transactionBuffers[chainId][receipt.transactionHash] = receipt;
  }

  // fetch the receipts and return them along with the block
  private async fetchAndAppendReceipts(
    block: BlockData,
    chainId: number
  ): Promise<{ block: BlockData; receipts: TxData[] }> {
    try {
      if (
        // check for length
        block.transactions.length &&
        // not the most efficient - but we want to check the tx's havent already been stacked
        this.incomingTransactionQueue.findIndex(
          (tx) =>
            tx &&
            tx.hash ===
              (typeof block.transactions[0] === "string"
                ? block.transactions[0]
                : block.transactions[0].hash)
        ) === -1 &&
        this.outgoingTransactionQueue.findIndex(
          (tx) =>
            tx &&
            tx.hash ===
              (typeof block.transactions[0] === "string"
                ? block.transactions[0]
                : block.transactions[0].hash)
        ) === -1
      ) {
        // append all items to the queue for processing
        this.incomingTransactionQueue.push(
          ...block.transactions
            .filter((v) => v)
            .map((tx) => {
              return {
                chainId,
                hash: typeof tx === "string" ? tx : tx.hash,
              };
            })
        );
        // throw to wait 1 seconds
        throw new Error("We need to wait for the transactions to be processed");
      } else {
        // attempt to collect all receipts
        const receipts = await Promise.all(
          block.transactions
            .filter((v) => v)
            .map((tx) => {
              return new Promise<TxData>((resolve, reject) => {
                // return the receipts
                if (this.transactionBuffers[chainId][tx.hash]) {
                  resolve(this.transactionBuffers[chainId][tx.hash]);
                } else {
                  reject();
                }
              });
            })
        ).catch((e) => {
          // throw in context
          throw e;
        });

        // iterate and delete from buffer
        block.transactions.forEach((tx) => {
          // make sure the tx exists
          if (tx) {
            // discover del index
            const delIndex = this.outgoingTransactionQueue.findIndex(
              (queueTx) =>
                queueTx &&
                (typeof tx === "string" ? tx : tx.hash) === queueTx.hash
            );
            // delete from the buffer
            delete this.transactionBuffers[chainId][
              typeof tx === "string" ? tx : tx.hash
            ];
            // delete from the queue
            if (delIndex !== -1) delete this.outgoingTransactionQueue[delIndex];
          }
        });

        // return block and receipts
        return {
          block,
          receipts,
        };
      }
    } catch (e) {
      // check again in 1s - all transactions must eventually resolve
      return new Promise((resolve, reject) => {
        // if we're waiting on resolution try again, else throw
        if (
          e.toString() ===
          "Error: We need to wait for the transactions to be processed"
        ) {
          setTimeout(() => {
            try {
              // attempt to resolve the fetch again
              resolve(this.fetchAndAppendReceipts(block, chainId));
            } catch (err) {
              // any hard rejections should carry to outer promise
              reject(err);
            }
          }, 1e3);
        } else {
          // clear all tx's from queues
          block.transactions.forEach((tx) => {
            // delete all from outgoing
            const outgoingDelIndex = this.outgoingTransactionQueue.findIndex(
              (queueTx) =>
                queueTx &&
                (typeof tx === "string" ? tx : tx.hash) === queueTx.hash
            );
            if (outgoingDelIndex !== -1)
              delete this.outgoingTransactionQueue[outgoingDelIndex];
            // delete all from incoming
            const incomingDelIndex = this.incomingTransactionQueue.findIndex(
              (queueTx) =>
                queueTx &&
                (typeof tx === "string" ? tx : tx.hash) === queueTx.hash
            );
            if (incomingDelIndex !== -1)
              delete this.incomingTransactionQueue[incomingDelIndex];
            // delete from the buffer incase of bogus response
            delete this.transactionBuffers[chainId][
              typeof tx === "string" ? tx : tx.hash
            ];
          });
          // throw in outer context to throw the block fetch (will throw parent to throw parent etc...)
          reject(e);
        }
      });
    }
  }

  // Worker definition to take a block and to fetch its content
  private async blockWorker(index: number) {
    while (!this.halted) {
      // take the next block to be processed from the incoming queue
      const block = this.incomingBlockQueue.shift();

      // if theres no block then skip until there is one
      if (!block) {
        break;
      }

      // if the block hasnt been recorded on to the outgoing queue yet...
      if (this.outgoingBlockQueue.indexOf(block) === -1) {
        // record the incoming in the outgoing track to order the async results
        this.outgoingBlockQueue.push(block);
      }

      // wrap this so that we can catch errors for retry attempts - these should always succeed
      try {
        // collect the block data
        const blockData = await this.fetchBlockDataWithRetries(
          block.chainId,
          block.number
        );

        // once collected pass the block along for processing
        if (blockData) {
          // attempt to process the block
          await this.processBlock(blockData, block.chainId);
        } else {
          // cannot proceed - restack and retry
          throw new Error("BlockData must resolve");
        }
      } catch (e) {
        // place it back on to the queue so we process it again next tick
        this.incomingBlockQueue.unshift(block);
      }
    }

    // in the next-tick, check that we havent halted execution before starting a new worker to replace ourself
    setTimeout(() => {
      if (!this.halted) this.startBlockWorker(index);
    });
  }

  // Start workers to fetch block data
  private startBlockWorker(index: number) {
    // start worker and set it against the pool
    this.blockWorkerPool[index] = this.blockWorker(index);
  }

  // Create workers to process transactions along side blocks (blocks thread is block by transaction thread)
  private async transactionWorker(index: number) {
    while (!this.halted) {
      // take the next tx to be processed from the incoming queue
      const tx = this.incomingTransactionQueue.shift();

      // if theres no tx then skip until there is one
      if (!tx) {
        break;
      }

      // if the tx hasnt been recorded on to the outgoing queue yet...
      if (this.outgoingTransactionQueue.indexOf(tx) === -1) {
        this.outgoingTransactionQueue.push(tx);
      }

      // wrap this so that we can catch errors for retry attempts - these should always succeed
      try {
        // fetch and append the transaction receipt
        const receipt = await this.fetchTxDataWithRetries(tx.chainId, tx);

        // once collected pass the tx along for processing
        if (receipt) {
          // attempt to process the tx
          await this.processTransaction(receipt, tx.chainId);
        } else {
          // cannot proceed - restack and retry
          throw new Error("TxData must resolve");
        }
      } catch (e) {
        // place it back on to the queue so we process it again next tick
        this.incomingTransactionQueue.unshift(tx);
      }
    }

    // in the next-tick, check that we havent halted execution before starting a new worker to replace ourself
    setTimeout(() => {
      if (!this.halted) this.startTransactionWorker(index);
    });
  }

  // Start transaction worker at index to replace itself
  private startTransactionWorker(index: number) {
    this.transactionWorkerPool[index] = this.transactionWorker(index);
  }

  // Fetch the block data from the appropriate RPC
  private async fetchTxDataWithRetries(
    chainId: number,
    tx: { hash: string } | string
  ) {
    return fetchDataWithRetries<TxData>(
      this.getRpcUrl(chainId),
      "eth_getTransactionReceipt",
      [typeof tx === "string" ? tx : tx.hash],
      this.maxRetries,
      this.silent
    );
  }

  // Fetch the block data from the appropriate RPC
  private async fetchBlockDataWithRetries(chainId: number, number: number) {
    return fetchDataWithRetries<BlockData>(
      this.getRpcUrl(chainId),
      "eth_getBlockByNumber",
      [`0x${number.toString(16)}`, true],
      this.maxRetries,
      this.silent
    );
  }

  // Logic to switch the current file buffer and to trigger a content flush
  private async swapOutputFiles() {
    // stop swapping when halted
    if (!this.halted) {
      // if blocks have been added to the inactive file, switch and read (else just timeout for another second)
      if (this.pendingBlockCount > 0) {
        // mark that we've entered swapping mode (temp lock)
        this.swapping = true;

        // mark the switch
        this.blockSaveStreamSwitch = !this.blockSaveStreamSwitch;

        // mark end of swapping mode
        this.swapping = false;

        // process all content from the inactive file (wait until all lines have been processed)
        await this.processPreviousFile(this.withLine);
      }

      // switch back to the inactive file in 1second (which should give the current active file time to accumulate data for processing)
      setTimeout(this.swapOutputFiles.bind(this), 1e3);
    }
  }

  // Process the inactive file...
  private async processPreviousFile(
    withLine: (ingestor: Ingestor, line: LineEntry) => void | Promise<void>
  ) {
    // blockSaveStreamSwitch switched just before we entered this function, we want to read the opposite file to the saveStream
    const previousReadArray = this.blockSaveStreamSwitch
      ? this.blockSaveStream2
      : this.blockSaveStream1;

    // for await (const line of previousReadArray) {
    while (previousReadArray.length > 0) {
      // shift the line and process
      const line = previousReadArray.shift();
      // process the line
      try {
        // attempt the provided withLine call
        await withLine(this, line);
      } catch (e) {
        // log errors but dont stop
        if (!this.silent) console.error(e);
      } finally {
        // remove from count
        this.pendingBlockCount -= 1;
      }
    }

    // print heapdump and trigger gc
    this.v8Print("Swap file heap dump");
  }

  // Print a heap dump if gc is enabled for manual management
  private v8Print(message: string) {
    if (!this.silent && global.gc) {
      global.gc();
      const heapStats = v8.getHeapStatistics();
      const heapStatsMB = heapStats;
      // eslint-disable-next-line guard-for-in
      for (const key in heapStatsMB) {
        heapStatsMB[key] = `${(
          ((heapStatsMB[key] / 1024 / 1024) * 100) /
          100
        ).toFixed(2)} MB`;
      }
      console.log("");
      console.log(message);
      console.table(heapStatsMB);
      console.log("");
    }
  }
}

// Method to fetch data via a given rpc url
async function fetchDataWithRetries<T>(
  url: string,
  rpcMethod: string,
  rpcParams: unknown[],
  maxRetries: number = 3,
  silent: boolean = true
) {
  // keep response outside so we can cancel the req in finally
  let response: Response;

  // attempt to fetch the block data
  for (let retry = 1; retry <= maxRetries; retry += 1) {
    try {
      // set an abort controller to timeout the request
      const controller = new AbortController();

      // timeout after 30seconds
      const timeout = setTimeout(() => {
        controller.abort();
      }, 30e3);

      // use fetch to grab the blockdata directly from the rpcUrl endpoint
      response = await fetch(url, {
        method,
        headers,
        body: JSON.stringify({
          jsonrpc: "2.0",
          method: rpcMethod,
          params: rpcParams,
          id: Math.floor(Math.random() * 1e8),
        }),
        signal: controller.signal,
      });

      // collect the response
      const data = await response.json();

      // clear the abort
      clearTimeout(timeout);

      // return the result
      if (data.result) {
        return data.result as T;
      }
    } catch (error) {
      // if theres an error we'll try again upto maxRetries
      if (!silent)
        console.error(`Error fetching ${rpcMethod} (retry ${retry}):`, error);
    } finally {
      // finally if we returned or errored, we need to cancel the body to close the resource
      if (response && !response.bodyUsed) {
        response.body.cancel();
      }
    }
  }

  // if we fail to return the result then we should restack the task
  return null;
}

// Construct an ingestor to process blocks as they arrive
export async function createIngestor(
  numBlockWorkers: number,
  numTransactionWorkers: number,
  printIngestorErrors: boolean = false
) {
  // get the engine
  const engine = await getEngine();
  // get all chainIds for defined networks
  const { syncProviders } = await getNetworks();

  // return the new ingestor
  return new Ingestor({
    // place a withLine function to trigger processing
    withLine: async (ingestor, line) => {
      // attempt to process the block
      await processListenerBlock(
        // blockNumber being processed...
        +line.number,
        // the chainId it belongs to...
        +line.chainId,
        // process validOps [for this chain] each tick to associate any new syncs (cache and invalidate?)
        await getValidSyncOps(),
        // pass through the config...
        engine.flags.collectBlocks,
        engine.flags.collectTxReceipts,
        engine.flags.silent,
        // pass through the length of the queue for reporting and for deciding if we should be saving or not
        ingestor.blockQueueLength - 1, // -1 to offset the line we're currently printing
        // helper parts to pass through entities, block & receipts...
        engine.indexedMigrations,
        engine.indexedMigrationEntities, // <-- TODO: reimplement this.
        // pass through the promise which is reading the block data back from disk
        Promise.resolve({
          // this is the full block with TransactionResponses
          block: line.block,
          // this is an index of all receipts against their txHash
          receipts: line.receipts.reduce((receipts, receipt) => {
            // index the receipts against their hash
            receipts[receipt.transactionHash] = receipt;

            // return all indexed receipts
            return receipts;
          }, {}),
        })
      );
    },
    // return an object of all available rpc urls
    rpcUrls: Object.keys(syncProviders).reduce((rpcUrls, chainId) => {
      rpcUrls[chainId] = syncProviders[chainId].connection.url;
      return rpcUrls;
    }, {}),
    // pass through workers config
    numBlockWorkers,
    numTransactionWorkers,
    // should we skip reporting on swap heap dumps and ingestion error logs?
    silent: !printIngestorErrors,
  });
}

// Create a set of listeners to connect a provider to the ingestor
export async function createListeners(
  ingestor: Ingestor,
  controls: {
    inSync: boolean;
    listening: boolean;
    suspended: boolean;
  },
  migrations: Migration[],
  errorHandler: {
    resolved?: boolean;
    reject?: (reason?: any) => void;
    resolve?: (value: void | PromiseLike<void>) => void;
  },
  errorPromise: Promise<void>,
  onError: (e: unknown, close: () => Promise<void>) => Promise<void>
) {
  // retrieve engine
  const engine = await getEngine();

  // we want to attach a set of listeners to the syncProviders to addBlocks to the ingestor
  const attached = await attachListeners(
    ingestor,
    controls,
    migrations,
    errorHandler
  );

  // attach errors and pass to handler to close the connection
  errorPromise.catch(async (e) => {
    // assign error for printing on exit
    engine.error = e;
    // chain error through user handler
    return onError(e, () => exit(ingestor, controls, errorHandler, attached));
  });

  // returning an array of [softClose, hardClose]
  return [
    // soft close (so that we can continue again)
    async () => {
      // await everything halting
      await halt(controls, attached);
      // stop processing on the ingestor but keep the queue intact
      await ingestor.stopProcessing();
      // resolve the error handler without throwing
      errorHandler.resolve();
    },
    async () => {
      // return close to allow external closure
      await exit(ingestor, controls, errorHandler, attached);
    },
  ];
}

// attach listeners (listening for blocks) to enqueue prior to processing
async function attachListeners(
  ingestor: Ingestor,
  controls: {
    inSync: boolean;
    listening?: boolean;
    suspended?: boolean;
  } = {
    inSync: true,
    listening: true,
    suspended: false,
  },
  migrations: Migration[] = [],
  errorHandlers: {
    reject?: (reason?: any) => void;
    resolve?: (value: void | PromiseLike<void>) => void;
  } = undefined
) {
  // retrieve engine
  const engine = await getEngine();

  // get all chainIds for defined networks
  const { syncProviders } = await getNetworks();

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

  // attach a single listener for each provider
  const listeners = await Promise.all(
    Object.keys(syncProviders).map(async (chainId) => {
      // construct a listener to attach to the provider
      const listener = createListener(ingestor, controls, +chainId);
      // construct an error handler to attach to the provider
      const handler = createErrorHandler(errorHandlers);

      // attach this listener to onBlock to start collecting blocks
      syncProviders[+chainId].on("block", listener);
      // attach close to error handler
      syncProviders[+chainId].on("error", handler);

      // return detach method to stop the handler - this will be triggered onError and exit/halt/close
      return async () => {
        // suspend processing on close
        controls.suspended = true;

        // stop listening for new errors and blocks first
        syncProviders[+chainId].off("error", handler);
        syncProviders[+chainId].off("block", listener);
      };
    })
  );

  // retun a handler to remove the listener
  return [
    // all valid listeners
    ...listeners.filter((v) => v),
  ];
}

// Create a listener to attach to the provider
function createListener(
  ingestor: Ingestor,
  controls: {
    inSync: boolean;
    listening?: boolean;
  },
  chainId: number
) {
  // return a closure to keep ref over the listener (for detach)
  return (number: number) => {
    // console.log("\nPushing block:", blockNumber, "on chainId:", chainId);
    if (controls.listening) {
      // push to the block stack to signal the block is retrievable (needs to be fetched and resolved by ingestor)
      ingestor.addBlock({
        number: +number,
        chainId: +chainId,
      });
    }
  };
}

// Construct an error handler to attach to the provider
function createErrorHandler(
  errorHandlers: {
    reject?: (reason?: any) => void;
    resolve?: (value: void | PromiseLike<void>) => void;
  } = undefined
) {
  return (error: Error & { code: string }) => {
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
}

// Construct method to close the connection
async function halt(
  controls: {
    inSync: boolean;
    listening: boolean;
    suspended: boolean;
  },
  attached: (() => Promise<void>)[]
) {
  // retrieve engine
  const engine = await getEngine();
  // notify in stdout that we're closing the connection
  if (!engine.flags.silent && engine.syncing)
    console.log("\nClosing listeners\n\n===\n");
  // close the loops straight away
  controls.listening = false;
  // close the lock
  engine.syncing = false;
  // await removal of listeners and current block
  await Promise.all(attached.map(async (detach) => detach && (await detach())));
}

// Attach a hard close handler to detach and exit
async function exit(
  ingestor: Ingestor,
  controls: {
    inSync: boolean;
    listening: boolean;
    suspended: boolean;
  },
  errorHandler: {
    resolved?: boolean;
    reject?: (reason?: any) => void;
    resolve?: (value: void | PromiseLike<void>) => void;
  },
  attached: (() => Promise<void>)[]
) {
  // retrieve engine
  const engine = await getEngine();
  // halt execution
  await halt(controls, attached);
  // stop processing on the ingestor and clear the queue
  await ingestor.stopProcessing(true);
  // mark error as rejected - we won't use the promise again
  if (!errorHandler.resolved && errorHandler.reject && errorHandler.resolve) {
    // if theres an error pipe through on error handling
    if (engine.error) {
      errorHandler.reject(engine.error);
    } else {
      errorHandler.resolve();
    }
  }
  // give other watchers chance to see this first - but kill the process on error (this gives implementation oppotunity to restart)
  setTimeout(() => process.exit(1));
}

// Restructure ops into ops by chainId
async function getValidSyncOps() {
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
    }, {} as Record<number, Sync[]>);
}
