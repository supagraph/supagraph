import {
  Block,
  BlockWithTransactions,
  TransactionResponse,
} from "@ethersproject/abstract-provider";
import { JsonRpcProvider, WebSocketProvider } from "@ethersproject/providers";

import { getEngine } from "@/sync/tooling/persistence/store";
import { readJSON, saveJSON } from "@/sync/tooling/persistence/disk";
import { processPromiseQueue } from "@/sync/tooling/promises";

import {
  createBlockRanges,
  fetchAndSaveBlock,
} from "@/sync/tooling/network/blocks";

import { getTransactionReceipt } from "@/sync/tooling/network/fetch";

// fetch and save all transactions in a block
export const fetchAndSaveTransactions = async (
  provider: JsonRpcProvider | WebSocketProvider,
  block: Block | undefined,
  chainId: number,
  number: number,
  result: Set<{
    hash: string;
    txIndex: number;
    block: number;
  }>,
  collectTxReceipts: boolean = false
) => {
  // fetch and save the block then the transactions (optionally pulling receipts)
  await (block && block.number
    ? // resolve the block we were provided
      Promise.resolve(block)
    : // attempt to read from disk and fallback to fetch
      new Promise((resolve) => {
        // attempt to read from disk
        try {
          resolve(
            readJSON<Block>(
              "blocks",
              `${chainId}-${parseInt(`${number}`).toString(10)}`
            ).then((data) => {
              // if the data is incomplete then throw to trigger fetch
              if (!data.number) throw new Error("Missing block data");
            })
          );
        } catch {
          // fallback to a fetch
          resolve(fetchAndSaveBlock(provider, chainId, number, new Set()));
        }
      })
  ).then(async (_block: Block | BlockWithTransactions) => {
    // save the transactions
    await Promise.all(
      (_block.transactions || []).map(
        async (tx: string | TransactionResponse, txIndex: number) => {
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

          // add the fetched block to the result
          result.add({
            // index according to position in the block
            txIndex,
            // allow for the tx to be passed as its hash (we're currently requesting all txResponses from the block so it will never be a string atm)
            hash: typeof tx === "string" ? tx : tx.hash,
            block: +parseInt(`${_block.number}`).toString(10),
          });

          // resolves to true;
          return true;
        }
      )
    );
  });
};

// pull all transactions in the requested range
export const txsFromRange = async (
  chainId: number,
  provider: JsonRpcProvider | WebSocketProvider,
  from: number,
  to: number,
  collectTxReceipts: boolean,
  // everything we place in the reqStack will eventually resolve
  reqStack: (() => Promise<any>)[],
  // this call will fill this result set and save everything into tmp storage to avoid filling memory
  result: Set<{
    hash: string;
    txIndex: number;
    block: number;
  }>,
  silent?: boolean
) => {
  // iterate the ranges and collect all blocks in that range
  while (from <= to) {
    // wait for each item to complete before fetching the next one
    await new Promise<unknown>((resolve, reject) => {
      // attempt to read the file from disk
      readJSON<Block>(
        "blocks",
        `${chainId}-${parseInt(`${from}`).toString(10)}`
      )
        .then(async (data) => {
          if (data) {
            // console.log(file,`${cwd}blocks-${chainId}-${parseInt(`${from}`).toString(10)}.json` )
            // check data holds transactions
            if (data.number && data.transactions) {
              // bail out and do the full fetch if we want receipts
              if (!collectTxReceipts) {
                // add all tx hashes to the result
                await Promise.all(
                  data.transactions.map(
                    async (
                      tx: string | TransactionResponse,
                      txIndex: number
                    ) => {
                      const hash = typeof tx === "string" ? tx : tx.hash;
                      result.add({
                        hash,
                        txIndex,
                        block: from,
                      });
                      // save each tx to disk to release from mem
                      await saveJSON(
                        "transactions",
                        `${chainId}-${hash}`,
                        tx as unknown as Record<string, unknown>
                      );
                    }
                  )
                );
                // complete the promise
                resolve(true);
              } else {
                // fetch and save using the block to read tx responses
                await fetchAndSaveTransactions(
                  provider,
                  data,
                  chainId,
                  from,
                  result
                )
                  .then(() => {
                    // complete the promise
                    resolve(true);
                  })
                  .catch((e) => {
                    reject(new Error(e));
                  });
              }
            } else {
              // fetch transactions from the provided block
              reject(new Error("Missing transactions"));
            }
          } else {
            // fetch block
            reject(new Error("Missing block"));
          }
        })
        .catch(async (err) => {
          // error reading file - fetch block
          reject(err);
        });
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
          // we'll push a new req to the stack and close this promise to release
          reqStack.push(() =>
            fetchAndSaveTransactions(
              provider,
              // check the block again each attempt, we don't want to waste fetch cycles or halt execution with a missing block
              undefined,
              chainId,
              from,
              result
            ).catch(retry(attempts + 1))
          );
        };
      })(1)
    );
    // move from for next tick
    from += 1;
  }
};

// wrap the transactions response to set and to cast type
export const wrapTxRes = async (
  entries: {
    hash: string;
    txIndex: number;
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
        // copy txIndex onto the instance for pre-data collect sort order
        txIndex: entry.txIndex,
        // process after all other events for the given tx
        logIndex: 999999999999999,
      };
    })
  );
};

// pull new events from the contract
export const getNewTransactions = async (
  fromBlock: number,
  toBlock: number,
  provider: JsonRpcProvider | WebSocketProvider,
  collectTxReceipts: boolean = false,
  silent: boolean = false
) => {
  // retrieve the engine
  const engine = await getEngine();
  // collect a list of events then map the event type to each so that we can rebuild all events into a single array later
  const result = new Set<{
    hash: string;
    txIndex: number;
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
  await processPromiseQueue(stack, engine.concurrency, true);

  // wrap the events with the known type
  return wrapTxRes(Array.from(result), provider, toBlock);
};
