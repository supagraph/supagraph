import {
  Block,
  TransactionReceipt,
  TransactionResponse,
} from "@ethersproject/abstract-provider";
import { JsonRpcProvider, WebSocketProvider } from "@ethersproject/providers";

import { getEngine } from "@/sync/tooling/persistence/store";
import { readJSON, saveJSON } from "@/sync/tooling/persistence/disk";
import { processPromiseQueue } from "@/sync/tooling/promises";

import { createBlockRanges } from "@/sync/tooling/network/blocks";

// get a full transaction receipt from hash
export const getTransactionReceipt = async (
  provider: JsonRpcProvider | WebSocketProvider,
  txHash: string | TransactionResponse,
  maxRetries: number = 3,
  retryDelay: number = 1000
): Promise<TransactionReceipt> => {
  let retries = 0;
  let txReceipt: TransactionReceipt;
  // keep attempting upto retry limit
  while (retries < maxRetries) {
    try {
      const tx = (txHash as TransactionResponse)?.hash
        ? txHash
        : await provider.send("eth_getTransactionByHash", [txHash]);
      const receipt = await provider.send("eth_getTransactionReceipt", [
        tx.hash,
      ]);

      // if we discovered the block record it
      if (receipt && receipt.transactionHash && tx.hash) {
        // set for returning
        txReceipt = {
          ...tx,
          ...receipt,
        };
        // break the while loop on success
        break;
      } else {
        // throw to retry/end
        throw new Error("No transaction response");
      }
    } catch (error) {
      // catch any errors
      retries += 1;

      // timeout with a delay
      if (retries < maxRetries) {
        await new Promise((resolve) => {
          setTimeout(resolve, retryDelay);
        });
      } else {
        // handle when maximum retries are reached, e.g., log an error or throw an exception.
        throw error;
      }
    }
  }

  // return the tx
  return txReceipt;
};

// pull all transactions in the requested range
export const txsFromRange = async (
  chainId: number,
  provider: JsonRpcProvider | WebSocketProvider,
  from: number,
  to: number,
  collectTxReceipts: boolean,
  reqStack: (() => Promise<any>)[],
  result: Set<{
    hash: string;
    txIndex: number;
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
        block.transactions.map(
          async (tx: string | TransactionResponse, txIndex: number) => {
            // add the fetched block to the result
            result.add({
              // index according to position in the block
              txIndex,
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
          }
        )
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

      // attempt to read the file from disk
      readJSON<Block>(
        "blocks",
        `${chainId}-${parseInt(`${from}`).toString(10)}`
      )
        .then(async (data) => {
          if (data) {
            // console.log(file,`${cwd}blocks-${chainId}-${parseInt(`${from}`).toString(10)}.json` )
            try {
              // check data holds transactions
              if (
                data.transactions &&
                data.transactions.length &&
                // bail out and do the full fetch if we want receipts
                !collectTxReceipts
              ) {
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
                // fetch transactions from the provided block
                await doFetch(data);
              }
            } catch {
              // fecth block and transactions from current from
              await doFetch();
            }
          }
        })
        .catch(async (err) => {
          if (err) {
            // fecth block and transactions from current from
            await doFetch();
          }
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
