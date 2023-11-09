import { JsonRpcProvider, WebSocketProvider } from "@ethersproject/providers";

import { getEngine } from "@/sync/tooling/persistence/store";
import { readJSON, saveJSON } from "@/sync/tooling/persistence/disk";

import { processPromiseQueue } from "@/sync/tooling/promises";
import { getBlockByNumber } from "./fetch";

// slice the range according to the provided limit
export const createBlockRanges = (
  fromBlock: number,
  toBlock: number,
  limit: number
): number[][] => {
  // each current is a tuple containing from and to
  let currentRange: number[] = [fromBlock];
  // we collect the tuples into an array
  const blockRanges: number[][] = [];

  // bracket the from-to to only include the limit number of blocks
  for (let i = fromBlock + 1; i <= toBlock; i += 1) {
    // if we step over the limit push a new entry
    if (i - currentRange[0] >= limit) {
      // -1 so we don't request the same block twice
      currentRange.push(i - 1);
      blockRanges.push(currentRange);
      // start the next range at boundary
      currentRange = [i];
    }
  }

  // push the final toBlock to the current range
  currentRange.push(toBlock);
  // record the final current range
  blockRanges.push(currentRange);

  // return all ranges
  return blockRanges;
};

// save to disk for future use
export const fetchAndSaveBlock = async (
  provider: JsonRpcProvider | WebSocketProvider,
  chainId: number,
  number: number,
  result: Set<number>
) => {
  // fetch the block and transactions via the provider (we dont need raw details ie logBlooms here)
  const block = await getBlockByNumber(provider, number);
  // if we have a block
  if (block?.number) {
    // save the block to disk to release from mem
    await saveJSON(
      "blocks",
      `${chainId}-${parseInt(`${block.number}`).toString(10)}`,
      block as unknown as Record<string, unknown>
    );
    // add the block to the result
    result.add(+parseInt(`${block.number}`).toString(10));
  } else {
    // trigger error handler and retry...
    throw new Error("No block response");
  }

  // return the block
  return block;
};

// pull all blocks in the requested range
export const blocksFromRange = async (
  chainId: number,
  provider: JsonRpcProvider | WebSocketProvider,
  from: number,
  to: number,
  // everything we place in the reqStack will eventually resolve
  reqStack: (() => Promise<any>)[],
  // this call will fill this result set and save everything into tmp storage to avoid filling memory
  result: Set<number>,
  silent: boolean
) => {
  // iterate the ranges and collect all blocks in that range
  while (+from <= +to) {
    await new Promise<unknown>((resolve, reject) => {
      readJSON("blocks", `${chainId}-${+from}`)
        .then(async (file) => {
          if (file) {
            // store hash for result
            result.add(+from);
            // successfully loaded the result
            resolve(true);
          } else {
            // do the actual fetch
            await fetchAndSaveBlock(provider, chainId, +from, result)
              .then(() => {
                // finished with promise
                resolve(true);
              })
              .catch((e) => {
                reject(e);
              });
          }
        })
        .catch(async (err) => {
          reject(err);
        });
    }).catch(
      (function retry(attempts) {
        // logging if we keep attempting and we're not getting anything...
        if (!silent && attempts % 10 === 0) {
          console.log(
            `Made ${attempts} attempts to get: ${chainId}::block::${+from}`
          );
        }

        // return the error handler (recursive stacking against the reqStack)
        return (): void => {
          reqStack.push(async () =>
            fetchAndSaveBlock(provider, chainId, +from, result)
              .then(() => {
                // resolves to true when we get the block
                return true;
              })
              .catch(retry(attempts + 1))
          );
        };
      })(1) // start attempts at 1
    );
    // move from for next tick
    from = +from + 1;
  }
};

// wrap the blocks response to set and to cast type
export const wrapBlockRes = async (
  entries: number[],
  provider: JsonRpcProvider | WebSocketProvider,
  toBlock: number
) => {
  return Promise.all(
    entries.map(async (entry) => {
      return {
        type: "onBlock",
        data: entry,
        number: toBlock,
        blockNumber: entry,
        chainId: provider.network.chainId,
        collectTxReceipt: false,
        collectBlock: true,
        txIndex: 999999999999999,
        logIndex: 999999999999999,
      };
    })
  );
};

// pull new events from the contract
export const getNewBlocks = async (
  fromBlock: number,
  toBlock: number,
  provider: JsonRpcProvider | WebSocketProvider,
  silent: boolean
) => {
  // fetch the engine
  const engine = await getEngine();
  // collect a list of block hashes then wrap the response to build a single array of happenings to process
  const result = new Set<number>();
  // get the chainId
  const { chainId } = provider.network;
  // create a new eventRange for each 500,000 blocks (to process in parallel)
  const ranges = createBlockRanges(fromBlock, toBlock, 10);
  // construct a reqStack so that we're only processing x reqs at a time
  const stack: ((reqStack: (() => Promise<any>)[]) => Promise<void>)[] = [];

  // iterate the ranges and collect all events in that range
  for (const [from, to] of ranges) {
    stack.push(async (reqStack) => {
      await blocksFromRange(
        chainId,
        provider,
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

  // wrap the events with the known type
  return wrapBlockRes(Array.from(result), provider, toBlock);
};
