/* eslint-disable import/no-import-module-exports */
import {
  JsonRpcProvider,
  TransactionReceipt,
  TransactionResponse,
} from "@ethersproject/providers";
import { ConnectionInfo } from "ethers/lib/utils";

// these are typescript...
import { getBlockByNumber } from "../blocks";
import { getTransactionReceipt } from "../transactions";
import { saveJSON } from "../../persistence/disk";

// get a receipt for the given details
const getReceipt = async (
  tx: string | TransactionResponse,
  chainId: number,
  rpcUrl: string | ConnectionInfo,
  tmpDir?: string,
  cleanup?: any
) => {
  return new Promise<TransactionReceipt>(
    getReceiptPromise(tx, chainId, rpcUrl, tmpDir, cleanup)
  );
};

// promise internals to get a receipt
const getReceiptPromise =
  (
    tx: string | TransactionResponse,
    chainId: number,
    rpcUrl: string | ConnectionInfo,
    tmpDir?: string,
    cleanup?: any
  ) =>
  (
    resolve: (
      value: TransactionReceipt | PromiseLike<TransactionReceipt>
    ) => void,
    reject: (reason?: any) => void
  ) => {
    // timeout after x seconds
    const timeout = setTimeout(() => {
      reject(
        new Error(`Error: timeout ${typeof tx === "string" ? tx : tx.hash}`)
      );
    }, 10000); // 10seconds

    // get a new provider with the given rpcUrl
    const provider = new JsonRpcProvider(rpcUrl);

    // get the receipt
    getTransactionReceipt(provider, tx)
      .then(saveReceipt(timeout, resolve, chainId, tmpDir, cleanup))
      .catch(rejectReceipt(timeout, reject));
  };

// reject receipt handling
const rejectReceipt =
  (timeout: NodeJS.Timeout, reject: (reason?: any) => void) => (e: any) => {
    clearTimeout(timeout);
    reject(e);
  };

// save the receipt and close the resolve
const saveReceipt =
  (
    timeout: NodeJS.Timeout,
    resolve: (value: TransactionReceipt) => void,
    chainId: number,
    tmpDir: string,
    cleanup: any
  ) =>
  async (fullTx: TransactionReceipt) => {
    // try again
    if (!fullTx.transactionHash) throw new Error("Missing hash");
    // if we're tmp storing data...
    if (!cleanup) {
      // save each tx to disk to release from mem
      await saveJSON(
        "transactions",
        `${chainId}-${fullTx.transactionHash}`,
        fullTx as unknown as Record<string, unknown>,
        tmpDir
      );
    }
    // clear the error throwing timeout
    clearTimeout(timeout);
    // return the tx
    resolve(fullTx);
  };

// pull all block and receipt details (cancel attempt after 30s) (race and retry inside here?)
export const saveListenerBlockAndReceipts = async (
  number: number,
  chainId: number,
  rpcUrl: string | ConnectionInfo,
  tmpDir?: string,
  cleanup?: boolean
) => {
  // return a promise to resolve the block & transactions and save them to disk
  return new Promise<boolean>(
    saveListenerBlockAndReceiptsPromise(
      number,
      chainId,
      rpcUrl,
      tmpDir,
      cleanup
    )
  );
};

// closure to return the promise method
const saveListenerBlockAndReceiptsPromise =
  (
    number: number,
    chainId: number,
    rpcUrl: string | ConnectionInfo,
    tmpDir?: string,
    cleanup?: boolean
  ) =>
  (
    outerResolve: (value: boolean | PromiseLike<boolean>) => void,
    outerReject: (reason?: any) => void
  ) => {
    // store ref to the timeout
    let timeout: NodeJS.Timeout;

    // get a new provider with the given rpcUrl
    const provider = new JsonRpcProvider(rpcUrl);

    // return a promise to resolve success/failure
    const promise = new Promise<boolean>((resolve, reject) => {
      // timeout after x seconds
      timeout = setTimeout(() => {
        reject(new Error(`Error: timeout ${chainId}-${+number}`));
      }, 30000); // 30seconds

      // get the full block details
      getBlockByNumber(provider, number)
        .then(async (block) => {
          // if any of the receipt collects fails then we can fail the whole op
          const receipts = (
            (await Promise.all(
              block.transactions.map(
                async (tx) =>
                  tx &&
                  (await getReceipt(tx, +chainId, rpcUrl, tmpDir, cleanup))
              )
            ).catch(reject)) || []
          ).reduce((all, receipt) => {
            // combine all receipts to create an indexed lookup obj
            return !receipt?.transactionHash
              ? all
              : {
                  ...all,
                  [receipt.transactionHash]: receipt,
                };
          }, {});

          // if we're tmp storing data...
          if (!cleanup) {
            // save the block for sync-cache
            await saveJSON(
              "blocks",
              `${chainId}-${+block.number}`,
              {
                block,
              },
              tmpDir
            ).catch(reject);
          }

          // save everything together to reduce readback i/o (if we're using cleanup true - this is all we will save - we will delete it after processing)
          await saveJSON(
            "blockAndReceipts",
            `${chainId}-${+block.number}`,
            {
              block,
              receipts,
            },
            tmpDir
          ).catch(reject);

          // saved - return true to outer
          resolve(true);
        })
        .catch(reject);
    });

    // return promise to outer (timesout after 30s)
    promise
      .then((v) => {
        // clear the timeout once processing is done
        clearTimeout(timeout);

        // carry the resolution value to parent
        outerResolve(v);
      })
      .catch((e) => {
        // clear the timeout once processing is done
        clearTimeout(timeout);

        // carry the error to parent
        outerReject(e);
      });
  };

// check that this is the main import script before processing the components
if (require.main.filename === module.filename) {
  // attempt the operation
  try {
    // Check if the correct number of command-line arguments are provided
    if (process.argv.length < 5) {
      console.log(process.argv);
      console.error(
        "Usage: node saveBlock.js <block> <chainId> <providerUrl> <tmpDir?> <cleanup?>"
      );
      process.exit(1);
    }

    // Parse provider args
    const block = +process.argv[2];
    const chainId = +process.argv[3];
    const providerUrl = process.argv[4];
    // this is optional...
    const tmpDir = process.argv[5];
    const cleanup = process.argv[6] === "true";

    // timeout here to make sure this closes
    const timeout = setTimeout(() => {
      process.exit(1);
    }, 30000);

    // save the block and receipt and exit this process
    saveListenerBlockAndReceipts(block, chainId, providerUrl, tmpDir, cleanup)
      .then(() => {
        clearTimeout(timeout);
        process.exit(0);
      })
      .catch((error) => {
        clearTimeout(timeout);
        console.error("Error:", error);
        process.exit(1);
      });
  } catch {
    process.exit(1);
  }
}
