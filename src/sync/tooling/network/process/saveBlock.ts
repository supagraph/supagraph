import {
  JsonRpcProvider,
  TransactionResponse,
  WebSocketProvider,
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
  provider: JsonRpcProvider | WebSocketProvider,
  tmpDir?: string,
  cleanup?: any
) => {
  // this promise.all is trapped until we resolve all tx receipts in the block
  try {
    // get the receipt
    const fullTx = await getTransactionReceipt(provider, tx);

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

    // return the tx
    return fullTx;
  } catch {
    // attempt to get receipt again on failure
    return getReceipt(tx, chainId, provider, tmpDir, cleanup);
  }
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
  return new Promise<boolean>((outerResolve, outerReject) => {
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
      getBlockByNumber(provider, number).then(async (block) => {
        // if any of the receipt collects fails then we can fail the whole op
        const receipts = (
          (await Promise.all(
            block.transactions.map(
              async (tx) =>
                tx &&
                (await getReceipt(tx, +chainId, provider, tmpDir, cleanup))
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
      });
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
  });
};

// check that this is the main import script before processing the components
if (require.main === module) {
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

    // save the block and receipt and exit this process
    saveListenerBlockAndReceipts(block, chainId, providerUrl, tmpDir, cleanup)
      .then(() => {
        process.exit(0);
      })
      .catch((error) => {
        console.error("Error:", error);
        process.exit(1);
      });
  } catch {
    process.exit(1);
  }
}
