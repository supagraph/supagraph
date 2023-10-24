// manipulate files in this script
import fs from "fs";

// type to ethers event when appropriate
import { Event } from "ethers";

// Parse and write csv documents (using csv incase we have to move processing into readStreams)
import csvParser from "csv-parser";
import csvWriter from "csv-write-stream";

// types for sync events
import { SyncEvent, SyncStage } from "@/sync/types";
import { toEventData } from "@/utils";

// working directory of the calling project or tmp if in prod
export const cwd =
  process.env.NODE_ENV === "development"
    ? `${process.cwd()}/data/`
    : "/tmp/data-"; // we should use /tmp/ on prod for an ephemeral store during the execution of this process (max 512mb of space on vercel)

// check a file exists
export const exists = async (
  type: string,
  filename: string
): Promise<boolean> => {
  try {
    await fs.promises.access(`${cwd}${type}-${filename}`, fs.constants.F_OK);
    return true;
  } catch (error) {
    if (error.code === "ENOENT") {
      return false; // file does not exist
    }
    throw new Error(`Error checking if the file exists: ${error.message}`);
  }
};

// read the file from disk
export const readJSON = async <T extends Record<string, any>>(
  type: string,
  filename: string,
  ucwd?: string
): Promise<T> => {
  // wrap in a try to return useful erros
  try {
    // read the file async and await its response
    const file = await fs.promises.readFile(
      `${ucwd || cwd}${type}-${filename}.json`,
      {
        encoding: "utf8",
      }
    );
    // parse the file
    if (file && file.length) {
      const data = JSON.parse(file);
      return data as T;
    }
    throw new Error("File is empty or does not exist");
  } catch (err) {
    throw new Error(`Error reading JSON file: ${err.message}`);
  }
};

// save a JSON data blob to disk
export const saveJSON = async (
  type: string,
  filename: string,
  resData: Record<string, unknown>,
  ucwd?: string
): Promise<boolean> => {
  try {
    // ensure the directory exists, creating it if necessary
    await fs.promises.access(ucwd || cwd, fs.constants.F_OK);
  } catch (err) {
    if (err.code === "ENOENT") {
      await fs.promises.mkdir(ucwd || cwd);
    } else {
      throw new Error(`Error accessing directory: ${err.message}`);
    }
  }

  // write the file asynchronously
  await fs.promises.writeFile(
    `${ucwd || cwd}${type}-${filename}.json`,
    JSON.stringify(resData)
  );

  return true;
};

// remove the file from disk
export const deleteJSON = async (
  type: string,
  filename: string,
  ucwd?: string
): Promise<boolean> => {
  try {
    await fs.promises.rm(`${ucwd || cwd}${type}-${filename}.json`);
    return true;
  } catch (error) {
    if (error.code === "ENOENT") {
      // the file doesn't exist, consider it already deleted
      return true;
    }
    throw new Error(`Error deleting JSON file: ${error.message}`);
  }
};

// read a csv file of events
export const readLatestRunCapture = async (
  filePath: string
): Promise<
  {
    type: string;
    data: Event;
    chainId: number;
    number: number;
    blockNumber: number;
    collectBlock: boolean;
    collectTxReceipt: boolean;
    timestamp?: number;
    from?: string;
    txIndex?: number;
    logIndex?: number;
  }[]
> => {
  const results: {
    type: string;
    data: Event;
    chainId: number;
    number: number;
    blockNumber: number;
    collectBlock: boolean;
    collectTxReceipt: boolean;
    timestamp?: number;
    from?: string;
    txIndex?: number;
    logIndex?: number;
  }[] = [];

  return new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(csvParser())
      .on("data", (data) => {
        if (data[0] !== "type") {
          results.push({
            type: data.type,
            // check if we're restoring event data...
            data: data.address
              ? ({
                  blockNumber: parseInt(data.blockNumber, 10),
                  blockHash: data.blockHash,
                  transactionIndex: parseInt(data.transactionIndex, 10),
                  removed: data.removed === "true",
                  address: data.address,
                  data: data.data,
                  topics: data.topics?.split(",") || [],
                  transactionHash: data.transactionHash,
                  logIndex: parseInt(data.logIndex, 10),
                  event: data.event,
                  eventSignature: data.eventSignature,
                  args: data.args?.split(",") || [],
                } as Event)
              : data.data,
            collectBlock: data.collectBlock === "true",
            collectTxReceipt: data.collectTxReceipt === "true",
            chainId: parseInt(data.chainId, 10),
            number: data.number,
            blockNumber: data.blockNumber,
            timestamp: data.blockTimestamp,
            from: data.from,
            txIndex: parseInt(data.txIndex, 10),
            logIndex: parseInt(data.logIndex, 10),
          });
        }
      })
      .on("end", () => {
        resolve(results);
      })
      .on("error", (error) => {
        reject(error);
      });
  });
};

// save events to csv file
export const saveLatestRunCapture = async (
  csvFileName: string,
  events: {
    type: string;
    data: Event | string | number;
    chainId: number;
    number: number;
    blockNumber: number;
    collectBlock: boolean;
    collectTxReceipt: boolean;
    timestamp?: number;
    from?: string;
    txIndex?: number;
    logIndex?: number;
  }[]
) => {
  // createWriteStream to save this run of raw events to a file
  const writableStream = fs.createWriteStream(csvFileName);

  // set up the csv document
  const writer = csvWriter({
    sendHeaders: true,
    headers: [
      "type",
      "collectBlock",
      "collectTxReceipt",
      "blockTimestamp",
      "blockNumber",
      "blockHash",
      "transactionIndex",
      "removed",
      "address",
      "data",
      "topics",
      "transactionHash",
      "txIndex",
      "logIndex",
      "event",
      "eventSignature",
      "args",
      "number",
      "from",
      "chainId",
    ],
  });

  // write to a new document each run
  const savingProcess = new Promise((resolve, reject) => {
    writer
      .pipe(writableStream)
      .on("finish", () => resolve(true))
      .on("error", (error) => reject(error));
  });

  // stream writes to the csv document
  events.forEach(
    (event: {
      type: string;
      data: Event | string;
      chainId: number;
      number: number;
      blockNumber: number;
      collectBlock: boolean;
      collectTxReceipt: boolean;
      timestamp?: number;
      from?: string;
      txIndex?: number;
      logIndex?: number;
    }) => {
      // check txData - we can ignore strings and numbers and count on vals being undf
      const txData = toEventData(event.data);

      // write all values against the headered column
      writer.write({
        type: event.type,
        collectBlock: event.collectBlock,
        collectTxReceipt: event.collectTxReceipt,
        blockTimestamp: event.timestamp,
        blockNumber: txData.blockNumber,
        blockHash: txData.blockHash,
        transactionIndex: txData.transactionIndex,
        removed: txData.removed,
        address: txData.address,
        data: txData.data || event.data,
        topics: txData.topics,
        transactionHash: txData.transactionHash,
        event: txData.event,
        eventSignature: txData.eventSignature,
        args: txData.args,
        txIndex: event.txIndex ?? txData.transactionIndex,
        logIndex: event.logIndex ?? txData.logIndex,
        number: event.number,
        from: event.from,
        chainId: event.chainId,
      });
    }
  );

  // close the stream
  writer.end();

  // wait for the process to complete
  await savingProcess;
};

// process the sorted events via the sync handler callbacks
export const doCleanup = async (
  events: SyncEvent[],
  silent?: boolean,
  start?: keyof typeof SyncStage | false,
  stop?: keyof typeof SyncStage | false
) => {
  // check if we're cleaning up in this sync
  if (
    (!start || SyncStage[start] <= SyncStage.process) &&
    (!stop || SyncStage[stop] >= SyncStage.process)
  ) {
    // log that we're starting
    if (!silent) process.stdout.write(`Cleanup tmp storage `);

    // iterate the events and delete all blocks and txs from this run
    await Promise.all(
      events.map(async (event) => {
        // make sure we've correctly discovered an iface
        if (typeof event.data === "object" && event.data.address) {
          // delete tx from tmp storage
          await deleteJSON(
            "transactions",
            `${event.chainId}-${event.data.transactionHash}`
          );
          // delete block from tmp storage
          await deleteJSON(
            "blocks",
            `${event.chainId}-${parseInt(`${event.data.blockNumber}`).toString(
              10
            )}`
          );
        } else if (event.type === "onBlock") {
          // delete block from tmp storage
          await deleteJSON(
            "blocks",
            `${event.chainId}-${parseInt(`${event.data}`).toString(10)}`
          );
        } else if (event.type === "onTransaction") {
          // delete tx from tmp storage
          await deleteJSON("transactions", `${event.chainId}-${event.data}`);
          // delete block from tmp storage
          await deleteJSON(
            "blocks",
            `${event.chainId}-${parseInt(`${event.blockNumber}`).toString(10)}`
          );
        }
      })
    ).catch(() => {
      // attempt the cleanup again - if everything is clean we should return true from deleteJSON
      doCleanup(events, silent, start, stop);
    });
  }

  // mark as complete in stdout
  if (!silent) process.stdout.write("✔\n");
};
