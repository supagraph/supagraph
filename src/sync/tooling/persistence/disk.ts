// manipulate files in this script
import fs from "fs";

// type to ethers event when appropriate
import { Event } from "ethers";

// Parse and write csv documents (using csv incase we have to move processing into readStreams)
import csvParser from "csv-parser";
import csvWriter from "csv-write-stream";

// types for sync events
import { SyncEvent, SyncStage } from "@/sync/types";

// working directory of the calling project or tmp if in prod
export const cwd =
  process.env.NODE_ENV === "development"
    ? `${process.cwd()}/data/`
    : "/tmp/data-"; // we should use /tmp/ on prod for an ephemeral store during the execution of this process (max 512mb of space)

// check a file exists
export const exists = (type: string, filename: string) => {
  const fileExists = fs.existsSync(`${cwd}${type}-${filename}`);
  return fileExists || false;
};

// read the file from disk
export const readJSON = async <T extends Record<string, any>>(
  type: string,
  filename: string
): Promise<T> => {
  return new Promise((resolve, reject) => {
    fs.readFile(`${cwd}${type}-${filename}.json`, "utf8", async (err, file) => {
      if (!err && file && file.length) {
        try {
          const data = JSON.parse(file);
          resolve(data as T);
        } catch (e) {
          reject(err);
        }
      } else {
        reject(err);
      }
    });
  });
};

// save a JSON data blob to disk
export const saveJSON = async (
  type: string,
  filename: string,
  resData: Record<string, unknown>
): Promise<boolean> => {
  return new Promise((resolve, reject) => {
    // create data dir if missing
    try {
      fs.accessSync(`${cwd}`);
    } catch {
      fs.mkdirSync(`${cwd}`);
    }
    // write the file
    fs.writeFile(
      `${cwd}${type}-${filename}.json`,
      JSON.stringify(resData),
      "utf8",
      async (err) => {
        if (!err) {
          resolve(true);
        } else {
          reject(err);
        }
      }
    );
  });
};

// remove the file from disk
export const deleteJSON = async (
  type: string,
  filename: string
): Promise<boolean> => {
  return new Promise((resolve) => {
    try {
      // delete the file
      fs.rm(`${cwd}${type}-${filename}.json`, async () => resolve(true));
    } catch {
      // already deleted
      resolve(true);
    }
  });
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
  }[] = [];

  return new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(csvParser())
      .on("data", (data) => {
        if (data[0] !== "type") {
          results.push({
            type: data.type,
            data: {
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
            } as Event,
            collectBlock: data.collectBlock === "true",
            collectTxReceipt: data.collectTxReceipt === "true",
            chainId: parseInt(data.chainId, 10),
            number: data.number,
            blockNumber: data.blockNumber,
            timestamp: data.blockTimestamp,
            from: data.from,
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
    data: Event | string;
    chainId: number;
    number: number;
    blockNumber: number;
    collectBlock: boolean;
    collectTxReceipt: boolean;
    timestamp?: number;
    from?: string;
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
  const savingProcess = new Promise((resolve) => {
    writer.pipe(writableStream).on("finish", () => resolve(true));
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
    }) => {
      const txData = typeof event.data === "string" ? ({} as any) : event.data;
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
        logIndex: txData.logIndex,
        event: txData.event,
        eventSignature: txData.eventSignature,
        args: txData.args,
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
    );
  }

  // mark as completed in stdout
  if (!silent) process.stdout.write("✔\n");
};
