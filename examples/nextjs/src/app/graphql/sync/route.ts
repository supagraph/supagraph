// Use addSync to add operations and sync to process them all in block/tx order
import { NextRequest, NextResponse } from "next/server";

// Import the sync command and db drivers to setup engine
import {
  DB,
  Mongo,
  Stage,
  SyncConfig,
  setEngine,
  setSyncs,
  sync,
} from "supagraph";

// Import mongodb client
import { getMongodb } from "@providers/mongoClient";

// Import all mappings to be registered
import { handlers } from "@supagraph/handlers";

// Import revalidation timings from config
import config from "@supagraph/config";

// forces the route handler to be dynamic
export const dynamic = "force-dynamic";

// Switch out the engine for development to avoid the mongo requirment locally
setEngine({
  // name the connection
  name: config.name,
  // db is dependent on state
  db:
    // in production/production like environments we want to store mutations to mongo otherwise we can store them locally
    !process.env.MONGODB_URI ||
    (process.env.NODE_ENV === "development" && config.dev)
      ? // connect store to in-memory/node-persist store
        DB.create({
          kv: {},
          name: config.name,
          reset: (config as unknown as SyncConfig)?.reset,
        })
      : // connect store to MongoDB
        Mongo.create({
          kv: {},
          name: config.name,
          mutable: config.mutable,
          client: getMongodb(process.env.MONGODB_URI!),
        }),
});

// set the sync ops
setSyncs(config as unknown as SyncConfig, handlers);

// Expose the sync command on a route so that we can call it with a cron job
export async function GET(request: NextRequest) {
  // set the start stage of the sync ("events", "blocks", "transactions", "sort", "process")
  const start =
    (request.nextUrl.searchParams.get("start") as keyof typeof Stage) || false;
  // set the stop stage of the sync ("events", "blocks", "transactions", "sort", "process")
  const stop =
    (request.nextUrl.searchParams.get("stop") as keyof typeof Stage) || false;

  // all new events discovered from all sync operations detailed in a summary
  const summary = await sync({
    // pass through config options
    listen: (config as unknown as SyncConfig).listen ?? false,
    cleanup: (config as unknown as SyncConfig).cleanup ?? true,
    // where should we start and stop this run? (allowing for staggered runs to build up the cache before executing the final step (process))
    start,
    stop,
    // include extra steps to get block details for the logs (we need to be able to calculate the gasCost and store the block.timestamp)
    collectBlocks: true, // collect blocks
    collectTxReceipts: true, // collect txs
  });

  // we don't need to sync more often than once per block - and if we're using vercel.json crons we can only sync 1/min
  return NextResponse.json(summary, {
    headers: {
      // allow to be cached for revalidate seconds and allow caching in shared public cache (upto revalidate seconds)
      "Cache-Control": `max-age=${config.revalidate}, public, s-maxage=${config.revalidate}`,
    },
  });
}
