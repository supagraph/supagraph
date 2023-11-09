// import cronParser to check when the last event should have been constructed for a block
import cronParser from "cron-parser";

// get the engine and updateSyncsOpsMeta incase scheduled events modifies sync operations
import { getEngine, updateSyncsOpsMeta } from "./persistence";

// scheduled events might use the promise queue to process data (unlikely and unnecessary)
import { processGlobalPromiseQueue } from "./promises";

// check the blockTimestamp against the schedule
export const checkSchedule = async (blockTimestamp: number) => {
  // grab engine for global state access
  const engine = await getEngine();

  // pull the schedule we're working with
  const schedule = engine.cronSchedule || [];

  // run through each expression in the schedule
  for (const cronExpression of schedule) {
    // wrap the attempt incase of bad expressions
    try {
      // set the interval according to the expression (use utc as basis for timebound ops)
      const interval = cronParser.parseExpression(cronExpression.expr, {
        currentDate: new Date(),
        utc: true,
      });
      // next execution according to interval
      const prevExecutionTime = Math.floor(interval.prev().getTime() / 1000);
      // default next run if absent
      if (!cronExpression.lastRun) {
        cronExpression.lastRun = prevExecutionTime;
      }
      // check if the handler should run against this block (only tick over when the block reports the timestamp in the past)
      if (
        +blockTimestamp >= prevExecutionTime &&
        prevExecutionTime > cronExpression.lastRun
      ) {
        // record length of promise queue before running scheduled update
        const promiseQueueLength = engine.promiseQueue.length;

        // attempt the update...
        try {
          // print that we're processing the scheduled event
          if (!engine.flags.silent)
            process.stdout.write(
              `\n--\n\nRunning scheduled function ${
                cronExpression.expr
              } @ ${new Date(
                +blockTimestamp * 1000
              ).toUTCString()}\n\nFunction processed `
            );

          // open a checkpoint (this will need commiting if we dont throw)
          engine.stage.checkpoint();

          // attempt the scheduled handler
          try {
            // run expression according to schedule (blocking)
            await cronExpression.handler();
          } catch (e) {
            // print error but don't stop
            console.error(e);
          }

          // await all promises that have been enqueued during execution of callbacks (this will be cleared afterwards ready for the next run)
          await processGlobalPromiseQueue(promiseQueueLength);

          // mark after we end the processing
          if (!engine.flags.silent) {
            process.stdout.write(`✔\nEntities stored `);
          }

          // update with any new syncOps added in the sync
          if (engine.handlers && engine.syncOps.meta) {
            // record the new syncs to db (this will replace the current entry)
            engine.syncOps.meta = await updateSyncsOpsMeta(
              engine.syncOps.meta,
              engine.syncs
            );
          }

          // make sure we perform all checkpoint updates in this call
          while (engine.stage.isCheckpoint) {
            // commit changes on stage
            await engine.stage.commit();
          }

          // update last run to prevent future runs
          cronExpression.lastRun = prevExecutionTime;

          // after all events are stored in db
          if (!engine.flags.silent) process.stdout.write("✔\n");
        } catch (e) {
          // print any errors from processing the schedule
          if (!engine.flags.silent) console.log(e);
          // revert changes
          engine.stage.revert();
        }
      }
    } catch (e) {
      // revert the checkpoint
      console.log("Scheduling error:", e);
    }
  }
};
