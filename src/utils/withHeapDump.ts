// Using v8 to measure memory usage between swaps
import * as v8 from "v8";

// Print a heap dump if gc is enabled for manual management
export function withHeapDump(message: string, silent: boolean = false) {
  // check if enable and if global.gc is present (start node with --expose-gc to enable)
  if (!silent && global.gc) {
    // run garbage collection
    global.gc();
    // gather stats
    const heapStats = v8.getHeapStatistics();
    const heapStatsMB = heapStats;
    // eslint-disable-next-line guard-for-in
    for (const key in heapStatsMB) {
      heapStatsMB[key] = `${(
        ((heapStatsMB[key] / 1024 / 1024) * 100) /
        100
      ).toFixed(2)} MB`;
    }
    // pretty-print the dumps stats
    console.log("");
    console.log(message);
    console.table(heapStatsMB);
    console.log("");
  }
}
