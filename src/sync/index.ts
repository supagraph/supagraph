// Expose db to handlers
export { DB } from "./db";
export { Mongo } from "./mongo";

// Use multicall pieces
export {
  MULTICALL_ABI,
  getMulticallContract,
  callMulticallContract,
} from "./multicall";

// Expose level-db entity store to handlers via engine
export { Entity, Store, getEngine, setEngine } from "./store";

// Add syncs and execute them
export {
  sync,
  setSyncs,
  addSync,
  enqueuePromise,
  processPromiseQueue,
} from "./toolkit";

// Export all other shared types
export type {
  Engine,
  Handlers,
  HandlerFn,
  HandlerFns,
  Migration,
  LatestEntity,
  Sync,
  SyncOp,
  SyncConfig,
  SyncResponse,
  SyncEvent,
  SyncStage,
  // @Deprecated - removing in next release
  SyncStage as Stage,
} from "./types";
