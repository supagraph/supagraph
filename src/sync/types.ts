// Import required types from ethers
import type {
  Block,
  BlockWithTransactions,
  TransactionReceipt,
  TransactionResponse,
} from "@ethersproject/abstract-provider";
import type {
  JsonRpcProvider,
  WebSocketProvider,
} from "@ethersproject/providers";
import type { ethers, providers } from "ethers";

// import persistence layer types
import type { DB, Stage, Entity } from "@/sync/tooling/persistence";

// This should probably just be string - Record<string, string | number | Buffer> (or anything else which is valid in a mongo setting)
export type KV = Record<string, Record<string, Record<string, unknown> | null>>;

// Allow for stages to be skipped via config
export enum SyncStage {
  events = 1,
  blocks,
  transactions,
  sort,
  process,
}

// Defines the properties available in the global engine
export type Engine = {
  name?: string;
  db?: DB;
  stage?: Stage;
  syncing?: boolean;
  error?: unknown;
  chainId?: number;
  block?: Record<number, Block>;
  newDb?: boolean;
  warmDb?: boolean;
  readOnly?: boolean;
  concurrency?: number;
  lastUpdate?: number;
  syncs?: Sync[];
  syncOps?: {
    syncs?: Sync[];
    meta?: Entity<{
      id: string;
      syncOps: SyncOp[];
    }> & {
      id: string;
      syncOps: SyncOp[];
    };
  };
  promiseQueue?: (
    | Promise<unknown>
    | ((stack?: (() => Promise<any>)[]) => Promise<unknown>)
  )[];
  providers?: Record<number, Record<number, providers.JsonRpcProvider>>;
  extraProviders?: string[];
  eventAbis?: Record<string, ethers.Contract["abi"]>;
  eventIfaces?: Record<string, ethers.utils.Interface>;
  startBlocks?: Record<number, number>;
  latestEntity?: LatestEntity;
  latestBlocks?: Record<number, Block>;
  callbacks?: Record<string, HandlerFn>;
  opSyncs?: Record<string, Sync>;
  handlers?: Handlers;
  events?: SyncEvent[];
  // flags to change runtime behavior
  flags?: {
    start?: keyof typeof SyncStage | false;
    stop?: keyof typeof SyncStage | false;
    collectBlocks?: boolean;
    collectTxReceipts?: boolean;
    listen?: boolean;
    cleanup?: boolean;
    silent?: boolean;
  };
  currentProcess?: Promise<void>;
  close?: (() => Promise<void>) | undefined;
  appendEvents?: (events: SyncEvent[], silent?: boolean) => Promise<void>;
};

// Defines a migration handler
export type Migration = {
  chainId: number;
  blockNumber: number | "latest";
  // collect all items for the entity
  entity?: string;
  // run the handler against every entity
  handler: (
    blockNumber?: number,
    chainId?: number,
    entity?: Entity<{ id: string }>
  ) => Promise<void> | void;
};

// describes a batch operation
export type BatchOp = {
  type: "put" | "del";
  key: string;
  value?: Record<string, unknown>;
};

// Define recordable syncOp type
export type SyncOp = {
  // name of the deployed contract (or chainId for network listeners)
  name?: string | number;
  // all ops will have a chainId
  chainId: number;
  // for network listeners we only need start and endblock
  startBlock: number | "latest";
  endBlock: number | "latest";
  // mark as added by runtime
  mode?: string;
  // Establish all event signatures available on this contract (we could also accept a .sol or .json file here)
  events?: string | string[];
  // the event this op relates to
  eventName?: string;
  // use handlers registered against "token" named group in handlers/index.ts
  handlers?: string;
  // use address for filter events
  address?: string | undefined;
  // allow for blocks and txReceipts to be retrieved against the sync op
  collectBlocks?: boolean;
  collectTxReceipts?: boolean;
};

// Defines a sync operation
export type Sync = {
  provider: JsonRpcProvider | WebSocketProvider;
  name: string;
  chainId: number;
  startBlock: number | "latest";
  endBlock: number | "latest";
  eventName: string;
  handlers: string;
  address?: string;
  eventAbi?: ethers.Contract["abi"];
  opts?: {
    mode?: string;
    collectBlocks?: boolean;
    collectTxReceipts?: boolean;
  };
  onEvent: HandlerFn;
};

// response type for a sync summary
export type SyncResponse = {
  error?: any;
  syncOps?: number;
  events?: number;
  runTime?: string;
  chainIds?: number[];
  eventsByChain?: Record<number, number>;
  startBlocksByChain?: Record<number, number>;
  latestBlocksByChain?: Record<number, number>;
  close?: (() => Promise<void>) | undefined;
};

// Export the complete supagraph configuration (sync & graph) - we can add everything from Mappings to this config
export type SyncConfig = {
  // name your supagraph (this will inform mongo table name etc...)
  name: string;
  // set the local engine (true: db || false: mongo)
  dev?: boolean;
  // boolean to reset server between runs (used to move back to startBlock each run)
  reset?: boolean;
  // boolean to start the listening service to keep the chain insync
  listen?: boolean;
  // clean up initial sync tmp files
  cleanup?: boolean;
  // optionally silence std-out until error
  silent?: boolean;
  // set readOnly on the engine via config
  readOnly?: boolean;
  // how many rpc reqs/promises to attempt concurrently
  concurrency?: number;
  // global tx/block capture
  collectBlocks?: boolean;
  collectTxReceipts?: boolean;
  // flag mutable to insert by upsert only (on id field (mutate entities))
  // - otherwise we use _block_number + id to make a unique document and do a distinct groupBy on the id when querying
  //   ie: do everything the immutable way (this can be a lot more expensive)
  mutable?: boolean;
  // how often do we want queries to be revalidated?
  revalidate?: number;
  staleWhileRevalidate?: number;
  // configure providers
  providers: {
    [chainId: number]: {
      rpcUrl: string;
    };
  };
  // register events into named groups
  events: {
    [group: string]: string[];
  };
  // configure available Contracts and their block details
  contracts: {
    [key: string | number]: SyncOp;
  };
  // define supagraph schema
  schema?: string;
  // define supagraph default query
  defaultQuery?: string;
};

// Defines a SyncEvent obj
export type SyncEvent = {
  type: string;
  data: ethers.Event;
  number: number;
  blockNumber: number;
  chainId: number;
  collectTxReceipt: boolean;
  collectBlock: boolean;
  timestamp?: number | undefined;
  from?: string | undefined;
  // in listen-mode a syncEvent can carry the relevant call data
  address?: string;
  eventName?: string;
  id?: string;
  logIndex?: number;
  args?: ethers.utils.Result;
  tx?: TransactionReceipt & TransactionResponse;
  txIndex?: number;
  migrationKey?: number;
  onEvent?: Migration["handler"];
};

// Handlers definition, to map handlers to contracts
export type Handlers = {
  [key: string | number]: {
    [key: string]: HandlerFn;
  };
};

// Handler fn definition
export type HandlerFn = (
  args: any,
  {
    tx,
    block,
    logIndex,
  }: {
    tx: TransactionReceipt & TransactionResponse;
    block: Block;
    logIndex: number;
  }
) => void | Promise<void>;

// Object holder handler onEvents
export type HandlerFns = Record<string, HandlerFn>;

// latestEntity type (for locking the db during syncs)
export type LatestEntity = Record<
  string,
  Entity<{
    id: string;
    latestBlock: number;
    latestBlockTime: number;
    locked: boolean;
    lockedAt: number;
    _block_num: number;
    _block_ts: number;
  }> & {
    id: string;
    locked: boolean;
    lockedAt: number;
    latestBlock: number;
    latestBlockTime: number;
    _block_num: number;
    _block_ts: number;
  }
>;

// Block listener helper
export type AsyncBlockParts = {
  cancelled?: boolean;
  block: BlockWithTransactions;
  receipts: Record<string, TransactionReceipt>;
};
