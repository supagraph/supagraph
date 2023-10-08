// Stage will extend DB to hold local state before it is commited to the constructed DB (on commit)
import { DB } from "./db";

// import types used by Stage
import { BatchOp } from "./types";

// Store any ops during checkpoint into keyValueMap to execute on parent later
export type Checkpoint = {
  keyValueMap: Map<string, (Record<string, unknown> | null)[]>;
};

// DB is a simple implementation of levelup working in memory over a kv store
export class Stage extends DB {
  // checkpoint data to enable commits + reverts
  public checkpoints: Checkpoint[];

  // the simple KV datastore backing this service
  public db: DB;

  // initialize a DB instance (checkpoint must extend from DB - create alternative checkpointed drivers eg mongo by feeding them through the constructor)
  constructor(db: DB) {
    // start a fresh kv store for the checkpoints
    super({});
    // set the db
    this.db = db;
    // keyValueMap state checkpoints
    this.checkpoints = [];
  }

  // is the DB currently inside a checkpoint phase?
  get isCheckpoint() {
    return this.checkpoints.length > 0;
  }

  // adds a new checkpoint to the stack
  checkpoint() {
    // each checkpoint holds a mapping of id -> value[] -- if db is mutable then we need to store all unique alterations as distinct entries
    this.checkpoints.push({
      keyValueMap: new Map<string, Record<string, unknown>[]>(),
    });
  }

  // commits the latest checkpoint to the underlying db mechanism/parent checkpoint
  async commit() {
    // take the last checkpoint from the array (LIFO)
    const { keyValueMap } = this.checkpoints.pop()! || {
      keyValueMap: new Map<string, Record<string, unknown>[]>(),
    };

    // if we have a checkpoint open then commit to parent checkpoint
    if (this.isCheckpoint) {
      // dump everything into the current (higher level) cache
      const currentKeyValueMap =
        this.checkpoints[this.checkpoints.length - 1].keyValueMap;
      // combine parent set and current set
      keyValueMap.forEach((value, key) => {
        // collect current set
        const currentSet = !(this.db as { mutable?: boolean }).mutable
          ? currentKeyValueMap.get(key) || []
          : [];
        // combine the current entries with the new entries (if !mutable there will only be 1 entry)
        currentKeyValueMap.set(key, [
          ...currentSet.filter((v) => v || v === null),
          ...value,
        ]);
      });

      // completed the action - return true
      return Promise.resolve(true);
    }

    // this was the final checkpoint, we should now commit and flush everything to disk
    const opStack: BatchOp[] = [];
    // convert the keyValueMap into a collection of entries grouped by chainId+blockNum
    keyValueMap.forEach((values, key) => {
      // for every other key except for __meta__ when immutable...
      if (
        // if immutable then save each item from each block
        !(this.db as { mutable?: boolean }).mutable &&
        // if from meta table then always save as mutable
        key.indexOf("__meta__.") !== 0
      ) {
        // record only one entry per block for each chainId each commit
        const recorded = [];
        // check each latest entry by block-number/chainId combo into the batch
        values.reverse().forEach((value) => {
          // only delete on last entry
          if (
            value === null &&
            recorded.indexOf(`${value._chain_id}-${value._block_num}`) === -1
          ) {
            opStack.push({
              key,
              type: "del",
            });
            recorded.push(`${value._chain_id}-${value._block_num}`);
          } else if (
            value &&
            recorded.indexOf(`${value._chain_id}-${value._block_num}`) === -1
          ) {
            opStack.push({
              key,
              type: "put",
              value,
            });
            recorded.push(`${value._chain_id}-${value._block_num}`);
          }
        });
      } else if (values.length && values[values.length - 1] === null) {
        opStack.push({
          key,
          type: "del",
        });
      } else if (values.length && values[values.length - 1]) {
        opStack.push({
          key,
          type: "put",
          value: values[values.length - 1],
        });
      }
    });

    // commit this batch against the associated db implementation
    return this.batch(opStack);
  }

  // pop the last checkpoint and discard
  revert() {
    // drop the most recent checkpoint (dropping the values)
    if (this.isCheckpoint) {
      this.checkpoints.pop()!;
    }
  }

  // retrieves a raw value from leveldb or the latest checkpoint
  async get(key: string) {
    // lookup the value in our cache - we return the latest checkpointed value (which should be the value on disk)
    for (let index = this.checkpoints.length - 1; index >= 0; index -= 1) {
      const values = this.checkpoints[index].keyValueMap.get(key);
      if (values !== undefined && values[values.length - 1] !== undefined) {
        // return a deep copy of the values to avoid mutating in place
        return JSON.parse(JSON.stringify(values[values.length - 1]));
      }
    }

    // if this is a newDb or we have a warm cache then we can skip looking up the item in the db
    if (!this.db.engine?.newDb) {
      // nothing has been found in cache, look up from disk
      const value = await this.db.get(key);
      // return the value if found (or not)
      return value;
    }

    // undiscovered - return null
    return null;
  }

  // writes a value directly to leveldb or stores it in a checkpoint
  async put(key: string, val: Record<string, unknown>): Promise<boolean> {
    if (this.isCheckpoint) {
      const currentSet = !(this.db as { mutable?: boolean }).mutable
        ? this.checkpoints[this.checkpoints.length - 1].keyValueMap.get(key) ||
          []
        : [];
      // put value in cache (this ensures we make only one write per key)
      this.checkpoints[this.checkpoints.length - 1].keyValueMap.set(
        key,
        // append val to currentSet - this will retain all unique entries for !mutable sets
        [...currentSet, val].filter((v) => v || v === null)
      );
      // marked as recorded
      return true;
    }
    // put the value into the db
    return this.db.put(key, val);
  }

  // removes a raw value in the underlying leveldb or stores it in a checkpoint
  async del(key: string): Promise<boolean> {
    if (this.isCheckpoint) {
      const currentSet = !(this.db as { mutable?: boolean }).mutable
        ? this.checkpoints[this.checkpoints.length - 1].keyValueMap.get(key) ||
          []
        : [];
      // delete the value in the current cache (for mutable sets we never delete but we should insert an empty)
      this.checkpoints[this.checkpoints.length - 1].keyValueMap.set(key, [
        ...currentSet,
        null,
      ]);
      // marked as recorded
      return true;
    }
    // delete the value from the db
    return this.db.del(key);
  }

  // performs a batch operation on db
  async batch(opStack: BatchOp[]): Promise<boolean> {
    if (this.isCheckpoint) {
      // eslint-disable-next-line no-restricted-syntax
      for (const op of opStack) {
        if (op.type === "put" && op.value) {
          // eslint-disable-next-line no-await-in-loop
          return this.put(op.key, op.value);
        }
        if (op.type === "del") {
          // eslint-disable-next-line no-await-in-loop
          return this.del(op.key);
        }
      }
    }
    // else commit it to db
    return this.db.batch(opStack);
  }
}
