// Stage will extend DB to hold local state before it is commited to the constructed DB (on commit)
import { DB } from "./db";

// describes a bactch operation
type BatchDBOp = {
  type: "put" | "del";
  key: string;
  value?: Record<string, unknown>;
};

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
    // each checkpoint holds a mapping of id -> value[] -- if db is mutable then we need to store all alterations as distinct entries
    this.checkpoints.push({
      keyValueMap: new Map<string, Record<string, unknown>[]>(),
    });
  }

  // commits the latest checkpoint to the underlying db mechanism/parent checkpoint
  async commit() {
    const { keyValueMap } = this.checkpoints.pop()!;
    if (!this.isCheckpoint && keyValueMap.size) {
      // this was the final checkpoint, we should now commit and flush everything to disk
      const batchOp: BatchDBOp[] = [];
      keyValueMap.forEach((values, key) => {
        values.forEach((value) => {
          if (value === null) {
            batchOp.push({
              key,
              type: "del",
            });
          } else {
            batchOp.push({
              key,
              type: "put",
              value,
            });
          }
        });
      });
      return this.batch(batchOp);
    }
    // if we have a checkpoint open then commit to parent checkpoint
    if (this.isCheckpoint) {
      // dump everything into the current (higher level) cache
      const currentKeyValueMap =
        this.checkpoints[this.checkpoints.length - 1].keyValueMap;
      keyValueMap.forEach((value, key) =>
        // combine the current entries with the new entries (if !mutable there will only be 1 entry)
        currentKeyValueMap.set(key, [
          ...(!(this.db as unknown as { mutable: boolean }).mutable
            ? currentKeyValueMap.get(key) || []
            : []),
          ...value,
        ])
      );
    }

    // completed the action - return true
    return Promise.resolve(true);
  }

  // retrieves a raw value from leveldb or the latest checkpoint
  async get(key: string): Promise<Record<string, unknown> | null> {
    // lookup the value in our cache - we return the latest checkpointed value (which should be the value on disk)
    for (let index = this.checkpoints.length - 1; index >= 0; index -= 1) {
      const values = this.checkpoints[index].keyValueMap.get(key);
      if (values !== undefined && values[values.length - 1] !== undefined) {
        // return a deep copy of the values to avoid mutating in place
        return JSON.parse(JSON.stringify(values[values.length - 1]));
      }
    }

    // if this is a newDb (we're starting the collection from startBlock) then we can skip looking up the item in the db (it won't be there)
    if (!this.db.engine?.newDb) {
      // nothing has been found in cache, look up from disk
      const value = await this.db.get(key);
      if (this.isCheckpoint) {
        // since we are in a checkpoint, put this value in cache, so future `get` calls will not look the key up again from disk
        this.checkpoints[this.checkpoints.length - 1].keyValueMap.set(key, [
          value,
        ]);
      }
      return value;
    }

    return null;
  }

  // writes a value directly to leveldb or stores it in a checkpoint
  async put(key: string, val: Record<string, unknown>): Promise<boolean> {
    if (this.isCheckpoint) {
      // put value in cache (this ensures we make only one write per key)
      this.checkpoints[this.checkpoints.length - 1].keyValueMap.set(
        // mutable objects should be pushed with an additional key part
        key,
        [
          ...(!(this.db as unknown as { mutable: boolean }).mutable
            ? this.checkpoints[this.checkpoints.length - 1].keyValueMap.get(
                key
              ) || []
            : []),
          val,
        ]
      );
      return true;
    }
    return this.db.put(key, val);
  }

  // removes a raw value in the underlying leveldb or stores it in a checkpoint
  async del(key: string): Promise<boolean> {
    if (this.isCheckpoint) {
      // delete the value in the current cache (for mutable sets we never delete but we should insert an empty)
      this.checkpoints[this.checkpoints.length - 1].keyValueMap.set(key, [
        ...(!(this.db as unknown as { mutable: boolean }).mutable
          ? this.checkpoints[this.checkpoints.length - 1].keyValueMap.get(
              key
            ) || []
          : []),
        null,
      ]);
      return true;
    }
    // delete the value on disk
    return this.db.del(key);
  }

  // performs a batch operation on db
  async batch(opStack: BatchDBOp[]): Promise<boolean> {
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
