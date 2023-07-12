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
  keyValueMap: Map<string, Record<string, unknown> | null>;
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
    this.checkpoints.push({
      keyValueMap: new Map<string, Record<string, unknown>>(),
    });
  }

  // commits the latest checkpoint to the underlying db mechanism
  async commit() {
    const { keyValueMap } = this.checkpoints.pop()!;
    if (!this.isCheckpoint) {
      // this was the final checkpoint, we should now commit and flush everything to disk
      const batchOp: BatchDBOp[] = [];
      keyValueMap.forEach((value, key) => {
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
      await this.batch(batchOp);
    } else {
      // dump everything into the current (higher level) cache
      const currentKeyValueMap =
        this.checkpoints[this.checkpoints.length - 1].keyValueMap;
      keyValueMap.forEach((value, key) => currentKeyValueMap.set(key, value));
    }
  }

  // retrieves a raw value from leveldb.
  async get(key: string): Promise<Record<string, unknown> | null> {
    // lookup the value in our cache - we return the latest checkpointed value (which should be the value on disk)
    for (let index = this.checkpoints.length - 1; index >= 0; index -= 1) {
      const value = this.checkpoints[index].keyValueMap.get(key);
      if (value !== undefined) {
        return value;
      }
    }

    // if this is a newDb (we're starting the collection from startBlock) then we can skip looking up the item in the db (it won't be there)
    if (!this.db.engine?.newDb) {
      // nothing has been found in cache, look up from disk
      const value = await this.db.get(key);
      if (this.isCheckpoint) {
        // since we are a checkpoint, put this value in cache, so future `get` calls will not look the key up again from disk
        this.checkpoints[this.checkpoints.length - 1].keyValueMap.set(
          key,
          value
        );
      }
      return value;
    }

    return null;
  }

  // writes a value directly to leveldb
  async put(key: string, val: Record<string, unknown>): Promise<boolean> {
    if (this.isCheckpoint) {
      // put value in cache
      this.checkpoints[this.checkpoints.length - 1].keyValueMap.set(key, val);
      return true;
    }
    return this.db.put(key, val);
  }

  // removes a raw value in the underlying leveldb
  async del(key: string): Promise<boolean> {
    if (this.isCheckpoint) {
      // delete the value in the current cache
      this.checkpoints[this.checkpoints.length - 1].keyValueMap.set(key, null);
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
