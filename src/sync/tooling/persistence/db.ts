// Import env file and load contents
import dotenv from "dotenv";

// Import types used by db
import { BatchOp, Engine, KV } from "@/sync/types";

// Import node-persist to power the local db store (this could be replace with something like sqlite)
import Storage from "node-persist";

// Get the user land current working directory
import { cwd } from "@/utils";

// load the .env to check NODE_ENV (only used for this purpose)
dotenv.config();

// Error to throw with a .notFound prop set to true
export class NotFound extends Error {
  notFound: boolean;

  constructor(msg: string) {
    super(msg);
    // mark as notFound
    this.notFound = true;
  }
}

// Simple key-value database store (abstract-leveldown compliantish)
export class DB {
  kv: KV;

  engine: Engine;

  useStorage?: boolean;

  // construct a kv store
  constructor(kv: KV, engine?: Engine) {
    // restore given kv
    this.kv = kv || {};
    this.engine = engine || ({} as Engine);
  }

  static async create({
    kv,
    name,
    reset,
    engine,
  }: {
    kv: KV;
    name?: string;
    reset?: boolean;
    engine?: Engine;
  } & Record<string, unknown>) {
    const db = new this(kv, engine);
    await db.update({ kv, name, reset });
    return db as DB & Record<string, any>;
  }

  async update({
    kv,
    name,
    reset,
  }: { kv: KV; name?: string; reset?: boolean } & Record<string, unknown>) {
    // use given kv
    const kvs = { ...kv };
    // init the localStorage mechanism
    await Storage?.init?.({
      // dump this with the rest of the .next artifacts to be periodically reclaimed?
      dir: `${cwd}${name || "supagraph"}-node-persist-storage`,
    });
    // clear the db between runs to move back to the start/to renew connection between external db and interal
    if (reset) {
      await Storage?.clear?.();
    }
    // restore given kv
    const keys = await Storage?.keys?.();
    if (keys.length) {
      // eslint-disable-next-line no-restricted-syntax
      for (const key of keys) {
        const [ref, id] = key.split(".");
        // eslint-disable-next-line no-await-in-loop
        const val = await Storage?.get?.(key);

        kvs[ref] = kvs[ref] || {};
        kvs[ref][id] = val;
      }
    }
    // mark as storage enabled
    this.useStorage = true;

    // restore the kv store
    this.kv = kvs;
  }

  async get(key: string) {
    // spit the key and get from kv store
    const [ref, id] = key.split(".");

    if (ref && id) {
      // console.log(`getting ${key}`);
      const val = this.kv[ref]?.[id];

      // return the value
      if (val) return val;
    }
    if (ref && !id) {
      // console.log(`getting ${ref}`);
      const val = this.kv[ref];

      // return the collection
      if (val) return Object.values(val);
    }

    // throw not found to indicate we can't find it
    throw new NotFound("Not Found");
  }

  async put(key: string, val: Record<string, unknown>) {
    // spit the key and get from kv store
    const [ref, id] = key.split(".");

    // default the collection
    this.kv[ref] = this.kv[ref] || {};

    if (ref && id) {
      // console.log(`putting ${key}`, val);
      this.kv[ref][id] = val;

      // set into cache
      if (this.useStorage) await Storage?.set?.(key, val);

      return true;
    }

    return false;
  }

  async del(key: string) {
    // spit the key and get from kv store
    const [ref, id] = key.split(".");

    // default the collection
    this.kv[ref] = this.kv[ref] || {};

    if (ref && id) {
      // console.log(`deleting ${key}`)
      delete this.kv[ref][id];

      // clear from storage
      if (this.useStorage) await Storage?.del?.(key);

      return true;
    }

    return false;
  }

  async batch(vals: BatchOp[]) {
    await Promise.all(
      vals.map(async (val) => {
        const [ref, id] = val.key.split(".");

        // default the collection
        this.kv[ref] = this.kv[ref] || {};

        if (val.type === "put" && val.value) {
          // console.log(`batch - putting: ${val.key} val:`, val.value);
          this.kv[ref][id] = val.value;
          // set into cache
          if (this.useStorage) await Storage?.set?.(val.key, val.value);
        } else if (val.type === "del") {
          // console.log(`batch - delete ${val.key}`);
          delete this.kv[ref][id];
          // clear from storage
          if (this.useStorage) await Storage?.del?.(val.key);
        }
      })
    );

    return true;
  }
}

export default DB;
