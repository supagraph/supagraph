// Database store backed by node-persist (this could be replace with something like sqlite)
import Storage from "node-persist";

// import env file and load contents
import dotenv from "dotenv";

// load the .env to check NODE_ENV (only used for this purpose)
dotenv.config();

// this should probably just be string - Record<string, string | number | string> (or anything else which is valid in a mongo setting)
type KV = Record<string, Record<string, Record<string, unknown> | null>>;

// working directory of the calling project or tmp if in prod
export const cwd =
  process.env.NODE_ENV === "development"
    ? `${process.cwd()}/data/`
    : "/tmp/data-"; // we should use /tmp/ on prod for an ephemeral store during the execution of this process (max 512mb of space)

// Simple key-value database store (abstract-leveldown compliantish)
export class DB {
  kv: KV;

  engine?: { newDb: boolean } & Record<string, unknown>;

  useStorage?: boolean;

  // construct a kv store
  constructor(kv: KV, engine?: { newDb: boolean } & Record<string, unknown>) {
    // restore given kv
    this.kv = kv || {};
    this.engine = engine || ({} as { newDb: boolean });
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
    engine?: { newDb: boolean };
  } & Record<string, unknown>) {
    const db = new this(kv, engine);
    await db.update({ kv, name, reset });
    return db;
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

    return null;
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

  async batch(
    vals: {
      type: "put" | "del";
      key: string;
      value?: Record<string, unknown>;
    }[]
  ) {
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
