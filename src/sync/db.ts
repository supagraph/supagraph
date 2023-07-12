// Database store backed by node-persist (this could be replace with something like sqlite)
import Storage from "node-persist";

// this should probably just be string - Record<string, string | number | string> (or anything else which is valid in a mongo setting)
type KV = Record<string, Record<string, Record<string, unknown> | null>>;

// Simple key-value database store (abstract-leveldown compliantish)
export class DB {
  kv: KV;

  store?: Promise<Storage>;

  engine?: { newDb: boolean } & Record<string, unknown>;

  // construct a kv store
  constructor(kv: KV, engine?: { newDb: boolean } & Record<string, unknown>) {
    // restore given kv
    this.kv = kv || {};
    this.engine = engine || ({} as { newDb: boolean });
  }

  static async create({
    kv,
    name,
    engine,
  }: { kv: KV; name?: string; engine?: { newDb: boolean } } & Record<
    string,
    unknown
  >) {
    const db = new this(kv, engine);
    await db.update({ kv, name });
    return db;
  }

  async update({
    kv,
    name,
  }: { kv: KV; name?: string } & Record<string, unknown>) {
    const kvs = { ...kv };
    // check for dev env
    if (process.env.NODE_ENV === "development") {
      // init the localStorage mechanism
      await Storage.init({
        // dump this with the rest of the .next artifacts to be periodically reclaimed?
        dir: `.next/${name || "supagraph"}/node-persist/storage`,
      });
      // restore given kv
      const keys = await Storage.keys();
      if (keys.length) {
        // eslint-disable-next-line no-restricted-syntax
        for (const key of keys) {
          const [ref, id] = key.split(".");
          // eslint-disable-next-line no-await-in-loop
          const val = await Storage.get(key);

          kvs[ref] = kvs[ref] || {};
          kvs[ref][id] = val;
        }
      }
    }

    // restore the kv;
    this.kv = kvs;
  }

  async get(key: string) {
    // spit the key and get from mongo
    const [ref, id] = key.split(".");

    if (process.env.NODE_ENV === "development") {
      const cached = await Storage.get(key);

      return cached;
    }

    if (ref && id) {
      // console.log(`getting ${key}`);
      const val = this.kv[ref]?.[id]; // we might want to split this key on the "." and store into collections here for in-memory graphql'ing

      // return the value
      return val;
    }
    return null;
  }

  async put(key: string, val: Record<string, unknown>) {
    // spit the key and get from mongo
    const [ref, id] = key.split(".");

    // default the collection
    this.kv[ref] = this.kv[ref] || {};

    if (ref && id) {
      // console.log(`putting ${key}`, val);
      this.kv[ref][id] = val;

      return true;
    }

    // set into cache
    if (process.env.NODE_ENV === "development") {
      await Storage.set(key, val);
    }

    return false;
  }

  async del(key: string) {
    // spit the key and get from mongo
    const [ref, id] = key.split(".");

    // default the collection
    this.kv[ref] = this.kv[ref] || {};

    if (ref && id) {
      // console.log(`deleting ${key}`)
      delete this.kv[ref][id];

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
    vals.forEach((val) => {
      const [ref, id] = val.key.split(".");

      // default the collection
      this.kv[ref] = this.kv[ref] || {};

      if (val.type === "put" && val.value) {
        // console.log(`batch - putting: ${val.key} val:`, val.value);
        this.kv[ref][id] = val.value;
        // set into cache
        if (process.env.NODE_ENV === "development") {
          Storage.set(val.key, val.value);
        }
      } else if (val.type === "del") {
        // console.log(`batch - delete ${val.key}`);
        delete this.kv[ref][id];
        if (process.env.NODE_ENV === "development") {
          Storage.del(val.key);
        }
      }
    });

    return true;
  }
}

export default DB;
