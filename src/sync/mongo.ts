// Mongo class wraps mongo with a simple entity management system (abstract-leveldown compatible)
import type { AnyBulkWriteOperation, Document, MongoClient } from "mongodb";

// Extend from db (abstract-leveldown compatible kv implementation)
import { DB } from "./db";

// This should probably just be string - Record<string, string | number | Buffer> (or anything else which is valid in a mongo setting)
type KV = Record<string, Record<string, Record<string, unknown> | null>>;

// Error to throw with a .notFound prop set to true
class NotFound extends Error {
  notFound: boolean;

  constructor(msg: string) {
    super(msg);
    // mark as notFound
    this.notFound = true;
  }
}

// Simple key-value database store (abstract-leveldown compliant)
export class Mongo extends DB {
  // underlying mongo client
  client: MongoClient | Promise<MongoClient>;

  // selected db on the mongo client
  db: ReturnType<MongoClient["db"]> | Promise<ReturnType<MongoClient["db"]>>;

  // name given to the db on the mongo client
  name: string;

  // are the entities in this db being upserted or not?
  mutable: boolean;

  // associated engine (which contains this db)
  declare engine?: { newDb: boolean } & Record<string, unknown>;

  // construct a kv store
  constructor(
    client: MongoClient | Promise<MongoClient>,
    name: string,
    kv: KV,
    mutable?: boolean,
    engine?: { newDb: boolean } & Record<string, unknown>
  ) {
    super(kv);
    // establish connection
    this.client = client;
    // record the connection name
    this.name = name;
    // are the ids unique?
    this.mutable = mutable || false;
    // associate the engine
    this.engine = engine || ({} as { newDb: boolean });
    // resolve the client then attach the named db
    this.db = Promise.resolve(client).then((mongo) =>
      mongo.db(name || "supagraph")
    );
  }

  // create a new instance statically
  static async create({
    client,
    name,
    kv,
    mutable,
    engine,
  }: {
    client: MongoClient | Promise<MongoClient>;
    name: string;
    kv: KV;
    mutable?: boolean;
    engine?: { newDb: boolean } & Record<string, unknown>;
  } & Record<string, unknown>) {
    const db = new this(client, name, kv, mutable, engine);
    await db.update({ kv });
    return db;
  }

  // update the kv store with a new set of values
  async update({ kv }: { kv: KV }) {
    // restore given kv
    this.kv = kv || {};
  }

  // get from mongodb
  async get(key: string) {
    // short-cut the response - we're not storing anything here internally to avoid mem-issues
    if (this.kv[key]) {
      return this.kv[key];
    }

    // otherwise spit the key and get from mongo
    const [ref, id] = key.split(".");

    // for valid reqs...
    if (ref && id && !this.engine.newDb) {
      // this wants to get only the most recent insertion
      return (await Promise.resolve(this.db))
        .collection(ref)
        .findOne({ id }, { sort: { _block_ts: -1 } });
    }

    throw new NotFound("Not Found");
  }

  // store into mongodb
  async put(key: string, val: Record<string, unknown>) {
    // spit the key and get from mongo
    const [ref, id] = key.split(".");

    // for valid reqs...
    if (ref && id) {
      // get the collection for this entity
      const collection = (await Promise.resolve(this.db)).collection(ref);
      // get the most recent document that matches the id
      const document = collection.findOne(
        {
          id,
          // if ids are unique then we can place by update
          ...(this.mutable || ref === "__meta__"
            ? {}
            : {
                _block_ts: val?._block_ts,
                _block_num: val?._block_num,
                _chain_id: val?._chain_id,
              }),
        },
        { sort: { _block_ts: -1 } }
      );

      // this will update the most recent entry or upsert a new document (do we want this to insert a new doc every update?)
      await (await Promise.resolve(this.db)).collection(ref).updateOne(
        {
          ...(document || {}),
          id,
        },
        {
          $set: val,
        },
        {
          upsert: true,
        }
      );
    }

    return true;
  }

  // delete from mongodb (not sure we ever need to do this? We could noop)
  async del(key: string) {
    // spit the key and get from mongo
    const [ref, id] = key.split(".");

    // for valid reqs...
    if (ref && id) {
      // get the collection for this entity
      const collection = (await Promise.resolve(this.db)).collection(ref);
      // this will delete the latest entry - do we want to delete all entries??
      // would it be better to put an empty here instead?
      const document = collection.findOne({ id }, { sort: { _block_ts: -1 } });

      // delete the single document we discovered
      if (document) {
        await collection.deleteOne(document);
      }
    }

    return true;
  }

  // perfom a bulkWrite against mongodb
  async batch(
    vals: {
      type: "put" | "del";
      key: string;
      value?: Record<string, unknown>;
    }[]
  ) {
    // collect every together into appropriate collections
    const byCollection = vals.reduce((collection, val) => {
      // avoid reassigning props of param error
      const collected = collection;

      // pull ref from the given key
      const [ref] = val.key.split(".");

      // keep going or start fresh
      collected[ref] = collected[ref] || [];

      // only collect true values
      if (val.type === "del" || val?.value) {
        collected[ref].push(val);
      }

      return collected;
    }, {} as Record<string, typeof vals>);

    // eslint-disable-next-line no-restricted-syntax
    for (const collection of Object.keys(byCollection)) {
      // eslint-disable-next-line no-await-in-loop
      await (await Promise.resolve(this.db)).collection(collection).bulkWrite(
        // convert the operations into a set of mongodb bulkWrite operations
        byCollection[collection].reduce((operations, val) => {
          // Put operation operates on the id + block to upsert a new entity entry
          if (val.type === "put") {
            operations.push({
              updateOne: {
                filter: {
                  // each entry is unique by block and id
                  id: val.key.split(".")[1],
                  // if ids are unique then we can place by update
                  ...(this.mutable || collection === "__meta__"
                    ? {}
                    : {
                        _block_ts: val.value?._block_ts,
                        _block_num: val.value?._block_num,
                        _chain_id: val.value?._chain_id,
                      }),
                },
                update: {
                  $set: {
                    // ignore the _id key because this will cause an error in mongo
                    ...Object.keys(val.value || {}).reduce((carr, key) => {
                      return {
                        ...carr,
                        // add everything but the _id
                        ...(key !== "_id"
                          ? {
                              [key]: val.value?.[key],
                            }
                          : {}),
                      };
                    }, {} as Record<string, unknown>),
                    // add block details to the insert (this is what makes it an insert - every event should insert a new document)
                    _block_ts: val.value?._block_ts,
                    _block_num: val.value?._block_num,
                    _chain_id: val.value?._chain_id,
                  },
                },
                upsert: true,
              },
            });
          }
          // del will delete ALL entries from the collection (this shouldnt need to be called - it might be wiser to insert an empty entry than to try to delete anything)
          if (val.type === "del") {
            operations.push({
              deleteMany: {
                filter: { id: val.key.split(".")[1] },
              },
            });
          }
          // returns all Document operations
          return operations;
        }, [] as unknown as AnyBulkWriteOperation<Document>[]),
        {
          // allow for parallel writes (we've already ensured one entry per key with our staged sets (use checkpoint & commit))
          ordered: false,
          // write objectIds mongo side
          forceServerObjectId: true,
        }
      );
    }

    return true;
  }
}

export default Mongo;
