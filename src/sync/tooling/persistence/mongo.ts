// Mongo class wraps mongo with a simple entity management system (abstract-leveldown compatible)
import type {
  AnyBulkWriteOperation,
  Document,
  Filter,
  MongoClient,
  WithId,
  WithoutId,
} from "mongodb";

// Extend from db (abstract-leveldown compatible kv implementation)
import { BatchOp, Engine, KV } from "@/sync/types";
import { DB, NotFound } from "@/sync/tooling/persistence/db";

// Simple key-value database store (abstract-leveldown compliant)
export class Mongo extends DB {
  // kv store holds a materialised view of current state in local cache (by ref.id)
  declare kv: KV;

  // engine carries global state
  declare engine: Engine;

  // useStorage will be undefined on this adapter because we're bypassing DB.update()
  declare useStorage?: boolean;

  // underlying mongo client
  client: MongoClient | Promise<MongoClient>;

  // name given to the db on the mongo client
  name: string;

  // are the entities in this db being upserted or not?
  mutable: boolean;

  // selected db on the mongo client
  _db: ReturnType<MongoClient["db"]> | Promise<ReturnType<MongoClient["db"]>>;

  // construct a kv store
  constructor(
    client: MongoClient | Promise<MongoClient>,
    name: string,
    kv: KV,
    mutable?: boolean,
    engine?: Engine
  ) {
    // init super
    super(kv, engine);
    // establish connection
    this.client = client;
    // record the connection name
    this.name = name;
    // are the ids unique?
    this.mutable = mutable || false;
    // associate the engine
    this.engine = engine || ({} as Engine);
    // store the db client
    this._db = this.db();
  }

  async db() {
    // return a new client connection
    const client = await this.client;

    // reopen the connection incase it is closed
    await client.connect();

    // connect it to mongodb
    return client.db(this.name || "supagraph");
  }

  async resetConnection() {
    // return a new client connection
    const client = await this.client;

    // close the connection
    await client.close();

    // set the db against context to reopen the connection
    this._db = this.db();
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
    engine?: Engine;
  } & Record<string, unknown>) {
    const db = new this(client, name, kv, mutable, engine);
    await db.update({ kv });
    return db;
  }

  // update the kv store with a new set of values (we're not starting node-persist here)
  async update({ kv }: { kv: KV } & Record<string, unknown>) {
    // use given kv
    const kvs = { ...kv };
    // restore the kv store
    this.kv = kvs;
  }

  // get from mongodb
  async get(key: string) {
    // otherwise spit the key and get from mongo
    const [ref, id] = key.split(".");

    // TODO: the hotpath here is usually checking on things which don't exist in the db
    //  - we've solved for this in one implementation by loading everything in to memory and setting engine.warmDb [to prevent db reads], but this has a mem limit (however heap is manageble atm)...
    //  - we could have a system to record only the index and store that in mem (btree + bloomfilters), but we will still risk reaching limits...
    //  - when we start nearing our limit we could move the btree to disk and continue there (depending on depth this could require several reads to seek by key)
    //  - alternatively we can perist everything to disk by key and we can refrain from doing any seeks by range etc - this will require local storage space the same size as the db medium
    //  - or we give up on using a db persistance layer and store everything locally - this would mean stateful deployments/a connected persistence medium - i/o could still be a constraint.

    // check in runtime cache
    if (ref && id) {
      // console.log(`getting ${key}`);
      const val = this.kv[ref]?.[id];

      // return the value
      if (val) return val;
    }

    // for valid single entry get...
    if (
      ref &&
      id &&
      !this.engine.newDb &&
      (!this.engine.warmDb || ref === "__meta__")
    ) {
      // this wants to get only the most recent insertion
      const result = await (await this._db)
        .collection(ref)
        .findOne({ id }, { sort: { _block_ts: -1 } });

      // return if we discovered a result
      if (result) return result;
    }

    // for valid entry reqs (used in migrations)...
    if (
      ref &&
      !id &&
      !this.engine.newDb &&
      (!this.engine.warmDb || ref === "__meta__")
    ) {
      // default the collection
      this.kv[ref] = this.kv[ref] || {};

      // return a materialised view for immutable collection - this will get a snapshot of the most recent state for each id...
      if (!this.mutable && ref !== "__meta__") {
        // get collection for the ref
        const collection = (await this._db).collection(ref);
        // get the total number of results we expect to retrieve
        const totalCountAggregate = collection.aggregate([
          {
            $sort: {
              _block_ts: -1,
            },
          },
          {
            $group: {
              _id: "$id",
              latestDocument: {
                $first: "$$ROOT",
              },
            },
          },
          {
            $count: "totalDocuments",
          },
        ]);
        // move skip along to paginate the result
        let skip = 0;
        // batch them into 10000 blocks to not overwhelm the driver
        const batchSize = 10000;
        const totalCount = await totalCountAggregate.next();
        const totalCountResult = totalCount ? totalCount.totalDocuments : 0;
        // return a page of results
        const getPage = async () => {
          return collection
            .aggregate([
              {
                $sort: {
                  _block_ts: -1,
                },
              },
              {
                $group: {
                  _id: "$id",
                  latestDocument: {
                    $first: "$$ROOT",
                  },
                },
              },
              {
                $replaceRoot: { newRoot: "$latestDocument" },
              },
              {
                $skip: skip,
              },
              {
                $limit: batchSize,
              },
            ])
            .toArray();
        };
        // collect all results into array
        const result = [];
        // while theres still results left...
        while (skip < totalCountResult) {
          // get the page and check its content before moving on...
          await getPage().then((page) => {
            // compare the len before to the len after to make sure we're fetching every document
            const beforeLen = result.length;
            // process the page results
            page.forEach((obj) => {
              if (obj) {
                // store into kv
                this.kv[ref][obj.id] = obj;
                // push all to the result
                result.push(obj);
              }
            });
            // move the skip pointer along only if we got an obj for every document in the collection
            skip +=
              beforeLen + batchSize === result.length ||
              result.length >= totalCountResult
                ? batchSize
                : 0;
          });
        }

        // return if the collections holds values
        if (result && result.length) return Object.values(this.kv[ref]);
      } else {
        // fing all on the ref table
        const result = await (
          await this._db
        )
          // if we're immutable then get from the materialised view
          .collection(ref)
          .find({})
          .toArray();

        // place into kv store
        result.forEach((obj) => {
          if (obj) {
            // store into kv
            this.kv[ref][obj.id] = obj;
          }
        });

        // return if the collection holds values
        if (result && result.length) return result;
      }
    } else if (ref && !id) {
      // can get all from kv store
      const val = this.kv[ref];

      // return the collection based on keys
      if (val) return Object.keys(val).map((valKey) => val[valKey]);
    }

    // throw not found to indicate we can't find it
    throw new NotFound("Not Found");
  }

  // store into mongodb
  async put(key: string, val: Record<string, unknown>) {
    // spit the key and get from mongo
    const [ref, id] = key.split(".");

    // default the collection
    this.kv[ref] = this.kv[ref] || {};

    // for valid reqs...
    if (ref && id) {
      // console.log(`putting ${key}`, val);
      this.kv[ref][id] = val;

      // prevent alterations in read-only mode
      if (!this.engine.readOnly) {
        // resolve db promise layer
        const db = await this._db;
        // filter for the document we want to replace
        const filter = {
          id,
          // if ids are unique then we can place by update by checking for a match on current block setup
          ...(this.mutable || ref === "__meta__"
            ? {}
            : {
                _block_ts: val?._block_ts,
                _block_num: val?._block_num,
                _chain_id: val?._chain_id,
              }),
        };
        // attempt the put and retry on failure until it succeeds (all other processing will be halted while we wait for this to complete)
        await put(db, ref, filter, val).catch(async function retry() {
          // wait a second before trying again
          await new Promise((resolve) => {
            setTimeout(
              resolve,
              // anywhere between 1 and 5 seconds
              Math.floor((Math.random() * (5 - 1) + 1) * 1e3)
            );
          });
          // retry the action (with another timeout on failure)
          return put(db, ref, filter, val).catch(retry);
        });
      }
    }

    return true;
  }

  // delete from mongodb (not sure we ever need to do this? We could noop)
  async del(key: string) {
    // spit the key and get from mongo
    const [ref, id] = key.split(".");

    // default the collection
    this.kv[ref] = this.kv[ref] || {};

    // for valid reqs...
    if (ref && id) {
      // remove from local-cache (the next time we put on this key we will produce a new empty object as starting point)
      delete this.kv[ref][id];

      // prevent alterations in read-only mode
      if (!this.engine.readOnly) {
        // resolve db promise layer
        const db = await this._db;
        // this will delete the latest entry - do we want to delete all entries??
        // would it be better to put an empty here instead?
        const document = (await this.get(key)) as WithId<Document>;

        // delete the single document we discovered
        if (document) {
          // keep attempting the delete until it succeeds
          await del(db, ref, document).catch(async function retry() {
            // wait a second before trying again
            await new Promise((resolve) => {
              setTimeout(
                resolve,
                // anywhere between 1 and 5 seconds
                Math.floor((Math.random() * (5 - 1) + 1) * 1e3)
              );
            });
            // retry the action (with another timeout on failure)
            return del(db, ref, document).catch(retry);
          });
        }
      }
    }

    return true;
  }

  // perfom a bulkWrite against mongodb
  async batch(vals: BatchOp[]) {
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
        // make sure the store exists to cache values into
        this.kv[ref] = this.kv[ref] || {};
      }

      return collected;
    }, {} as Record<string, typeof vals>);

    // eslint-disable-next-line no-restricted-syntax
    for (const collection of Object.keys(byCollection)) {
      // convert the operations into a set of mongodb bulkWrite operations
      const batch = byCollection[collection].reduce((operations, val) => {
        // Put operation operates on the id + block to upsert a new entity entry
        if (val.type === "put") {
          // construct replacement value set
          const replacement = {
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
          };
          // push operation for bulkwrite
          operations.push({
            replaceOne: {
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
              replacement,
              // insert new entry if criteria isnt met
              upsert: true,
            },
          });
          // store in to local-cache
          this.kv[collection][val.key.split(".")[1]] = replacement;
        }
        // del will delete ALL entries from the collection (this shouldnt need to be called - it might be wiser to insert an empty entry than to try to delete anything)
        if (val.type === "del") {
          operations.push({
            deleteOne: {
              filter: {
                // each entry is unique by block and id
                id: val.key.split(".")[1],
              },
              // use the _block_ts index to pick the most recent inserted
              hint: { _block_ts: -1 },
            },
          });
          // delete the value from cache
          delete this.kv[collection][val.key.split(".")[1]];
        }
        // returns all Document operations
        return operations;
      }, [] as AnyBulkWriteOperation<Document>[]);

      // save the batch to mongo
      if (!this.engine.readOnly && batch.length) {
        // resolve the db
        const db = await this._db;
        // don't stop trying until this returns successfully
        await bulkWrite(db, collection, batch).catch(async function retry() {
          // wait a second before trying again
          await new Promise((resolve) => {
            setTimeout(
              resolve,
              // anywhere between 1 and 5 seconds
              Math.floor((Math.random() * (5 - 1) + 1) * 1e3)
            );
          });
          // retry the action (with another timeout)
          return bulkWrite(db, collection, batch).catch(retry);
        });
      }
    }

    return true;
  }
}

// attempt the put operation (we use replaceOne here and uniquify with a filter if we need to)
const put = async (
  db: ReturnType<MongoClient["db"]>,
  collection: string,
  filter: Filter<Document>,
  replacement: WithoutId<Document>
) => {
  // replace with upsert to insert if filter doesnt match
  await db.collection(collection).replaceOne(filter, replacement, {
    upsert: true,
  });
};

// attempt the put operation
const del = async (
  db: ReturnType<MongoClient["db"]>,
  collection: string,
  document: WithId<Document>
) => {
  await db.collection(collection).deleteOne({
    // delete only the specified document
    _id: document._id,
  });
};

// attempt the bulkWrite operation
const bulkWrite = async (
  db: ReturnType<MongoClient["db"]>,
  collection: string,
  batch: AnyBulkWriteOperation<Document>[]
) => {
  await db.collection(collection).bulkWrite(batch, {
    // allow for parallel writes (we've already ensured one entry per key with our staged sets (using checkpoint & commit))
    ordered: false,
    // write objectIds mongo side
    forceServerObjectId: true,
  });
};

export default Mongo;
