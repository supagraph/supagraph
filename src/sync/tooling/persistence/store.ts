// Each Entity is store as TypedMap holding TypedMapEntries
import { Block } from "@ethersproject/providers";
import { BigNumber } from "ethers";

// Convert collection refs to toCamelCase
import { toCamelCase } from "@/utils/toCamelCase";

// Import persistance tooling to construct Type Maps
import { TypedMap } from "@/sync/tooling/persistence/typedMap";
import { TypedMapEntry } from "@/sync/tooling/persistence/typedMapEntry";

// Store can use either in-memory or mongo as a driver
import { DB } from "@/sync/tooling/persistence/db";
import { Stage } from "@/sync/tooling/persistence/stage";

// Construct engine here to prevent circular references - we need to access the static engine inside the static Store
import { Engine } from "@/sync/types";

// create a global to store in-memory engine for duration of request
const engine: Engine = ((global as typeof global & { engine: Engine }).engine =
  (global as typeof global & { engine: Engine }).engine || {
    events: [],
    block: {},
    opSyncs: {},
    callbacks: {},
    eventAbis: {},
    eventIfaces: {},
    startBlocks: {},
  });

// return all entities in the store (async because we'll change this logic to get entities from db)
export const getEntities = async () => {
  // get the root db kv store
  return engine?.db?.kv;
};

// return all entities in the store (async because we'll change this logic to get entities from db)
export const getEngine = async () => {
  return engine;
};

// override the default engine
export const setEngine = async ({
  name,
  db,
}: {
  name: string;
  db: DB | Promise<DB>;
}) => {
  // set the given engine into global (the rest of the props are transient and we'll set them at runtime)
  if (!engine.stage) {
    // set the name
    engine.name = name;
    // resolve the raw db and store
    engine.db = await Promise.resolve(db);
    // set the engine against the db
    if (engine.db) {
      // wrap the db in a checkpoint staging db
      engine.stage = new Stage(engine.db);
      // place the engine against the db for access by ref
      engine.db.engine = engine as { newDb: boolean };
    }
  }
  return engine;
};

// override the default engine
export const resetEngine = async ({
  name,
  db,
}: {
  name: string;
  db: DB | Promise<DB>;
}) => {
  // clear the engine stored in the global engine and setEngine again
  (global as typeof global & { engine: Engine }).engine = (
    global as typeof global & { engine: Engine }
  ).engine || {
    events: [],
    block: {},
    opSyncs: {},
    callbacks: {},
    eventAbis: {},
    eventIfaces: {},
    startBlocks: {},
  };

  return setEngine({ name, db });
};

// Each distinct kv entity - exposed via Store.get<T> - fully typed according to T
export class Entity<T extends { id: string }> extends TypedMap<
  keyof T,
  T[keyof T]
> {
  id: string;

  ref: string;

  chainId: number;

  block: Block | undefined;

  // allow the entity to be fully defined on construct to rebuild prev known entities (entites stored into Store on .save())
  constructor(
    ref: string,
    id: string,
    entries: Array<TypedMapEntry<keyof T, T[keyof T]>>,
    block?: Block,
    chainId?: number
  ) {
    super();
    // record ref for saving to store later
    this.id = id;
    this.ref = ref;
    // set the entities if and as provided
    if (entries) this.entries = entries;

    // update block and chain details when provided
    this.block = block ?? Store.getBlock();
    this.chainId = chainId ?? Store.getChainId() ?? 0;

    // place getters and setters directly onto the instance to expose vals (this is the `T &` section of the type)
    [...this.entries, new TypedMapEntry("id", id)].forEach((kv) => {
      // @ts-ignore
      if (typeof this[kv.key] === "undefined") {
        // define properties
        Object.defineProperty(this, kv.key as string, {
          // Alternatively, use: `get() {}`
          get() {
            return this.get(kv.key);
          },
          // Alternatively, use: `set(newValue) {}`
          set(newValue) {
            this.set(kv.key, newValue);
          },
        });
      }
    });

    // set the provided ID into the entries so thats its definitely available when we save
    this.set("id" as keyof T, id as T["id"]);

    // type assertion to add properties of T directly to the instance
    // eslint-disable-next-line no-constructor-return
    return this as this & Entity<T> & T;
  }

  copy(id?: string) {
    const thisId = id || (this.getEntry("id" as keyof T)?.value as string);
    return new Entity<T>(
      this.ref,
      thisId,
      this.entries.map(
        (entry) =>
          new TypedMapEntry<typeof entry.key, typeof entry.value>(
            entry.key,
            entry.value
          )
      ),
      this.block,
      this.chainId
    ) as Entity<T> & T;
  }

  set<K extends keyof T>(key: K, value: T[K]): void {
    super.set(
      key,
      // eslint-disable-next-line no-underscore-dangle
      (value as { _isBigNumber: boolean })?._isBigNumber
        ? ((value as BigNumber).toString() as unknown as T[keyof T])
        : value
    );
  }

  replace(value: T): void {
    Object.keys(value).forEach((key) => {
      super.set(
        key as keyof T,
        // eslint-disable-next-line no-underscore-dangle
        (value[key] as { _isBigNumber: boolean })?._isBigNumber
          ? ((value[key] as BigNumber).toString() as unknown as T[keyof T])
          : value[key]
      );
    });
  }

  get<K extends keyof T>(key: K): T[K] {
    return super.get(key) as T[K];
  }

  getEntry<K extends keyof T>(key: K): TypedMapEntry<K, T[K]> | null {
    return super.getEntry(key) as TypedMapEntry<K, T[K]> | null;
  }

  async save(updateBlock = true): Promise<Entity<T> & T> {
    // get our id
    const id = this.getEntry("id" as keyof T)?.value as string;

    // this should commit changes (adding them to a batch op to be commited to db later)
    if (id) {
      // update block and chain details when provided
      if (this.block && updateBlock) {
        // adding these regardless of typings - we need them to settle the latest entry from the db
        this.set(
          "_block_ts" as keyof T,
          +`${Number(this.block?.timestamp).toString(10)}` as T[keyof T]
        );
        this.set(
          "_block_num" as keyof T,
          +`${Number(this.block?.number).toString(10)}` as T[keyof T]
        );
      }
      // adding chainId will need to be done on first insert (even if we don't have the block yet)
      if (this.chainId) {
        this.set("_chain_id" as keyof T, this.chainId as T[keyof T]);
      }
      // replace the current entry with the new one
      await Store.set(this.ref, id, this.valueOf() as unknown as T);
    }

    // return a new copy of the saved entity (without fetching from db again)
    return this.copy(id);
  }
}

// Define methods to interact with the data store...
export class Store {
  // if this is async we can grab entities on-demand
  static async get<T extends { id: string }>(
    ref: string,
    id: string,
    newId: boolean = false
  ): Promise<Entity<T> & T> {
    // if we attempt to use this without an engine then prepare one now...
    if (!engine.db)
      await setEngine({
        name: "supagraph",
        db: new DB({}),
      });
    // return the Entity (as a clone or as a new entry (this isnt recorded into state until it is saved later))
    let fromDb: T | boolean = false;
    // attempt to pull from db...
    try {
      // pull from db (if not marked as a newId)
      fromDb =
        !newId &&
        ((await engine?.stage?.get(`${toCamelCase(ref)}.${id}`)) as T);
    } finally {
      // eslint-disable-next-line no-unsafe-finally
      return new Entity<T>(
        ref,
        id,
        (!!fromDb &&
          Object.keys(fromDb).map((key) => {
            return new TypedMapEntry(
              key as keyof T,
              (fromDb as T)[key as keyof T]
            );
          })) ||
          []
      ) as Entity<T> & T;
    }
  }

  static async set<T extends { id: string }>(
    ref: string,
    id: string,
    value: T
  ): Promise<void> {
    // put the value into the db
    await engine?.stage?.put(
      `${toCamelCase(ref)}.${id}`,
      value as unknown as Record<string, unknown>
    );
  }

  static async remove(ref: string, id: string) {
    // del the value set from the db
    await engine?.stage?.del(`${toCamelCase(ref)}.${id}`);
  }

  static async setEngine({ name, db }: { name: string; db: DB | Promise<DB> }) {
    // set db and name in global state
    setEngine({ name, db });

    return engine;
  }

  static setChainId(chainId: number) {
    engine.chainId = chainId;
  }

  static getChainId() {
    return engine.chainId;
  }

  static setBlock(block: Block) {
    engine.block[engine.chainId] = block;
  }

  static clearBlock() {
    engine.block[engine.chainId] = undefined;
  }

  static getBlock() {
    return engine.block[engine.chainId];
  }
}
