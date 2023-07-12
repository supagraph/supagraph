/* eslint-disable max-classes-per-file, @typescript-eslint/no-use-before-define */

// Each Entity is store as TypedMap holding TypedMapEntries

import { Block } from "@ethersproject/providers";

import { BigNumber } from "ethers";
import { TypedMap } from "./typedMap";
import { TypedMapEntry } from "./typepMapEntry";

// Store can use either in-memory or mongo as a driver
import { DB } from "./db";
import { Stage } from "./stage";

// convert collection refs to toCamelCase
import { toCamelCase } from "../utils/toCamelCase";

type Engine = {
  name?: string;
  db?: DB;
  stage?: Stage;
  chainId?: number;
  block?: Block;
  newDb?: boolean;
};

// create a global to store in-memory engine for duration of request
// eslint-disable-next-line no-multi-assign
const engine: Engine = ((global as typeof global & { engine: Engine }).engine =
  (global as typeof global & { engine: Engine }).engine || {}); // should we default this to the memory db?

// Entity store -- flat array of entity lookups keyed by ref-id - this will be backed by a levelDown like driver
const entities: {
  [ref: string]: Record<string, Entity<{ id: string }>>;
} = {};

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
    // wrap the db in a checkpoint staging db
    engine.stage = new Stage(engine.db);
  }
};

// Each distinct kv entity - exposed via Store.get<T> - fully typed according to T
export class Entity<T extends { id: string }> extends TypedMap<
  keyof T,
  T[keyof T]
> {
  ref: string;

  // allow the entity to be fully defined on construct to rebuild prev known entities (entites stored into Store on .save())
  constructor(
    ref: string,
    id: string,
    entries: Array<TypedMapEntry<keyof T, T[keyof T]>>
  ) {
    super();
    // record ref for saving to store later
    this.ref = ref;
    // set the entities if and as provided
    if (entries) this.entries = entries;

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
    return this as unknown as Entity<T> & T;
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

  get<K extends keyof T>(key: K): T[K] {
    return super.get(key) as T[K];
  }

  getEntry<K extends keyof T>(key: K): TypedMapEntry<K, T[K]> | null {
    return super.getEntry(key) as TypedMapEntry<K, T[K]> | null;
  }

  async save(): Promise<Entity<T> & T> {
    // get our id
    const id = this.getEntry("id" as keyof T)?.value as string;
    // pull the block
    const block = Store.getBlock();
    // pull the chainID
    const chainId = Store.getChainId();

    // this should commit changes (adding them to a batch op to be commited to db later)
    if (id) {
      // update block and chain details when provided
      if (block) {
        // adding these regardless of typings - we need them to settle the latest entry from the db
        this.set("_block_ts" as keyof T, block.timestamp as T[keyof T]);
        this.set("_block_num" as keyof T, block.number as T[keyof T]);
      }
      // adding chainId will need to be done on first insert (even if we don't have the block yet)
      if (chainId) {
        this.set("_chain_id" as keyof T, chainId as T[keyof T]);
      }
      // replace the current entry with the new one
      await Store.set(
        this.ref,
        id,
        this.valueOf() as unknown as Entity<{ id: string }>
      );
    }

    // return the new copy of the saved entity
    return Store.get(this.ref, id);
  }
}

// Define methods to interact with the data store...
export class Store {
  // if this is async we can grab entities on-demand
  static async get<T extends { id: string }>(
    ref: string,
    id: string,
    newId: boolean = false
  ): Promise<T & Entity<T>> {
    // if we attempt to use this without an engine then prepare one now...
    if (!engine.db)
      await setEngine({
        name: "supagraph",
        db: new DB({}),
      });
    // default the entity if missing
    entities[ref] = entities[ref] || {};
    // return the Entity (as a clone or as a new entry (this isnt recorded into state until it is saved later))
    return (entities[ref][id]?.entries
      ? // always return a new clone of the Entity to avoid mutation by ref
        new Entity<T>(
          ref,
          id,
          // returning a clone of the typeMap array and each TypedMapEntry to prevent unexpected mutations
          (
            entities[ref][id].entries as TypedMapEntry<keyof T, T[keyof T]>[]
          ).map((v) => new TypedMapEntry(v.key, v.value))
        )
      : await (async () => {
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
            ) as unknown as T & Entity<T>;
          }
        })()) as unknown as T & Entity<T>;
  }

  static async set<T extends { id: string }>(
    ref: string,
    id: string,
    value: Entity<T>
  ): Promise<void> {
    // default the entity if missing
    entities[ref] = entities[ref] || {};
    // set the value
    entities[ref][id] = value as Entity<T | { id: string }>;
    // put the value into the db
    await engine?.stage?.put(
      `${toCamelCase(ref)}.${id}`,
      value as unknown as Record<string, unknown>
    );
  }

  static async remove(ref: string, id: string) {
    // clear the value
    delete entities[`${ref}`][`${id}`];
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
    engine.block = block;
  }

  static clearBlock() {
    engine.block = undefined;
  }

  static getBlock() {
    return engine.block;
  }
}
