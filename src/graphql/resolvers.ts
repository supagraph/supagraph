import { MongoClient } from "mongodb";

import {
  Args,
  Entities,
  EntityRecord,
  Key,
  Operation,
  SimpleSchema,
} from "./types";

import { DB } from "../sync/db";
import { toCamelCase } from "../utils/toCamelCase";
import { generateIndexes, createQuery, unwindResult } from "./mongo";

// Filter key content to only the named fields (no plural field information)
export const filteredKeys = (schema: SimpleSchema) => {
  // filter out plural information keys
  return Object.keys(schema).filter((entity) => {
    // these are used to carry the plural/single form through the initial setup - we don't need them here
    return !entity.match(/-plural-form$/) && !entity.match(/-single-form$/);
  });
};

// this resolver hooks directly into the DB to expose entities as raw arrays of objects
export const memoryResolver = async ({ name }: { name: string }) => {
  // create a new db connection
  const db = await DB.create({ kv: {}, name });
  // extract the kv store as entities
  const memEntities = db.kv;
  // return a proxy to expose the resolver for any request
  return new Proxy(memEntities as Record<string, unknown>, {
    get: async (target, key) => {
      // promise methods pass straight through
      if (key === "then" || key === "catch" || key === "finally") {
        return target[key];
      }

      // everything else is an entity held as a camelCased key in the entities - let supagraph handle resolution on the full set...
      return () => memEntities[toCamelCase(key as string)];
    },
  }) as Entities;
};

// Create and store the index of every entity according to the schema
export async function mongoIndexer({
  name,
  client,
  schema,
}: {
  name: string;
  client: Promise<MongoClient>;
  schema: SimpleSchema;
}) {
  // fetch the db connection
  const mongo = (await client).db(name || "supagraph");
  // we need to pull the metaTable from mongo
  const metaTable = mongo.collection("__meta__");

  // save all indexes to db
  return Promise.all(
    filteredKeys(schema).map(async (entity) => {
      // get the indexes for this entity
      const indexes = generateIndexes(schema, entity);
      // this collection of values
      const collection = mongo.collection(toCamelCase(entity));

      // find index entry in the __meta__ for this Entity
      const currentIndex = await metaTable.findOne<{ index: string }>({
        indexFor: entity,
      });

      // check if the index is upto date, if it isnt update it
      if (currentIndex?.index !== JSON.stringify(indexes)) {
        // create all the indexes and update the __meta__ entry for this entity
        await Promise.all(
          indexes.map(async (index) => {
            return collection.createIndex(index);
          })
        );
        // update the index marker for this entity (reindex if it changes)
        metaTable.updateOne(
          {
            indexFor: entity,
          },
          {
            $set: {
              index: JSON.stringify(indexes),
            },
          },
          {
            upsert: true,
          }
        );
      }
      return entity;
    })
  );
}

// Construct mongo resolver by connecting to db and constructing queries from the graphql ast on-the-fly
export function mongoResolver({
  name,
  client,
  mutable = false,
}: {
  name: string;
  client: Promise<MongoClient>;
  mutable?: boolean;
}) {
  // we return a schema resolver to map the entities against mongo resolvers (using the queries AST)
  return async (schema: SimpleSchema) => {
    // fetch the db connection
    const mongo = (await client).db(name || "supagraph");

    // store any changes in the schema against the index (this will be checked again after every build/compile in dev or after a revalidate)
    await mongoIndexer({ name, client, schema });

    // map the schema entities to a resolver function to pull the full query from mongodb
    return filteredKeys(schema).reduce((carr, entity) => {
      // define a resolver for this entity
      const resolver = async (
        _key: Key,
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        [_parent, _args, context, ast]: [EntityRecord, Args, any, any],
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        _operation: Operation
      ) => {
        // console.log("doing entity", entity, context?.result);
        // no result means we're on the root leaf of the query, extract all intel from the AST and query mongo for everything all-at-once...
        if (!context?.result) {
          // we walk the AST through all args and all fields and recreate a mongo query to request all the information required to satisfy the query the one request
          const query = createQuery(schema, entity, mutable, ast, context);
          // connect to the entity collection
          const collection = mongo.collection(toCamelCase(entity));
          // get the result from the complete query
          const result = collection.aggregate(query, {
            // allow sort on disk (we probably don't need this if we can live without the timewalk groupBy on block_ts_')
            allowDiskUse: true,
            // force sorts to use numericOrdering on number-like-id's (this matches the manual sorts we build in ./queries)
            collation: {
              locale: "en_US",
              numericOrdering: true,
            },
          });

          // extract all from cursor
          const arrResult = await result.toArray();

          // unwind the result (flatMap till we get all entries by collection name) and store the result into the context
          // so that subsequent queries can pull from the already queried result
          context.result = unwindResult(schema, entity, arrResult);
        }

        // if the context.result[entity] is present then we can return a result...
        // * Note that this is EVERY document of this entity type in the result, we need to post-filter to get to the correct result for the graphql response
        //   - this post filtering is the default behaviour of supagraph (it works directly against arrays of documents)
        return context?.result[entity] || [];
      };

      // disable limits on this resolver (we won't make any skips in the set)
      resolver.disableSkips = true;

      // return a resolver to gather the entity from the full AST
      return {
        ...carr,
        // set the mongo resolver for the entity...
        [entity]: resolver,
      };
    }, {} as Entities);
  };
}
