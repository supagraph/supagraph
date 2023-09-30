// Construct supagraph graphql server
import { createSupagraph, memoryResolver, mongoResolver } from "supagraph";

// Import mongo connection
import { getMongodb } from "@providers/mongoClient";

// Import the currect configuration
import config from "@supagraph/config";

// Revalidate this page every 12s (avg block time)
export const { revalidate } = config;

// Create a new supagraph handler
export const graphql = createSupagraph({
  // pass in the graphql schema
  schema: config.schema,
  // setup the entities (we're feeding in a mongo/mem-db driven store here - all queries are made at runtime directly against mongo in 1 request or are fully satisified from a local cache)
  entities:
    // in development mode...
    (process.env.NODE_ENV === "development" && config.dev) ||
    // or if the mongodb uri isnt set...
    !process.env.MONGODB_URI
      ? // for development we can keep entities in node-persist and share between connections
        memoryResolver({
          // name the database (in-memory/persisted on disk to .next dir)
          name: config.name,
        })
      : // for production/production-like we want to query mongo for results...
        mongoResolver({
          // name the database (this defines the db name for mongodb - changing the name will create a new db)
          name: config.name,
          // connect to mongodb
          client: getMongodb(process.env.MONGODB_URI),
          // if we want supagraph to produce a new entry for every event set this to `false`
          mutable: config.mutable,
        }),
  // set the default query to show in graphiql
  defaultQuery: config.defaultQuery,
  // set cacheControl header on response
  headers: {
    "Cache-Control": `max-age=${config.revalidate}, public, s-maxage=${config.revalidate}, stale-while-revalidate=${config.staleWhileRevalidate}`,
  },
});
