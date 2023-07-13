# Supagraph

This repo contains a utility toolkit to index any `RPC` using `subgraph-like` mapping definitions backed by `MongoDB`/`node-persist`.

The constructed `Entities` are made available in the background to a schema-driven `graphql-yoga` instance via resolvers.

## Tech Stack

We are depending on:

- `graphql-yoga` to create a GraphQL endpoint
- `ethers` to map `Events` to stored `Entities`
- `mongodb`/`node-persist` as a persistence layer
- `typescript`, `eslint`, and `prettier` to maintain coding standards

## Documentation

- **Sync**: Read the `supagraph/src/sync` docs [here](./docs/sync/README.md)
- **GraphQL**: Read the `supagraph/src/graphql` docs [here](./docs/graphql/README.md)

## Usage

`Supagraph` has been broken into two core stand alone pieces, `sync` and `graphql`, you might want to run either or both in order to index and/or query your data.

### Getting started

To get started with `supagraph`, follow these steps:

1. Install the required dependencies:

   ```bash
   $ pnpm add supagraph
   ```

## Indexing contracts with supagraph sync

To create a new supagraph `syncOp[]` handler and keep the `supagraph` data `Store` up to date, follow these steps:

1. Import the necessary functions:

   ```typescript
   import { addSync, DB, Mongo, Store, sync } from "supagraph";
   ```

2. Configure a `db` engine to back the `Store`:

   ```typescript
   // switch out the engine for development to avoid the mongo requirement locally
   Store.setEngine({
     // name the connection
     name: SUPAGRAPH_NAME,
     // db is dependent on state
     db:
       // in production/production like environments we want to store mutations to mongo otherwise we can store them locally
       process.env.NODE_ENV === "development" && SUPAGRAPH_DEV_ENGINE
         ? // connect store to in-memory/node-persist store
           DB.create({ kv: {}, name: SUPAGRAPH_NAME })
         : // connect store to MongoDB
           Mongo.create({
             kv: {},
             name: SUPAGRAPH_NAME,
             mutable: SUPAGRAPH_MUTABLE_ENTITIES, // if we want supagraph to produce a new entry for every event set this to `false`
             client: getMongodb(process.env.MONGODB_URI!), // getMongodb should return a MongoClient instance (imported from mongodb)
           }),
   });
   ```

3. Create a new `syncOp[]` handler using `addSync()`:

   ```typescript
   const handler = addSync<T>(
     CONTRACT_EVT,
     CHAINS_PROVIDER,
     CONTRACT_START_BLOCK,
     CONTRACT_ADDRESS,
     CONTRACT_ABI,
     EVENT_HANDLER
   );
   ```

4. Retrieve an `entity` from the `Store` and update its properties inside an event handler:

   ```typescript
   const handler = addSync<{ name: string; number: string }>(
     CONTRACT_EVT,
     CHAINS_PROVIDER,
     CONTRACT_START_BLOCK,
     CONTRACT_ADDRESS,
     CONTRACT_ABI,
     // handler code...
     async ({ name, number }, { tx, block }) => {
       let entity = await Store.get<{
         id: string;
         name: string;
         number: string;
         owner: string;
       }>("Name", tx.from);

       entity.set("name", name);
       entity.set("number", number);
       entity.set("owner", tx.from);

       entity = await entity.save();
     }
   );
   ```

5. Call the `sync()` method in the `GET` handler of a nextjs `route.ts` document (or by any other means if running `supagraph` in a different environment):

   ```typescript
   export async function GET() {
     const summary = await sync(); // await all new events to be processed and stored from all sync operations

     // Code for handling the response
   }
   ```

   To keep a Vercel-hosted `supagraph` instance up to date, set a `cron` job in the project's `vercel.json` config to call the `${sync_get_route}` every minute (`reqs per day` === `syncOps * 1440`):

   ```
   # vercel.json
   {
     "crons": [
       {
         "path": `${sync_get_route}`,
         "schedule": "* * * * *"
       }
     ]
   }
   ```

   [Preview deployments will not run cron-jobs](https://vercel.com/guides/how-to-setup-cron-jobs-on-vercel#deploy-your-project). Instead we can register an external cron with a service like [cron-job.org](cron-job.org) to invoke the `${sync_get_route}` against the same schedule\*.

   \*Note: the `db` will lock between `sync()` operations to enforce idempotent processing, therefore we only need to register one cron-job for each unique `supagraph` instance (by `name`).

## Querying indexed data with GraphQL

1. Construct a `schema` which will be mapped 1:1 against the `entities` you supply in the config:

   ```typescript
   // Schemas follow GraphQL Schema Definition Language (SDL)
   const schema = `
     # id = \`\${owner}\`
     type Name @entity {
       id: ID!
       name: String
       owner: Bytes
       number: BigNumber
     }
   `;

   // Entities must eventually resolve to an object of named entity arrays
   const entities = {
     Name: [
       {
         id: "0",
         name: "grezle",
         owner: "0x0...",
         number: "0848293943825030",
       },
     ],
   };
   ```

2. When using `supagraph` to construct `GraphQL` endpoints from static data, `createSupagraph` can be supplied a raw mapping of arrays (`{[EntityName]: []}`) as the `entities` prop, otherwise, `entities` should be supplied as a valid `resolver`:

   ```typescript
   // import graphql factory and resolvers
   import { createSupagraph, memoryResolver, mongoResolver } from "supagraph";

   // construct the graphql server instance
   const supagraph = createSupagraph({
     // pass the schema (as a string)
     schema,
     // pass the entities as a resolver
     entities:
       // in development mode...
       (process.env.NODE_ENV === "development" && SUPAGRAPH_DEV_ENGINE) ||
       // or if the mongodb uri isn't set...
       !process.env.MONGODB_URI
         ? // for development we can keep entities in node-persist and share between connections
           memoryResolver({
             // name the database (in-memory/persisted on disk to .next dir)
             name: SUPAGRAPH_NAME,
           })
         : // for production/production like we want to query mongo for results...
           mongoResolver({
             // name the database (this defines the db name for mongodb - changing the name will create a new db)
             name: SUPAGRAPH_NAME,
             // connect to mongodb
             client: getMongodb(process.env.MONGODB_URI), // getMongodb should return a MongoClient instance (imported from mongodb)
             // if we want supagraph to produce a new entry for every event set this to `false`
             mutable: SUPAGRAPH_MUTABLE_ENTITIES,
           }),
     // the absolute path which will proxy supagraph.GET()/.POST() requests
     graphqlEndpoint: `graphql`,
     // the default query supplied to graphiql
     defaultQuery: `
       {
         names {
           id
           name
           number
           owner
         }
       }`,
   });
   ```

3. The `supagraph` instance is a `http` compatible server implementation and can be exposed wherever the `GET`/`POST` - `request`/`response` interface is implemented, for example;

   - As a `nodejs` `http` sever

     ```typescript
     import * as http from "http";

     ...

     // create a http server instance
     const server = http.createServer(supagraph);

     // bind to port
     server.listen(4001, () => {
       console.info("Server is running on http://localhost:4001/graphql");
     });
     ```

     Start with:

     ```bash
     $ node ./[filename].js
     ```

   - Or as a `nextjs13` `route.ts` endpoint:

     ```typescript
     import type { NextRequest, NextResponse } from "next";

     ...

     // GET will always return the graphiql user interface...
     export async function GET(request: NextRequest) {
       try {
         // get html response
         const response = await graphql(request);

         // return via nextjs
         return new NextResponse(response.body, {
           status: response.status,
           headers: response.headers,
         });
       } catch (e) {
         // eslint-disable-next-line no-console
         console.log(e);
       }
     }

     // POST methods exclusively accept and return JSON bodies...
     export async function POST(request: NextRequest) {
       try {
         // get json response
         const response = await graphql(request);

         // process the graphql response
         return new NextResponse(JSON.stringify(await response.json()), {
           status: response.status,
           headers: response.headers,
         });
       } catch (e) {
         // eslint-disable-next-line no-console
         console.log(e);
       }
     }
     ```

     Start with:

     ```bash
     $ pnpm dev -or- pnpm build && pnpm start
     ```

## Contributing

If you would like to contribute to `supagraph`, please follow these steps:

1. Fork this repository.
2. Create a new branch for your changes.
3. Make your changes and test them thoroughly.
4. Create a pull request and describe your changes.

## Support

Support can be found on our Discord channel [#supagraph](https://discord.gg/ryxy6eA6Dv)
