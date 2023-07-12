# Supagraph

This repo contains a utility toolkit to index any RPC using subgraph-like mapping definitions backed by MongoDB.

The constructed `Entities` are made available in the background to a schema-driven `graphql-yoga` instance via resolvers.

## Tech Stack

We are depending on:

- `graphql-yoga` to create a GraphQL endpoint
- `ethers` to map `Events` to stored `Entities`
- `mongodb`/`node-persist` as a persistence layer
- `typescript`, `eslint`, and `prettier` to maintain coding standards

## Usage

### Getting started

To get started with `supagraph`, follow these steps:

1. Install the required dependencies:

   ```bash
   $ pnpm add supagraph
   ```

### GraphQL:

1. Construct a schema that will be 1:1 mapped against your `sync` handler entities:

   ```typescript
   const schema = `
     # id = \`\${owner}\`
     type Name @entity {
       id: ID!
       name: String
       owner: Bytes
       number: BigNumber
     }
   `;
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

2. If you're using `supagraph` to construct static `GraphQL` endpoints, you can call `createSupagraph` and supply a raw mapping of arrays (`{[EntityName]: []}`) as the `entities` prop, otherwise, you might want to use a `resolver` to read from a persistence layer:

   ```typescript
   import { createSupagraph, memoryResolver, mongoResolver } from "supagraph";

   import * as http from "http";

   const supagraph = createSupagraph({
     schema,
     entities:
       // in development mode...
       (process.env.NODE_ENV === "development" && SUPAGRAPH_DEV_ENGINE) ||
       // or if the mongodb uri isnt set...
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
     graphqlEndpoint: `graphql`, // the absolute path which will proxy supagraph.GET()/.POST() requests
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

   const server = http.createServer(supagraph);

   server.listen(4001, () => {
     console.info("Server is running on http://localhost:4001/graphql");
   });
   ```

3. Start `supagraph` with node:

   ```bash
   $ node ./[filename].js
   ```

4. Or expose it as a `nextjs` endpoint:

   ```typescript
   import type { NextApiRequest, NextApiResponse } from "next";

   export default createSupagraph<NextApiRequest, NextApiResponse>({
     schema,
     entities,
     graphqlEndpoint: `graphql`, // this _must_ match the current route
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

5. Start `nextjs` as normal:

   ```bash
   $ yarn dev
   ```

### Sync

To create a new `supagraph syncOp[]` handler and keep the `supagraph` instance up to date, follow these steps:

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
   const handler = addSync<{ name: string; number: string }>(
     CONTRACT_EVT,
     CHAINS_PROVIDER,
     CONTRACT_ABI,
     CONTRACT_ADDRESS,
     async ({ name, number }, { tx, block }) => {
       // Code for handling the sync operation
     }
   );
   ```

4. Retrieve an `entity` from the `Store` and update its properties:

   ```typescript
   const handler = addSync<{ name: string; number: string }>(
     CONTRACT_EVT,
     CHAINS_PROVIDER,
     CONTRACT_ABI,
     CONTRACT_ADDRESS,
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

6. The simplest way to keep a Vercel-hosted `supagraph` instance up to date is to set a `cron` job in the project's `vercel.json` config to call the `${sync_get_route}` every minute (`reqs per day` === `syncOps * 1440`):

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

   Preview deployments [will not run cron-jobs](https://vercel.com/guides/how-to-setup-cron-jobs-on-vercel#deploy-your-project). Instead we can register an external cron with a service like [cron-job.org](cron-job.org) to invoke the `${sync_get_route}` against the same schedule\*.

   \*Note: the `db` will lock between `sync()` operations to enforce idempotent processing, therefore we only need to register one cron-job for each unique `supagraph` instance (by `name`).

## Contributing

If you would like to contribute to `supagraph`, please follow these steps:

1. Fork this repository.
2. Create a new branch for your changes.
3. Make your changes and test them thoroughly.
4. Create a pull request and describe your changes.

## Support

Support can be found on our Discord channel [#supagraph](https://discord.gg/ryxy6eA6Dv)
