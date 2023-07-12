# Supagraph

This directory contains documentation explaining the core pieces of `supagraph/dist/graphql` and how to use it.

## Getting started

To get started with `supagraph`, follow these steps:

1. Install the required dependencies:

   ```bash
   $ pnpm add supagraph
   ```

## GraphQL

`Supagraph` provides a minimal configuration setup to rollout `subgraph` flavoured `graphql-yoga` servers over the top of our `supagraph syncs` data `Store`.

### What are entities?

- In `supagraph`, entities represent individual objects in your data model. Each entity corresponds to a collection or a similar data construct.

### What is a resolver?

- Resolvers are responsible for fetching data from your data sources and mapping them to GraphQL queries. Resolvers define the logic for how GraphQL fields are resolved and what data they return. `Supagraph` ships with two resolvers, `memoryResolver` and `mongoResolver`, `memoryResolver` will persist your data locally to a tmp directory (inside the projects `.next` dir) and `mongoResolver` will persist your data into the provided `mongodbClient` instance.

### What is a schema?

- Schema's define the structure of your GraphQL API. They specify the available entities, their properties and any joins that they might have.

### How do I define my schema?

- You can define your schema using a stripped down version of GraphQL Schema Definition Language (SDL) much like `subgraph`. Here we only need to define entities, types and any derived fields.

  ```typescript
  const schema = `
    # id = \`\${owner}\`
    type Name @entity {
      id: ID!
      name: String!
      owner: Bytes!
      number: BigNumber!
    }
  `;
  ```

### How do I represent joins?

- In `supagraph`, you can represent joins between entities using GraphQL relationships. By defining relationships in your schema and resolving them in resolvers, we can fetch related data and perform complex queries involving multiple entities. For example, if we wanted each `Name` entity to have multiple `Numbers`, we could create a one-to-many join like so:

  ```typescript
  const schema = `
    # id = \`\${owner}\`
    type Name @entity {
      id: ID!
      name: String!
      owner: Bytes!
      number: [Number!]! @derivedFrom(field: "owner")
    }
    
    # id = \`\${number}\`
    type Number @entity {
      id: ID!
      owner: Bytes!
      number: BigNumber!
    }
  `;
  ```

### How do I setup `supagraphs` GraphQL server?

- To set up a `supagraph` GraphQL server, you need to provide your `schema`, `resolver`, and any necessary configuration `options`. This can be done using the `createSupagraph` function provided by `supagraph`.

  ```typescript
  import { createSupagraph, memoryResolver, mongoResolver } from "supagraph";

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
  });
  ```

### Can I personalize "Supagraph Playground"?

- Yes, you can personalize the `Supagraph Playground` by providing a custom `icon`, `title` and `defaultQuery`:

  ```typescript
  import { createSupagraph } from "supagraph";

  const supagraph = createSupagraph({
    title: "Supagraph Playground",
    icon: "/supagraph.png",
    defaultQuery: `
      {
        names {
          id
          name
          number
          owner
        }
      }
    `,
  });
  ```

### How do I expose `supagraphs` GraphQL server?

1. We call `createSupagraph` and configure our `schema`, `resolver` and set our entry point (`graphqlEndpoint`), then we can expose `supagraph` as a server:

   ```typescript
   import { createSupagraph, memoryResolver } from "supagraph";

   import * as http from "http";

   const supagraph = createSupagraph({
     schema,
     entities,
     graphqlEndpoint: `graphql`, // this _must_ match the current route
   });

   const server = http.createServer(supagraph);

   server.listen(4001, () => {
     console.info("Server is running on http://localhost:4001/graphql");
   });
   ```

   To start `supagraph`, we would run this script with `nodejs`:

   ```bash
   $ node ./[filename].js
   ```

2. Alternatively we can expose a `supagraph` GraphQL server as a `nextjs` endpoint:

   ```typescript
   import { createSupagraph, memoryResolver } from "supagraph";

   import type { NextApiRequest, NextApiResponse } from "next";

   const supagraph = createSupagraph<NextApiRequest, NextApiResponse>({
     schema,
     entities,
     graphqlEndpoint: `graphql`, // this _must_ match the current route
   });

   // GET will always return the graphiql user interface...
   export async function GET(request: NextRequest) {
     try {
       // get html response
       const response = await supagraph(request);

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
       const response = await supagraph(request);

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

   Start `nextjs` as normal:

   ```bash
   $ yarn dev
   ```

### Can I add authentication and other headers?

- Yes, you can add authentication and other headers to your GraphQL requests. For example to authenticate your requests, include an `Authorization` header with an appropriate token. You can also include any other header as needed for your API operations.

  ```json
  {
    "Authorization": "Bearer YOUR_TOKEN"
  }
  ```

## Error Handling

If an error occurs during a request, the response will include an `errors` field with details about the error. You can handle errors by checking the presence of this field in the response.

## Support

Support can be found on our Discord channel [#supagraph](https://discord.gg/ryxy6eA6Dv)
