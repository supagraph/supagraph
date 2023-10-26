# Supagraph

Supagraph is an open-source toolkit for building efficient cross-chain indexes from onchain data.

## Features

- ✅ - Index multiple chains with a single service
- ✅ - `onBlock` / `onTransaction` and log `event` handlers in typescript (with side-effects)
- ✅ - Fast initial load and reloads using `queryFilters` and a local cache
- ✅ - Handle `async` messages in the `sync` process to be awaited later (non-blocking `async` handling of sequential processes)
- ✅ - Sync as a daemon or on a schedule
- ✅ - Support for migrations, factory contracts and recovery scripts
- ✅ - Support for cron based scheduling
- ✅ - Easy to deploy anywhere with minimal configuration
- ✅ - Efficient `graphql` -> `mongo` querying for cheap data storage and retrieval
- ✅ - Immutable storage option to snapshot state at any block (immutable by default)
- 🏗️ - Code / type generators and subgraph migration guides
- 🏗️ - Postgres / other database adapters and resolvers
- 🏗️ - Dashboard ui with sync monitoring and admin controls
- 🏗️ - Chain reconciles and reorganisations handling

## Installation

To install `supagraph`, go to an empty folder, initialise a pnpm project (i.e. `pnpm init`), and run:

  ```bash
  $ pnpm add supagraph
  ```

## Documentation

`Supagraph` has been broken into two core stand alone pieces, `sync` and `graphql`, you might want to run either or both in order to index and/or query your data.

- **Sync**: Read the `supagraph/src/sync` docs [here](./docs/sync/README.md)
- **GraphQL**: Read the `supagraph/src/graphql` docs [here](./docs/graphql/README.md)

## Tech Stack

We are depending on:

- `graphql-yoga` and `graphql` to create a GraphQL endpoint
- `ethers` to map `Events` to stored `Entities`
- `mongodb`/`node-persist` as a persistence layer
- `typescript`, `eslint`, and `prettier` to maintain coding standards
- `ttsc` and `typescript-transform-paths` for comp

## Contributing

If you would like to contribute to `supagraph`, please follow these steps:

1. Fork this repository.
2. Create a new branch for your changes.
3. Make your changes and test them thoroughly.
4. Create a pull request and describe your changes.

## Support

Support can be found on our Discord channel [#supagraph](https://discord.gg/ryxy6eA6Dv)
