# Supagraph

This directory contains documentation explaining the core pieces of `supagraph/src/sync` and how to use it.

## Getting started

To get started with `supagraph`, follow these steps:

1. Install the required dependencies:

   ```bash
   $ pnpm add supagraph
   ```

## Sync

Syncs are the heart of `supagraph` as an indexer, we use syncs to handle the processing of an `event` as we see it happen onchain.

`Supagraph` is configured to run these syncs periodically in a "catch-up" fashion to allow RPC usage to be both deterministic and configurable.

Some layer2 chains are producing a huge number of blocks and its getting more and more expensive to obtain a full history in a reasonable time-frame, to ease this pain, `supagraph` offers several options to cut down on the amount of data we need to obtain in order to produce our data-store, but these are wholly dependent on the use-case and carry significant trade-offs, we explain more about these options in the sections below.

### What exactly are we handling?

We're handling EVM events, events are emitted as `logs` when an EVM contract emits an event. The details of `log` messages are limited to just the content of the emitted message and a small amount of supplementary information pertaining to the transaction (the block it was emitted in and the transaction responsible for producing it etc...).

If we want to gather more information than just the event `log` content (such as the `tx.from` or the `block.timestamp`) then we need to opt-in to pulling the `tx` and `block` from our RPC provider (see section on ["Syncing"](#syncing)), by opting-in to collecting this data, we're significantly increasing our RPC usage so its always useful to ask yourself if you can get-by with just the log content.

### What does a handler look like?

A basic handler has the following structure:

```typescript
const handler = addSync<types_for_emitted_log_message>({
  eventName: CONTRACT_EVT,
  address: CONTRACT_ADDRESS,
  provider: CHAINS_PROVIDER,
  eventAbi: CONTRACT_ABI,
  startBlock: CONTRACT_START_BLOCK,
  onEvent: async (args, { tx, block }) => {
    // Code for handling the sync operation
  }
);
```

Breaking that down, we're supplying a generic type (`types_for_emitted_log_message`) to `addSync<T>(...)`, this type matches the event signature defined in the `CONTRACT_ABI` for this `CONTRACT_EVT`.

We then supply a `Sync` object as the only parameter to `addSync()`. The `Sync` object defines where we're going to get the event data from and how we're going to process it.

If this database is fresh then the `Sync` will begin from the provided `CONTRACT_START_BLOCK`, otherwise we'll pick up where we left off on the last sync and pull any new events that have been emitted since.

Each `log` discovered in a `sync()` will be passed through the `onEvent()` handler, this function is supplied the `args` (typed according to the `addSync` generic) parsed from the `log` data along with some `tx` and `block` data. The extent of how much `tx` and `block` data depends on the configuration we feed through the `sync()` method (see section on ["Syncing"](#syncing)), if we want to include the full `tx` and `block` data, then we need to opt-in by feeding the appropriate options (`{ skipBlocks: false, skipTransactions: false, skipOptionalArgs: false }`) when calling `sync(options)`.

### How can I find the correct information to feed to an `addSync` handler?

To create a handler we need the following:

1. `eventName`: The name of the event we want to retrieve `logs` for on the given contract
2. `address`: The address of the contract we want to retrieve `logs` for
3. `provider`: An `ethers.Providers.JsonRpcProvider` for the RPC we want to pull data from
4. `eventAbi`: A `full-abi` or a `human-readable-abi`, it can also be partial or complete but it must contain the ABI details for the given `eventName`
5. `startBlock`: The block our syncing should start from - if syncing has already started this will be ignored

### What should I do in my handler?

A handler is just typescript, so we can do pretty much whatever we like with the data we see, but the default expectation is for us to define and create `Entities` via the `Store`.

The `Store` is a static construct available from the root level of `supagraph`:

```typescript
import { Store } from "supagraph";
```

To use the `Store` we:

1. first `.get<T>()` the entity by `ref` and `key`:

   ```typescript
   // Get the Entity and hydrate from value store
   const entity1 = await Store.get<{
     a: string;
   }>("EntityName", "key-1");

   // Get without hydrating from value store
   const entity2 = await Store.get<{
     a: string;
   }>("EntityName", "key-2", true);

   entity1.a; // possibly defined
   entity2.a; // undefined
   ```

2. Now that we have an entity we can modify it:

   ```typescript
   entity1.set("a", "new-a-value");
   ```

3. Finally to save:

   ```typescript
   await entity1.save();
   ```

### Setup

To use the `Store` with a more permanent db backing, we might want to register an alternative engine before we start calling `sync()`:

```typescript
import { DB, Mongo, Store } from "supagraph";

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

### Syncing

So far, we have defined our `Sync` operations and set-up our `engine` to handle persisting the state `Store` of entities we build up, now we need to run everything.

We do that using the `sync` method exposed at the root level of `supagraph`:

```typescript
import { sync } from "supagraph";
```

The `sync()` method accepts an optional set of options:

```typescript
sync: (options: {
  start?: keyof typeof Stage | false;
  stop?: keyof typeof Stage | false;
  skipBlocks?: boolean;
  skipTransactions?: boolean;
  skipOptionalArgs?: boolean;
}) => Promise<SyncSummary>;
```

1. `start`: Which `Stage` of the process do we start syncing from? (`"events"`, `"blocks"`, `"transactions"`, `"sort"`, `"process"`)
2. `stop`: Which `Stage` of the process do we stop syncing? (`"events"`, `"blocks"`, `"transactions"`, `"sort"`, `"process"`)
3. `skipBlocks`: Skip collecting the `block` data for the `logs` we discover
4. `skipTransactions`: Skip collecting the `tx` data for the `logs` we discover
5. `skipOptionalArgs`: Don't attempt to provide the full `tx`/`block` as `args` to the `handler` (use a lighter version which can be built from the `log` data)

The first two options (start and stop) allow us to initiate partial runs and save any discovered `events`/`blocks`/`txs` to disk. This can be useful for situations where we might want to make many changes to the handlers, but we won't be changing the contract data, we can run through everything once then on subsequent runs, move the start position to `"process"` to avoid fetching all the event data again.

We're also able to construct our entity collections much quicker if we supply an option set which doesn't collect `tx` or `block` information at all, but there are few things we should be aware of;

- All events from all handlers are sorted by block prior to processing to ensure we approach them in the correct order.
- If we are collecting blocks from multiple chains and the order that events are processed matters for the resultant `Entity`, we must use `skipBlocks = false` to collect all `blocks` enabling us to sort by `block.timestamp`
- If we use `skipTransactions`, we're unable to say which address the `log` relates to, this is okay in situations like `erc20` tokens, because they emit a `from` address in the `log` data.

Finally we need to think about how often we call this `sync` method, it would never make sense to call it more frequently than the average block time, but there really is no bound for how infrequently we call it.

Fewer calls of the `sync` method means fewer requests in total, but the number of `logs`/`blocks`/`txs` is a constant regardless of how often we make the sync, so the frequency is really driven by our appetite for staleness.

### How does the sync work?

The `sync()` method takes every `Sync` operation you have constructed with `addSync<T>()` and constructs a set of `Providers` you are using in all of your `Syncs` to establish appropriate `StartBlocks` to use for the `queryFilter` calls we're about to make. If this is the first time we're running `sync()` with a fresh data-store, we will default to the provided `Sync.startBlock`, if we've previously ran a `sync()` then the `StartBlock` will be pulled from the `__META__` table in the form of the `latestBlock` we saw for this `Provider.network.chainId`.

Once we've established where we're going to start our `queryFilter` from, we run it using the `eventAbi` and `eventFn` defined in the `Sync`. Most RPC's will limit the number of `logs` a single `queryFilter` request can return, so `supagraph` will attempt the queries using a `bisect` approach (we'll divide the block-range in two and try again) to eventually pull the full range of events between the `StartBlock` and the `latest` block. This requesting of `logs` constitutes the `"events"` `Stage` and if we call sync with a `stop` here (`sync({ stop: "events"})`) we would pull the `logs`, temporarily storing them to disk, and end the processing there.

The next steps are `"blocks"` and `"transactions"` - these two are ran sequentially and you can stop between the two (or skip them entirely) - but they perform very similar actions. They will request every `block` or `transactionReceipt` for every discovered `log` event we found in the previous step, they will extract the `timestamp` or `from` and add those details to the event data directly for easy access, and store the full `block`/`transactionReceipt` onto disk so that it can be used in the `Sync.onEvent()` handlers.

Next we `"sort"` the `logs`. As the `logs` are pulled for each handler then combined, these `logs` need to be sorted into a sensible approach order. If we collected all of the `blocks` we can use the `block.timestamp` which will give us the most reliable `multi-chain` sort. However if we don't have the `blocks` the best we can do is sort on is `log.blockNumber` and `log.transactionIndex` (this is really only safe to do when indexing a single chain).

The final step is to `"process"` all of the `logs` through the appropriate `Sync.onEvent()` handler. If we have already ran the sync with a `{ stop: "sort" }`, everything will be fully cached in the `tmp` directory and we can `start` the `sync()` from `"process"` (`sync({ start: "process"})`).

After the process has completed and after all new `Entity` data has been `bulk` written to the `db`, we will finally update the pointers to correspond with the true `latestBlock` for each provider. This true `latestBlock` reflects the `blockNumber` of the final `log` in the discovered events array for each `Provider.network.chainId`.

## Support

Support can be found on our Discord channel [#supagraph](https://discord.gg/ryxy6eA6Dv)
