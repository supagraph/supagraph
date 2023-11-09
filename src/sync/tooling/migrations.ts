// Import required ethers types
import { providers, Event } from "ethers";

// Import persitence tooling to construct entities for migrations
import { TypedMapEntry } from "@/sync/tooling/persistence/typedMapEntry";
import { getEngine, Store, Entity } from "@/sync/tooling/persistence/store";

// Import network block handler to retrieve migration blocks to set against entities
import { getBlockByNumber } from "@/sync/tooling/network/fetch";

// Import required Migration and config types
import {
  Migration,
  SyncConfig,
  HandlerFn,
  SyncStage,
  SyncEvent,
} from "@/sync/types";
import {
  Block,
  TransactionReceipt,
  TransactionResponse,
} from "@ethersproject/abstract-provider";

// apply any migrations that fit in the event range
export const applyMigrations = async (
  migrations: Migration[],
  config: SyncConfig = {
    name: "supagraph",
    providers: {},
    events: {},
    contracts: {},
  },
  events: SyncEvent[] = []
) => {
  // retrieve the engine
  const engine = await getEngine();

  // check for migrations - we should insert an event for every item in the entity collection to run the callback
  if (migrations) {
    let migrationCount = 0;
    // check if we have any migrations to run in this range
    for (const migrationKey of Object.keys(migrations)) {
      // locate the migration
      const migration = migrations[migrationKey];
      // set the migrationKey into the migration
      migration.migrationKey = migrationKey;
      // set the latest blockNumber in place and move to the end
      if (migration.blockNumber === "latest") {
        // if we don't have a latest block or the chain isn't registered, do it now...
        if (!engine.latestBlocks[migration.chainId]) {
          if (config && config.providers) {
            engine.providers[migration.chainId] =
              engine.providers[migration.chainId] || [];
            engine.providers[migration.chainId][0] =
              engine.providers[migration.chainId][0] ||
              new providers.JsonRpcProvider(
                config.providers[migration.chainId].rpcUrl
              );
            // fetch the meta entity from the store (we'll update this once we've committed all changes)
            engine.latestEntity[migration.chainId] = await Store.get(
              "__meta__",
              `${migration.chainId}`
            );
            // make sure this lines up with what we expect
            engine.latestEntity[+migration.chainId].chainId =
              +migration.chainId;
            engine.latestEntity[migration.chainId].block = undefined;
          }
          // place current block as chainIds latestBlock
          if (engine.providers[migration.chainId][0]) {
            engine.latestBlocks[migration.chainId] = (await getBlockByNumber(
              engine.providers[migration.chainId][0],
              "latest"
            )) as unknown as Block; // engine will work with either definition
          }
        }
        // use the latest detected block and fix it so we don't apply latest again
        migration.blockNumber = engine.latestBlocks[migration.chainId].number;
      }
      // check this will be triggered here...
      if (
        migration.blockNumber >= engine.startBlocks[migration.chainId] &&
        migration.blockNumber <= engine.latestBlocks[migration.chainId].number
      ) {
        // assign the callback (masquarade this as valid for the context)
        engine.callbacks[
          `${migration.chainId}-migration-${migration.blockNumber}-${
            migration.entity || "false"
          }-${migration.migrationKey}`
        ] = migration.handler as unknown as HandlerFn;
        // if we're basing this on entity, then push an event for each entity
        if (migration.entity) {
          // this migration needs to be ran within this sync - start gathering the query results now
          const allEntries =
            migration.entity &&
            ((await engine.db.get(migration.entity)) as ({
              id: string;
            } & Record<string, unknown>)[]);

          // push a migration entry
          allEntries.forEach((entity) => {
            events.push({
              // insert as migration type
              type: "migration",
              // @ts-ignore
              data: {
                entity: new Entity<typeof entity>(migration.entity, entity.id, [
                  ...Object.keys(entity).map((key) => {
                    return new TypedMapEntry(key, entity[key]);
                  }),
                ]),
                // set the name for callback recall
                entityName: migration.entity,
                blockNumber: migration.blockNumber as number,
              } as unknown as Event,
              chainId: migration.chainId,
              number: migration.blockNumber as number,
              blockNumber: migration.blockNumber as number,
              migrationKey: migration.migrationKey,
              args: [],
              tx: {} as TransactionReceipt & TransactionResponse,
              // onMigration first
              txIndex: -2,
              logIndex: -2,
              collectBlock: true,
              collectTxReceipt: false,
            });
            // incr by one
            migrationCount += 1;
          });
        } else {
          events.push({
            // insert as migration type
            type: "migration",
            // @ts-ignore
            data: {
              blockNumber: migration.blockNumber as number,
            } as unknown as Event,
            chainId: migration.chainId,
            number: migration.blockNumber as number,
            blockNumber: migration.blockNumber as number,
            migrationKey: migration.migrationKey,
            collectBlock: true,
            collectTxReceipt: false,
            args: [],
            tx: {} as TransactionReceipt & TransactionResponse,
            // onMigration first
            txIndex: -2,
            logIndex: -2,
          });
          // incr by one
          migrationCount += 1;
        }
      }
      // clean up migrations after adding events (we can only use a migration once as it is associated with a blockNumber)
      delete migrations[migrationKey];
    }
    // if we're starting the process here then print the discovery
    if (
      !engine.flags.silent &&
      (!engine.flags.start ||
        (engine.flags.start &&
          SyncStage[engine.flags.start] < SyncStage.process)) &&
      migrationCount > 0
    ) {
      // detail everything which was loaded in this run
      console.log("Total no. migrations in range:", migrationCount, "\n\n--\n");
    }
  }
};
