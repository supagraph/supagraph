// We use BigNumber to handle all numeric operations
import { BigNumber } from "ethers";

// Use Store to interact with entity storage
import { Store } from "supagraph";

// Each event is supplied the block and tx along with the typed args
import { Block, TransactionReceipt } from "@ethersproject/providers";

// - These types will be generated based on the event signatures exported by the defined contracts in config (coming soon TM);
import type {
  AccountEntity,
  MigationEntity,
  TokensMigratedEvent,
} from "../types";

// Generic handler to consume TokensMigrated events
export const TokensMigratedHandler = async (
  args: TokensMigratedEvent,
  { tx, block }: { tx: TransactionReceipt; block: Block }
) => {
  // load the entity for this account and migration (migration based on txHash)
  const account = await Store.get<AccountEntity>("Account", args.to);
  const migration = await Store.get<MigationEntity>(
    "Migration",
    tx.transactionHash
  );

  // add the migration to users migratedMnt total
  const newBalance = BigNumber.from(account.migratedMnt || "0").add(
    args.amountSwapped
  );
  // keep count of how many migrations the user has made (so we can paginate Migrations)
  const newCount = BigNumber.from(account.migrationCount || "0").add("1");

  // calculate gas usage
  const gasUsed = BigNumber.from(tx.gasUsed);
  const gasPrice = BigNumber.from(tx.effectiveGasPrice);
  const gasCostInWei = gasUsed.mul(gasPrice);

  // update/set the accounts details
  account.set("migratedMnt", newBalance);
  account.set("migrationCount", newCount);
  account.set("blockNumber", tx.blockNumber);
  account.set("transactionHash", tx.transactionHash);

  // add a migration for each event (recording all relevant details from the args, tx and block)
  migration.set("account", args.to);
  migration.set("amountSwapped", args.amountSwapped);
  migration.set("blockNumber", tx.blockNumber);
  migration.set("blockTimestamp", block.timestamp);
  migration.set("transactionHash", tx.transactionHash);

  // record gas-cost for future refund
  migration.set("gasCost", gasCostInWei);

  // save all changes
  await migration.save();
  await account.save();
};
