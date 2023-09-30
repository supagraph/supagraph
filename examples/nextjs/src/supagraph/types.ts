// All numeric values will be handled as BigNumbers
import type { BigNumber } from "ethers";

// Each sync will be provided its own provider
import type { BigNumberish } from "ethers/lib/ethers";

// Definitions for the TokensMigrated Events args (as defined in the abi)
export type TokensMigratedEvent = {
  to: string;
  amountSwapped: BigNumber;
  amountReceived?: BigNumber;
};

// Account entity definition
export type AccountEntity = {
  id: string;
  migratedMnt: BigNumberish;
  migrationCount: BigNumberish;
  blockNumber: BigNumberish;
  transactionHash: String;
};

// Delegation entity definition
export type MigationEntity = {
  id: string;
  account: string;
  amountSwapped: BigNumberish;
  gasCost: BigNumberish;
  blockTimestamp: BigNumberish;
  blockNumber: BigNumberish;
  transactionHash: string;
  refunded: boolean;
  refundTx: string;
};
