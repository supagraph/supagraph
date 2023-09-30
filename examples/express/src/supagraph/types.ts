// All numeric values will be handled as BigNumbers
import type { BigNumber } from "ethers";

// Definitions for the Approval Events args (as defined in the abi)
export type ApprovalEvent = {
  owner: string;
  spender: string;
  value: BigNumber;
};

// Definitions for the DelegateChanged Events args (as defined in the abi)
export type DelegateChangedEvent = {
  delegator: string;
  fromDelegate: string;
  toDelegate: string;
};

// Definitions for the DelegateVotesChanged Events args (as defined in the abi)
export type DelegateVotesChangedEvent = {
  delegate: string;
  previousBalance: BigNumber;
  newBalance: BigNumber;
};

// Definitions for the Initialized Events args (as defined in the abi)
export type InitializedEvent = {
  version: BigNumber;
};

// Definitions for the MintCapNumeratorChanged Events args (as defined in the abi)
export type MintCapNumeratorChangedEvent = {
  from: string;
  previousMintCapNumerator: BigNumber;
  newMintCapNumerator: BigNumber;
};

// Definitions for the OwnershipTransferred Events args (as defined in the abi)
export type OwnershipTransferredEvent = {
  previousOwner: string;
  newOwner: string;
};

// Definitions for the Transferred Events args (as defined in the abi)
export type TransferEvent = {
  from: string;
  to: string;
  value: string;
};

// Delegate entity definition
export type DelegateEntity = {
  id: string;
  mntTo: string;
  bitTo: string;
  l2MntTo: string;
  // @DEPRECATED - balance checks are expensive to maintain - we don't want to record ALL L2 balances, so we opt to store none
  // balance: BigNumber;
  // mntBalance: BigNumber;
  // bitBalance: BigNumber;
  l2MntBalance: BigNumber;
  votes: BigNumber;
  mntVotes: BigNumber;
  bitVotes: BigNumber;
  l2MntVotes: BigNumber;
  delegatorsCount: BigNumber;
  mntDelegatorsCount: BigNumber;
  bitDelegatorsCount: BigNumber;
  l2MntDelegatorsCount: BigNumber;
  blockNumber: number;
  transactionHash: string;
};
