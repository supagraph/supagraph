// We use BigNumber to handle all numeric operations
import { BigNumber, Contract, ethers } from "ethers";
import { getAddress } from "ethers/lib/utils";

// Import outer config to pull provider dets
import config from "@supagraph/config";

// Each event is supplied the block and tx along with the typed args
import {
  JsonRpcProvider,
  TransactionReceipt,
  TransactionResponse,
} from "@ethersproject/providers";

// Use Store to interact with entity storage and withDefault to type default values
import { Store, withDefault } from "supagraph";

// Import types for defined entities
import type { Entity } from "supagraph";
import type { DelegateEntity } from "@supagraph/types";

// Import tooling to check bloomFilters
import { isTopicInBloom } from "ethereum-bloom-filters";

// construct the provider once
const provider = new JsonRpcProvider(
  config.providers[withDefault(process.env.L2_MANTLE_CHAIN_ID, 5001)].rpcUrl
);

// Connect contract to fetch delegations
const delegatesInterface = new ethers.utils.Interface(config.events.tokenl2);

// get the l2 contract address once
const l2ContractAddress = getAddress(config.contracts.l2mantle.address);

// update the voter with changed state
const updateVoter = async (
  from: string,
  entity: Entity<DelegateEntity> & DelegateEntity,
  tx: TransactionReceipt & TransactionResponse,
  direction = 0
) => {
  // define token specific props
  const votesProp = "l2MntVotes";
  const balanceProp = "l2MntBalance";
  const otherVotesProps = ["mntVotes", "bitVotes"];
  // const otherBalanceProps = ["mntBalance", "bitBalance"];
  // check that this event isn't being handled by an event watcher
  const topic = delegatesInterface.getEventTopic("DelegateChanged");

  // only if the l2MntTo is set do we need to record l2 balance transfers (only record movements when we're not interacting with the delegation contract)
  if (
    entity.l2MntTo &&
    !(
      ((tx.contractAddress &&
        getAddress(tx.contractAddress) === l2ContractAddress) ||
        (tx.to && getAddress(tx.to) === l2ContractAddress)) &&
      isTopicInBloom(tx.logsBloom, topic)
    )
  ) {
    // get the voteRecipient so that we can add the value change now
    let voteRecipient =
      from === entity.l2MntTo
        ? entity
        : await Store.get<DelegateEntity>(
            "Delegate",
            getAddress(entity.l2MntTo)
          );

    // update sender if its not 0x0 address
    if (from !== "0x0000000000000000000000000000000000000000") {
      // get the balance before (this will only be applied if we already have the balance in the db)
      let newBalance = entity[balanceProp] || "0";
      let balanceBefore = entity[balanceProp] || "0";

      // if we havent recorded the balance already, then we need to run this through the DelegateChangedHandler first
      if ((balanceBefore && direction === 0) || direction === 1) {
        // get the current balance for this user as starting point (they should have one because they've made this tx)
        if (!entity[balanceProp]) {
          // get the balance for this user in this block (we don't need to sub anything if we get the fresh balance)
          newBalance = await provider.getBalance(from, tx.blockNumber);
        } else {
          if (direction === 0) {
            // // calculate gas usage (we need to introduce a gas oracle to use this correctly because it does not include l1 rollup fees)
            // const gasUsed = BigNumber.from(tx.gasUsed);
            // const gasPrice = BigNumber.from(tx.effectiveGasPrice || tx.gasPrice);
            // const gasCostInWei = gasUsed.mul(gasPrice);
            // // get new balance for sender (sub the value and gas-spent)
            // newBalance = BigNumber.from(newBalance)
            //   .sub(tx.value)
            //   .sub(gasCostInWei);
            newBalance = await provider.getBalance(from, tx.blockNumber);
          } else {
            // add the new value to the old balance
            newBalance = BigNumber.from(newBalance).add(tx.value);
          }
        }

        // set new balance value for sender
        entity.set(balanceProp, newBalance);

        // // sum all the balances for sender
        // entity.set(
        //   "balance",
        //   BigNumber.from(newBalance).add(
        //     otherBalanceProps.reduce((sum, otherBalanceProp) => {
        //       return sum.add(BigNumber.from(entity[otherBalanceProp] || "0"));
        //     }, BigNumber.from("0"))
        //   )
        // );

        // update pointers for lastUpdate
        entity.set("blockNumber", +tx.blockNumber);
        entity.set("transactionHash", tx.hash || tx.transactionHash);

        // save the changes
        entity = await entity.save();

        // if the entity is the recipient, make sure we don't lose data
        if (from === entity.l2MntTo) {
          voteRecipient = entity;
        }

        // get the corrected votes for this situation
        const newL2Votes = BigNumber.from(voteRecipient[votesProp] || "0")
          .sub(balanceBefore)
          .add(newBalance || "0");

        // update the votes for l2
        voteRecipient.set(votesProp, newL2Votes);

        // votes is always a sum of all vote props
        voteRecipient.set(
          "votes",
          newL2Votes.add(
            otherVotesProps.reduce((sum, otherVotesProp) => {
              return sum.add(
                BigNumber.from(voteRecipient[otherVotesProp] || "0")
              );
            }, BigNumber.from("0"))
          )
        );

        // save the changes
        voteRecipient = await voteRecipient.save();
      }

      // if the entity is the recipient, make sure we don't lose data
      if (from === entity.l2MntTo) {
        entity = voteRecipient;
      }
    }
  }
  return entity;
};

// Handler to consume Transfer events for known delegates
export const TransactionHandler = async (
  _: unknown,
  { tx }: { tx: TransactionReceipt & TransactionResponse }
) => {
  // console.log("transfer: from", args.from, "to", args.to, "for", args.value.toString());

  // load the entities involved in this tx
  let entity = await Store.get<DelegateEntity>("Delegate", getAddress(tx.from));

  // update sender (if they are delegating)
  entity = await updateVoter(tx.from, entity, tx, 0);

  // check the recipient
  const recipient =
    tx.to &&
    (tx.to === tx.from
      ? entity
      : await Store.get<DelegateEntity>("Delegate", getAddress(tx.to)));

  // update the recipient (if they are delegating)
  if (tx.to) await updateVoter(tx.to, recipient, tx, 1);
};
