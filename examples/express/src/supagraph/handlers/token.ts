// We use BigNumber to handle all numeric operations
import { BigNumber } from "ethers";
import { getAddress } from "ethers/lib/utils";

// Import config to pull provider urls
import config from "@supagraph/config";

// Use Store to interact with entity storage
import { Entity, Store, withDefault } from "supagraph";

// Each event is supplied the block and tx along with the typed args
import {
  JsonRpcProvider,
  TransactionReceipt,
  TransactionResponse,
} from "@ethersproject/providers";

// - These types will be generated based on the event signatures exported by the defined contracts in config (coming soon TM);
import type {
  DelegateEntity,
  DelegateChangedEvent,
  DelegateVotesChangedEvent,
} from "../types";

// Extract the Mantle token address so that we can detect which contract the event belongs to
const MANTLE_TOKEN_ADDRESS = getAddress(
  process.env.MANTLE_ADDRESS || "0xc1dC2d65A2243c22344E725677A3E3BEBD26E604"
);
const BITDAO_TOKEN_ADDRESS = getAddress(
  process.env.BITDAO_ADDRESS || "0xB17B140eddCC575DaD4256959b8A35d0E7E1Ae17"
);
const L2_MANTLE_TOKEN_ADDRESS = getAddress(
  process.env.L2_MANTLE_ADDRESS || "0xEd459209796D741F5B609131aBd927586fcCACC5"
);

// construct the provider once
const L2Provider = new JsonRpcProvider(
  config.providers[withDefault(process.env.L2_MANTLE_CHAIN_ID, 5001)].rpcUrl
);

// update sync pointers and save
const updatePointers = async (
  entity: Entity<DelegateEntity> & DelegateEntity,
  tx: TransactionReceipt & TransactionResponse
) => {
  // update pointers for lastUpdate
  entity.set("blockNumber", +tx.blockNumber);
  entity.set("transactionHash", tx.transactionHash || tx.hash);

  // save the changes
  return await entity.save();
};

// Handler to consume DelegateChanged events from known contracts
export const DelegateChangedHandler = async (
  args: DelegateChangedEvent,
  { tx }: { tx: TransactionReceipt & TransactionResponse }
) => {
  // console.log("delegate: from", args.fromDelegate, "to", args.toDelegate);

  // format the current contractAddress
  const contractAddress = getAddress(tx.contractAddress || tx.to);

  // make sure we're calling from an appropriate contract
  if (
    [
      MANTLE_TOKEN_ADDRESS,
      BITDAO_TOKEN_ADDRESS,
      L2_MANTLE_TOKEN_ADDRESS,
    ].indexOf(contractAddress) !== -1
  ) {
    // construct token specific props
    const toProp = `${
      contractAddress === MANTLE_TOKEN_ADDRESS
        ? "mnt"
        : contractAddress === BITDAO_TOKEN_ADDRESS
        ? "bit"
        : "l2Mnt"
    }To` as "mntTo" | "bitTo" | "l2MntTo";
    const countProp = `${
      contractAddress === MANTLE_TOKEN_ADDRESS
        ? "mnt"
        : contractAddress === BITDAO_TOKEN_ADDRESS
        ? "bit"
        : "l2Mnt"
    }DelegatorsCount` as
      | "mntDelegatorsCount"
      | "bitDelegatorsCount"
      | "l2MntDelegatorsCount";

    // load the entity for this batchIndex
    let entity = await Store.get<DelegateEntity>(
      "Delegate",
      getAddress(args.delegator)
    );

    // fetch old (this could be for the same delegate as entity)
    let oldDelegate =
      args.fromDelegate === args.delegator
        ? entity
        : await Store.get<DelegateEntity>(
            "Delegate",
            getAddress(args.fromDelegate)
          );

    // fetch new (this could be for the same delegate as entity or oldDelegate)
    let newDelegate =
      // eslint-disable-next-line no-nested-ternary
      args.toDelegate === args.delegator
        ? entity
        : args.fromDelegate === args.toDelegate
        ? oldDelegate
        : await Store.get<DelegateEntity>(
            "Delegate",
            getAddress(args.toDelegate)
          );

    // ignore delegations that move FROM the 0x0 address (these are previously undelegated)
    if (args.fromDelegate !== "0x0000000000000000000000000000000000000000") {
      // update the old and new delegates with the correct votes
      oldDelegate.set(
        "delegatorsCount",
        BigNumber.from(oldDelegate.delegatorsCount || "0").sub(1)
      );
      oldDelegate.set(
        countProp,
        BigNumber.from(oldDelegate[countProp] || "0").sub(1)
      );

      // update pointers for lastUpdate
      oldDelegate = await updatePointers(oldDelegate, tx);

      // align delegates
      if (args.fromDelegate === args.delegator) {
        entity = oldDelegate;
      }
      if (args.fromDelegate === args.toDelegate) {
        newDelegate = oldDelegate;
      }
    }

    // when not delegating to the 0x0 address, update counters (we don't care for the voting weight of 0x0)
    if (args.toDelegate !== "0x0000000000000000000000000000000000000000") {
      // update the counter on the delegate we're delegating to
      newDelegate.set(
        "delegatorsCount",
        BigNumber.from(newDelegate.delegatorsCount || "0").add(1)
      );
      newDelegate.set(
        countProp,
        BigNumber.from(newDelegate[countProp] || "0").add(1)
      );

      // update pointers for lastUpdate
      newDelegate = await updatePointers(newDelegate, tx);

      // align delegates
      if (args.toDelegate === args.delegator) {
        entity = newDelegate;
      }
      if (args.fromDelegate === args.toDelegate) {
        oldDelegate = newDelegate;
      }
    }

    // on L2 we don't have a DelegateVotesChangedHandler, so we should apply changes now against this event
    if (contractAddress === L2_MANTLE_TOKEN_ADDRESS) {
      // fetch the oldBalance (if this isnt set then we havent recorded the delegation to the oldDelegate)
      let oldBalance = entity.l2MntBalance || "0";
      // get the current l2Balance for the user (we want this post gas spend for this tx)
      let newBalance = await L2Provider.getBalance(
        args.delegator,
        tx.blockNumber
      );

      // if we're not setting a new delegation...
      if (args.fromDelegate !== "0x0000000000000000000000000000000000000000") {
        oldDelegate.set(
          "votes",
          BigNumber.from(oldDelegate.votes || "0").sub(
            BigNumber.from(oldBalance)
          )
        );
        oldDelegate.set(
          "l2MntVotes",
          BigNumber.from(oldDelegate.l2MntVotes || "0").sub(
            BigNumber.from(oldBalance)
          )
        );

        // save the changes to old delegate
        oldDelegate = await updatePointers(oldDelegate, tx);

        // align delegates
        if (args.fromDelegate === args.delegator) {
          entity = oldDelegate;
        }
        if (args.fromDelegate === args.toDelegate) {
          newDelegate = oldDelegate;
        }
      }

      // if we're not removing the delegation...
      if (args.toDelegate !== "0x0000000000000000000000000000000000000000") {
        newDelegate.set(
          "votes",
          BigNumber.from(newDelegate.votes || "0").add(newBalance)
        );
        newDelegate.set(
          "l2MntVotes",
          BigNumber.from(newDelegate.l2MntVotes || "0").add(newBalance)
        );

        // save the changes to new delegate
        newDelegate = await updatePointers(newDelegate, tx);

        // align delegates
        if (args.toDelegate === args.delegator) {
          entity = newDelegate;
        }
        if (args.fromDelegate === args.toDelegate) {
          oldDelegate = newDelegate;
        }
      }

      // record the new balance
      entity.set("l2MntBalance", newBalance);

      // save the changes
      entity = await updatePointers(entity, tx);
    }

    // set the to according to the delegation
    entity.set(toProp, args.toDelegate);

    // update pointers for lastUpdate
    await updatePointers(entity, tx);
  }
};

// Handler to consume DelegateVotesChanged events from known contracts
export const DelegateVotesChangedHandler = async (
  args: DelegateVotesChangedEvent,
  { tx }: { tx: TransactionReceipt & TransactionResponse }
) => {
  // console.log("votes changed:", args.delegate, "from", args.previousBalance.toString(), "to", args.newBalance.toString());

  // format the current contractAddress
  const contractAddress = getAddress(tx.contractAddress || tx.to);

  // make sure we're calling from an appropriate contract
  if (
    [MANTLE_TOKEN_ADDRESS, BITDAO_TOKEN_ADDRESS].indexOf(contractAddress) !==
      -1 &&
    args.delegate !== "0x0000000000000000000000000000000000000000"
  ) {
    // construct token specific props
    const votesProp = `${
      contractAddress === MANTLE_TOKEN_ADDRESS ? "mnt" : "bit"
    }Votes` as "mntVotes" | "bitVotes";
    const otherVotesProps = ["mntVotes", "bitVotes", "l2MntVotes"].filter(
      (val) => val !== votesProp
    );

    // load the entity for this batchIndex
    const entity = await Store.get<DelegateEntity>(
      "Delegate",
      getAddress(args.delegate)
    );

    // store changes
    entity.set(votesProp, args.newBalance);

    // votes is always a sum of both
    entity.set(
      "votes",
      BigNumber.from(args.newBalance || "0").add(
        otherVotesProps.reduce((sum, otherVotesProp) => {
          return sum.add(BigNumber.from(entity[otherVotesProp] || "0"));
        }, BigNumber.from("0"))
      )
    );

    // update pointers for lastUpdate
    await updatePointers(entity, tx);
  }
};

// // Handler to consume Transfer events from known contracts (I DONT THINK WE NEED TO RECORD THIS -- WHY DO WE WANT THE BALANCES IN THE GRAPHQL DB??)
// export const TransferHandler = async (
//   args: TransferEvent,
//   { tx }: { tx: TransactionReceipt }
// ) => {
//   // console.log("transfer: from", args.from, "to", args.to, "for", args.value.toString());

//   // format the current contractAddress
//   const contractAddress = getAddress(tx.contractAddress || tx.to);

//   // make sure we're calling from an appropriate contract
//   if (
//     [MANTLE_TOKEN_ADDRESS, BITDAO_TOKEN_ADDRESS].indexOf(contractAddress) !== -1
//   ) {
//     // construct token specific props
//     const balanceProp = `${
//       contractAddress === MANTLE_TOKEN_ADDRESS ? "mnt" : "bit"
//     }Balance` as "mntBalance" | "bitBalance";
//     const otherBalanceProps = [
//       "mntBalance",
//       "bitBalance",
//       "l2MntBalance",
//     ].filter((val) => val !== balanceProp);

//     // load the entities involved in this tx
//     const entity = await Store.get<DelegateEntity>(
//       "Delegate",
//       getAddress(args.from)
//     );
//     const recipient = await Store.get<DelegateEntity>(
//       "Delegate",
//       getAddress(args.to)
//     );

//     // update sender if its not 0x0 address
//     if (args.from !== "0x0000000000000000000000000000000000000000") {
//       // get new balance for sender
//       const newBalance = BigNumber.from(entity[balanceProp] || "0").sub(
//         args.value
//       );

//       // sub from the sender
//       entity.set(balanceProp, newBalance);

//       // sum all the balances for sender
//       entity.set(
//         "balance",
//         BigNumber.from(newBalance).add(
//           otherBalanceProps.reduce((sum, otherBalanceProp) => {
//             return sum.add(BigNumber.from(entity[otherBalanceProp] || "0"));
//           }, BigNumber.from("0"))
//         )
//       );

//       // update pointers for lastUpdate
//       await updatePointers(entity, tx);
//     }

//     // add the transfer to the balance
//     const newBalance = BigNumber.from(recipient[balanceProp] || "0").add(
//       args.value
//     );

//     // if we're not transfering to the 0x0 address...
//     if (args.to !== "0x0000000000000000000000000000000000000000") {
//       // add to the recipient
//       recipient.set(balanceProp, newBalance);

//       // sum all the balances for receiptient
//       recipient.set(
//         "balance",
//         BigNumber.from(newBalance).add(
//           otherBalanceProps.reduce((sum, otherBalanceProp) => {
//             return sum.add(BigNumber.from(recipient[otherBalanceProp] || "0"));
//           }, BigNumber.from("0"))
//         )
//       );

//       // update pointers for lastUpdate
//       await updatePointers(recipient, tx);
//     }
//   }
// };
