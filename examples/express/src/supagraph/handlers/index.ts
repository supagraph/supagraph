/* eslint-disable no-console */

// type the handlers
import { Handlers, withDefault } from "supagraph";

// Import the handlers
import { TransactionHandler } from "./network";
import {
  // TransferHandler,
  DelegateChangedHandler,
  DelegateVotesChangedHandler,
} from "./token";

// Construct the handlers to register each contract against its handlers
const handlers: Handlers = {
  // construct handlers for the network level events (withDefault will parse numerics)
  [withDefault(process.env.L2_MANTLE_CHAIN_ID, 5001)]: {
    // eventName -> handler()
    onTransaction: TransactionHandler,
  },
  tokenl1: {
    // eventName -> handler()
    // Transfer: TransferHandler,
    DelegateChanged: DelegateChangedHandler,
    DelegateVotesChanged: DelegateVotesChangedHandler,
  },
  tokenl2: {
    // eventName -> handler()
    DelegateChanged: DelegateChangedHandler,
  },
};

// export handlers as default
export default handlers;
