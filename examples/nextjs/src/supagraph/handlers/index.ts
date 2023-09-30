// type the handlers
import { Handlers } from "supagraph";

// Import the handlers
import { TokensMigratedHandler } from "./migrator";

// Construct the handlers to register each contract against its handlers
export const handlers: Handlers = {
  // construct as a named group
  migrator: {
    // eventName -> handler()
    TokensMigrated: TokensMigratedHandler,
  },
};
