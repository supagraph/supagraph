// Assign default value to provided env
import { withDefault } from "supagraph";

// import env file and load contents
import dotenv from "dotenv";
dotenv.config();

// Export the complete supagraph configuration (sync & graph)
const config = {
  // name your supagraph (this will inform mongo table name etc...)
  name: withDefault(
    process.env.SUPAGRAPH_NAME,
    "supagraph--token-express--testnet--0-0-1"
  ),
  // set the local engine (true: db || false: mongo)
  dev: true,
  // should we cleanup files after the initial sync?
  cleanup: true,
  // collect blocks to sort by ts
  collectBlocks: true,
  // flag mutable to insert by upsert only on id field (mutate entities)
  // - otherwise use _block_number + id to make a unique entry and do a distinct groupBy on the id when querying
  //   ie: do everything the immutable way (this can be a lot more expensive)
  mutable: false,
  // how often do we want queries to be revalidated?
  revalidate: 12,
  staleWhileRevalidate: 59,
  // configure providers
  providers: {
    1: {
      rpcUrl: `https://mainnet.infura.io/v3/${process.env.NEXT_PUBLIC_INFURA_API_KEY}`,
    },
    5: {
      rpcUrl: `https://goerli.infura.io/v3/${process.env.NEXT_PUBLIC_INFURA_API_KEY}`,
    },
    [process.env.L2_MANTLE_CHAIN_ID]: {
      rpcUrl: withDefault(
        process.env.MANTLE_RPC_URI,
        "https://rpc.testnet.mantle.xyz"
      ),
    },
  },
  // register events into named groups
  events: {
    tokenl1: [
      "event Transfer(address indexed from, address indexed to, uint256 value)",
      "event DelegateVotesChanged(address indexed delegate, uint256 previousBalance, uint256 newBalance)",
      "event DelegateChanged(address indexed delegator, address indexed fromDelegate, address indexed toDelegate)",
    ],
    tokenl2: [
      "event DelegateChanged(address indexed delegator, address indexed fromDelegate, address indexed toDelegate)",
    ],
  },
  // configure available Contracts and their block details
  contracts: {
    // // setup network listeners (this will be an expensive handler to catchup - we can uncomment this after initial sync...)
    // [withDefault(process.env.L2_MANTLE_CHAIN_ID, 5001)]: {
    //   // establish the point we want to start and stop syncing from
    //   startBlock: withDefault(process.env.L2_MANTLE_START_BLOCK, 18889522),
    //   endBlock: withDefault(process.env.L2_MANTLE_END_BLOCK, "latest"),
    //   // collect receipts to gather gas usage
    //   collectTxReceipts: true,
    // },
    mantle: {
      // establish all event signatures available on this contract (we could also accept a .sol or .json file here)
      events: "tokenl1",
      // use handlers registered against "token" named group in handlers/index.ts
      handlers: "tokenl1",
      // set config from env
      chainId: withDefault(process.env.MANTLE_CHAIN_ID, 5),
      address: withDefault(
        process.env.MANTLE_ADDRESS,
        "0xc1dC2d65A2243c22344E725677A3E3BEBD26E604"
      ),
      startBlock: withDefault(process.env.MANTLE_START_BLOCK, 9127688),
      endBlock: withDefault(process.env.MANTLE_END_BLOCK, "latest"),
    },
    bitdao: {
      // establish all event signatures available on this contract (we could also accept a .sol or .json file here)
      events: "tokenl1",
      // use handlers registered against "token" named group in handlers/index.ts
      handlers: "tokenl1",
      // set config from env
      chainId: withDefault(process.env.BITDAO_CHAIN_ID, 5),
      address: withDefault(
        process.env.BITDAO_ADDRESS,
        "0xB17B140eddCC575DaD4256959b8A35d0E7E1Ae17"
      ),
      startBlock: withDefault(process.env.BITDAO_START_BLOCK, 7728490),
      endBlock: withDefault(process.env.BITDAO_END_BLOCK, "latest"),
    },
    l2mantle: {
      // establish all event signatures available on this contract (we could also accept a .sol or .json file here)
      events: "tokenl2",
      // use handlers registered against "token" named group in handlers/index.ts
      handlers: "tokenl2",
      // set config from env
      chainId: withDefault(process.env.L2_MANTLE_CHAIN_ID, 5),
      address: withDefault(
        process.env.L2_MANTLE_ADDRESS,
        "0xEd459209796D741F5B609131aBd927586fcCACC5"
      ),
      startBlock: withDefault(process.env.L2_MANTLE_START_BLOCK, 18889522),
      endBlock: withDefault(process.env.L2_MANTLE_END_BLOCK, "latest"),
    },
  },
  // define supagraph schema
  schema: `
    type Delegate @entity {
      id: ID!
      bitTo: String!
      mntTo: String!
      l2MntTo: String!
      votes: BigInt!
      mntVotes: BigInt!
      bitVotes: BigInt!
      l2MntVotes: BigInt!
      mntDelegators: [Delegate!]! @derivedFrom(field: "mntTo")
      bitDelegators: [Delegate!]! @derivedFrom(field: "bitTo")
      l2MntDelegators: [Delegate!]! @derivedFrom(field: "l2MntTo")
      delegatorsCount: BigInt!
      mntDelegatorsCount: BigInt!
      bitDelegatorsCount: BigInt!
      l2MntDelegatorsCount: BigInt!
      blockNumber: BigInt!
      transactionHash: String!
    }
  `,
  // define supagraph default query
  defaultQuery: `
    query TopTenDelegatesByVotes {
      delegates(
        first: 10
        orderBy: votes
        orderDirection: desc
        where: {votes_gt: "0"}
      ) {
        id
        bitTo
        mntTo
        l2MntTo
        votes
        bitVotes
        mntVotes
        l2MntVotes
        delegatorsCount
        bitDelegatorsCount
        mntDelegatorsCount
        l2MntDelegatorsCount
        bitDelegators {
          id
        }
        mntDelegators {
          id
        }
        l2MntDelegators {
          id
        }
      }
    }
  `,
};

// export config as default export
export default config;
