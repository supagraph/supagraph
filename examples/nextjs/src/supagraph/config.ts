// Assign default value to provided env
import { withDefault } from "supagraph";

// Export the complete supagraph configuration (sync & graph) - we can add everything from Mappings to this config
const config = {
  // set the local engine (true: db || false: mongo)
  dev: false,
  // set the listening state of the sync
  listen: false,
  // should we cleanup the values we pull in the initial sync?
  cleanup: false,
  // name your supagraph (this will inform mongo table name etc...)
  name: withDefault(
    process.env.SUPAGRAPH_NAME,
    "supagraph--migrator--testnet--0-0-1"
  ),
  // flag mutable to insert by upsert only on id field (mutate entities)
  // - otherwise use _block_number + id to make a unique entry and do a distinct groupBy on the id when querying
  //   ie: do everything the immutable way (this can be a lot more expensive)
  mutable: true,
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
  },
  // configure available Contracts and their block details
  contracts: {
    migrator: {
      // set the handlers
      handlers: "migrator",
      // Establish all event signatures available on this contract (we could also accept a .sol or .json file here)
      events: [
        process.env.NEXT_PUBLIC_L1_CHAIN_ID === "1"
          ? "event TokensMigrated(address indexed to, uint256 amountSwapped)"
          : "event TokensMigrated(address indexed to, uint256 amountSwapped, uint256 amountReceived)",
      ],
      // set config from env
      chainId: withDefault(process.env.NEXT_PUBLIC_L1_CHAIN_ID, 5),
      address: withDefault(
        process.env.L1_CONVERTER_CONTRACT_ADDRESS,
        "0x144D9B7F34a4e3133C6F347886fBe2700c4cb268"
      ),
      startBlock: withDefault(
        process.env.L1_CONVERTER_CONTRACT_START_BLOCK,
        9127692
      ),
      endBlock: withDefault(
        process.env.L1_CONVERTER_CONTRACT_END_BLOCK,
        "latest"
      ),
      // We will always collect receipts here to construct gas-cost of call Migration for refund
      collectTxReceipts: true,
    },
  },
  // define supagraph schema
  schema: `
    type Account @entity {
      id: ID!
      migrations: [Migration!]! @derivedFrom(field: "account")
      migratedMnt: BigInt!
      migrationCount: Int!
      blockNumber: BigInt!
      transactionHash: String! 
    }
    type Migration @entity {
      id: ID!
      account: Account!
      amountSwapped: BigInt!
      gasCost: BigInt!
      refunded: Boolean!
      refundTx: String!
      blockTimestamp: Int!
      blockNumber: Int!
      transactionHash: String! 
    }
  `,
  // define supagraph default query
  defaultQuery: `
    query TopTenMNTMigrators {
      accounts(
        first: 10
        orderBy: migratedMnt
        orderDirection: desc
        where: {migratedMnt_gt: "0"}
      ) {
        id
        migratedMnt
        migrationCount
        migrations(first: 10, orderBy: blockTimestamp, orderDirection: desc) {
          amountSwapped
          gasCost
          refunded
          blockTimestamp
        }
      }
    }
  `,
};

// export config as default export
export default config;
