import { Request, Response } from "express";

// Use config to id mongodb connection
import { config } from "@supagraph/config";
import { withDefault } from "supagraph";

// Import client generator
import { getMongodb } from "@providers/mongoClient";

// Only check values against checksummed addresses
import { getAddress } from "ethers/lib/utils";

// Generate client
const client = getMongodb(process.env.MONGODB_URI);

// Method to format as snapshot expects
export const snapshot = async (req: Request, res: Response) => {
  // fetch the db connection
  const mongo = (await client).db(config.name || "supagraph");

  // connect to the entity collection
  const collection = mongo.collection("delegate");

  // return only the requested addresses at the given snapshot block
  const { addresses, snapshot } = req.body as {
    addresses: string[];
    snapshot: string;
  };

  // checksum addresses
  const checksummed = addresses.map((address) =>
    getAddress(address.toLowerCase())
  );

  // query directly from mongo to save cycles (this doesnt need to be a graphql query)
  const result = collection.aggregate(
    [
      // limit the results to those left of the snapshot
      {
        $match: {
          // fetch from before (or from) the requested blockNumber
          _block_num: {
            $lte: +`${snapshot}`,
          },
          // limit to updates on this chainId (to make sure we're matching the correct blockNumber)
          _chain_id: withDefault(config.contracts.l2mantle.chainId, 5001),
          // this should be provided as a max of 500 addresses
          id: {
            $in: checksummed,
          },
        },
      },
      // sort what we find
      {
        $sort: {
          _block_ts: -1,
        },
      },
      // take the first entry
      {
        $group: {
          _id: "$id",
          latestDocument: {
            $first: "$$ROOT",
          },
        },
      },
      // project the shape of the response
      {
        $project: {
          _id: 1,
          id: "$latestDocument.id",
          l2MntVotes: "$latestDocument.l2MntVotes",
        },
      },
    ],
    {
      // allow sort on disk (we probably don't need this if we can live without the timewalk groupBy on block_ts_')
      allowDiskUse: true,
      // force sorts to use numericOrdering on number-like-id's (this matches the manual sorts we build in ./queries)
      collation: {
        locale: "en_US",
        numericOrdering: true,
      },
    }
  );

  // get score for each address
  const scoreResults = (await result.toArray()).reduce((all, delegate) => {
    return {
      ...all,
      [delegate.id]: delegate.l2MntVotes || "0",
    };
  }, {});

  // default to no score if we can't find it in the db
  const finalVotes: { address: string; score: string }[] = await Promise.all(
    addresses.map(async (address) => {
      return {
        address,
        score: scoreResults[getAddress(address.toLowerCase())] || "0",
      };
    })
  );

  // return the requested scores for the requested users
  return res.json({
    score: finalVotes,
  });
};
