import { MongoClient } from "mongodb";

// Add a global rejection-handler (hopefully this isnt triggered)
process.on("unhandledRejection", (err, promise) => {
  // eslint-disable-next-line no-console
  console.log(`Unhandled rejection (promise: ${promise}, reason: ${err})`);
});

// Containers for connections we have...
const client: Record<string, MongoClient> = {};
const clientPromise: Record<string, Promise<MongoClient>> = {};

// Keep trying for the db
export const getMongodb = async (uri: string): Promise<MongoClient> => {
  if (!uri) {
    throw new Error("Please provide a monogo uri to proceed");
  }

  // wrap in try-catch so we can retry on failure
  try {
    // giving 29seconds for the client to respond before cancelling and trying again
    const asPromised = await Promise.race([
      // if this finishes first we return another call to getMongodb()
      new Promise((resolve) => {
        setTimeout(() => resolve(false), 29000);
      }),
      // if this finished first then we got the client...
      new Promise((resolve, reject) => {
        try {
          if (!clientPromise[uri] || process.env.NODE_ENV === "production") {
            // if in dev this is the first time we're defining the connection...
            if (process.env.NODE_ENV === "development") {
              // In development mode, use a global variable so that the value
              // is preserved across module reloads caused by HMR (Hot Module Replacement).
              const globalWithMongoClientPromise =
                global as typeof globalThis & {
                  mongoClientPromise: Record<string, Promise<MongoClient>>;
                };
              // check if the global store needs to be defined
              if (
                !globalWithMongoClientPromise.mongoClientPromise ||
                !globalWithMongoClientPromise.mongoClientPromise[uri]
              ) {
                // console.log("connect client", uri);
                client[uri] = new MongoClient(uri as string);
                globalWithMongoClientPromise.mongoClientPromise =
                  globalWithMongoClientPromise.mongoClientPromise || {};
                globalWithMongoClientPromise.mongoClientPromise[uri] =
                  client[uri].connect();
              }

              // resolve to the alredy setup global promise
              clientPromise[uri] =
                globalWithMongoClientPromise.mongoClientPromise[uri];
            } else if (!clientPromise[uri]) {
              // In production mode, it's best to not to restore from a global variable - but we can still establish the connection once per run
              client[uri] = new MongoClient(uri as string);
              // connect and store the connection
              clientPromise[uri] = client[uri].connect();
            }
          }
        } catch (e) {
          reject(e);
        }
        // resolve to the established clientPromise
        resolve(clientPromise[uri]);
      }),
    ]);
    // keep trying...
    if (!asPromised) {
      // but keep the action on a single chain
      return await getMongodb(uri);
    }
    // we've got the promised client
    return await (asPromised as Promise<MongoClient>);
  } catch (e) {
    // keep retyring until we get a response...
    return getMongodb(uri);
  }
};
