// import express
import express, { Express } from "express";

// import local supagraph init scripts
import { start } from "./sync";
import { graphql } from "./graphql";
import { snapshot } from "./snapshot";

// type and cast default values
import { withDefault } from "supagraph";

// import env file and load contents
import dotenv from "dotenv";
dotenv.config();

// create a new app to respond to reqs
const app: Express = express();

// use json reqs/resps
app.use(express.json());

// attach public api access to the supagraph content by default
if (withDefault(process.env.PUBLIC, true)) {
  // attach supagraphs graphql endpoint
  app.use("/graphql", graphql);

  // attach snapshot strategy
  app.post("/snapshot", snapshot);
}

// listen for connections (default to 8000)
app.listen(withDefault(process.env.PORT, 8000), async () => {
  // server started - lets go...
  console.log(
    `⚡️[server]: Server is running at http://localhost:${withDefault(
      process.env.PORT,
      8000
    )}`
  );
  // catch any unprocessed errors and kill the service
  process.on("uncaughtException", function (err) {
    console.error("[SERVER ERROR - STOP]:", err);
    // if we exit more than 999999999 times, we will need to manually restart the app (when deployed via railway.app)
    process.exit(1);
  });
  // start the sync operation (no need to await - it will run forever)
  start();
});
