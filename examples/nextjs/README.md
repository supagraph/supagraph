# Supagraph - Mantle Migrator Example

## Startup

- Install deps (`$ pnpm i`)
- Copy `.example.env` to `.env` and set `NEXT_PUBLIC_INFURA_API_KEY` and `MONGODB_URI` 
- Start nextjs (`$ pnpm dev`)
- Visit http://localhost:3000/graphql/sync to sync all data
- Visit http://localhost:3000/graphql to query the data set with graphql

## Going to production

- If deployed via vercel then the cron job scheduled in `vercel.json` will invoke the /graphql/sync endpoint every minute
- If deployed anywhere else then the /graphql/sync endpoint will need to be called externally (we can create a cron ourselves with tools like https://cron-job.org)

