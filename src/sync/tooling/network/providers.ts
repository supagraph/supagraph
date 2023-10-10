// this script will deal with providers supplied by ethers
import { providers } from "ethers";

// global engine to store runtime state
import { JsonRpcProvider, WebSocketProvider } from "@ethersproject/providers";

// import global types/tools
import { Sync } from "@/sync/types";
import { getEngine } from "@/sync/tooling/persistence/store";

// list of public rpc endpoints
export const altProviders: Record<number, string[]> = {
  1: [`https://rpc.ankr.com/eth`],
  5: [
    `https://ethereum-goerli.publicnode.com`,
    `https://eth-goerli.public.blastapi.io`,
    `https://rpc.goerli.eth.gateway.fm`,
    `https://rpc.ankr.com/eth_goerli`,
  ],
  5000: [`https://rpc.mantle.xyz`],
  5001: [`https://rpc.testnet.mantle.xyz`],
};

// set the default RPC providers
export const rpcProviders: Record<
  number,
  Record<number, providers.JsonRpcProvider>
> = {};

// function to return a provider from the alt list
export const getRandomProvider = (
  chainId: number,
  extraProviders: string[] = []
) => {
  const useProviders = [...extraProviders, ...altProviders[chainId]];
  const key = Math.floor(Math.random() * useProviders.length);
  // ensure record exists for rpcProviders on chainId
  rpcProviders[chainId] = rpcProviders[chainId] || {};
  rpcProviders[chainId][key] =
    rpcProviders[chainId][key] ||
    new providers.JsonRpcProvider(useProviders[key]);
  return rpcProviders[chainId][key];
};

// function to return a provider from the alt list
export const getProvider = async (chainId: number) => {
  const engine = await getEngine();

  return engine.providers?.[chainId]?.[0] || false;
};

// get all networks chainIds
export const getNetworks = async (useSyncs?: Sync[]) => {
  // chainIds visited in the process
  const chainIds: Set<number> = new Set<number>();
  const syncProviders: Record<number, JsonRpcProvider | WebSocketProvider> = {};

  // retrieve the engine backing the Store
  const engine = await getEngine();

  // get all network descriptions now...
  await Promise.all(
    (useSyncs || engine.syncs).map(async (syncOp) => {
      if (syncOp.provider?.network?.chainId) {
        // record the chainId
        chainIds.add(syncOp.provider.network.chainId);
        // record the provider for the chain
        syncProviders[syncOp.provider.network.chainId] = syncOp.provider;
      } else {
        // get the network from the provider
        const { chainId } = await syncOp.provider.getNetwork();
        // record the chainId
        chainIds.add(chainId);
        // record the provider for the chain
        syncProviders[chainId] = syncOp.provider;
      }
    })
  );

  return {
    chainIds,
    syncProviders,
  };
};
