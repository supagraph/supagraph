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

// map provider from the syncOp
export const mapProviders = async (syncOp: Sync) => {
  // if the network is resolved for the provider...
  if (syncOp.provider?.network?.chainId) {
    // record the provider for the chain
    return syncOp.provider;
  }
  // get the network from the provider
  await syncOp.provider.getNetwork();
  // record the provider for the chain
  return syncOp.provider;
};

// reduce the providers
export const reduceProviders = (
  all: Record<number, JsonRpcProvider | WebSocketProvider>,
  provider: JsonRpcProvider | WebSocketProvider
) => {
  // place the provider into an index
  all[provider.network.chainId] = provider;
  return all;
};

// get all networks chainIds
export const getNetworks = async (useSyncs?: Sync[]) => {
  // retrieve the engine backing the Store
  const engine = await getEngine();

  // get all network descriptions now...
  const syncProviders = (
    await Promise.all((useSyncs || engine.syncs).map(mapProviders))
  ).reduce(
    reduceProviders,
    {} as Record<number, JsonRpcProvider | WebSocketProvider>
  );

  // return both chainIds and syncProviders
  return {
    // return all providers
    syncProviders,
    // index the chains defined
    chainIds: new Set<number>([...Object.keys(syncProviders).map((v) => +v)]),
  };
};
