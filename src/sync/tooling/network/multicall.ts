import { Provider } from "@ethersproject/providers";
import { BigNumberish, Contract } from "ethers";

/**
 * @notice Multicall contract abi - Multicall3: https://github.com/mds1/multicall
 */
export const MULTICALL_ABI = [
  // https://github.com/mds1/multicall
  "function aggregate(tuple(address target, bytes callData)[] calls) payable returns (uint256 blockNumber, bytes[] returnData)",
  "function aggregate3(tuple(address target, bool allowFailure, bytes callData)[] calls) payable returns (tuple(bool success, bytes returnData)[] returnData)",
  "function aggregate3Value(tuple(address target, bool allowFailure, uint256 value, bytes callData)[] calls) payable returns (tuple(bool success, bytes returnData)[] returnData)",
  "function blockAndAggregate(tuple(address target, bytes callData)[] calls) payable returns (uint256 blockNumber, bytes32 blockHash, tuple(bool success, bytes returnData)[] returnData)",
  "function getBasefee() view returns (uint256 basefee)",
  "function getBlockHash(uint256 blockNumber) view returns (bytes32 blockHash)",
  "function getBlockNumber() view returns (uint256 blockNumber)",
  "function getChainId() view returns (uint256 chainid)",
  "function getCurrentBlockCoinbase() view returns (address coinbase)",
  "function getCurrentBlockDifficulty() view returns (uint256 difficulty)",
  "function getCurrentBlockGasLimit() view returns (uint256 gaslimit)",
  "function getCurrentBlockTimestamp() view returns (uint256 timestamp)",
  "function getEthBalance(address addr) view returns (uint256 balance)",
  "function getLastBlockHash() view returns (bytes32 blockHash)",
  "function tryAggregate(bool requireSuccess, tuple(address target, bytes callData)[] calls) payable returns (tuple(bool success, bytes returnData)[] returnData)",
  "function tryBlockAndAggregate(bool requireSuccess, tuple(address target, bytes callData)[] calls) public view returns (uint256 blockNumber, bytes32 blockHash, tuple(bool success, bytes returnData)[] returnData)",
];

/**
 * @notice Get an ethers contract pointing at the multicall contract with given address
 * @param calls {address, provider} The address, the provider to make the calls against
 * @returns ethers.Contract multicall contract
 */
export async function getMulticallContract(
  address: `0x${string}`,
  provider: Provider
) {
  const multicall = new Contract(address, MULTICALL_ABI, provider);

  return multicall;
}

/**
 * @notice Get the multicall data for the fns on a contract @ address
 * @param calls {target, contract, fns} The address, contract and fns we want to call via multicall
 * @returns A list of calls we can make to multicall.tryBlockAndAggregate()
 */
const getMulticallData = (
  calls: {
    target: string;
    contract: Contract;
    fns: (string | { fn: string; args: (string | BigNumberish | boolean)[] })[];
  }[]
) => {
  const flatMap: {
    target: string;
    callData: string;
  }[] = [];
  calls.forEach((call) =>
    flatMap.push(
      ...call.fns.map((fn) => {
        const args = typeof fn === "string" ? [fn] : [fn.fn, fn.args];
        return {
          target: call.target,
          callData: call.contract.interface.encodeFunctionData(
            args[0] as string,
            args[1] as (string | BigNumberish | boolean)[]
          ),
        };
      })
    )
  );

  return flatMap;
};

/**
 * @notice Decode the returnData we get by calling multicall.tryBlockAndAggregate
 * @param returnData An array of multicall.tryBlockAndAggregate results
 * @param calls {contract, fns} The contract and fns we want to decode the result for
 * @returns all results returned in an array
 */
const decodeMulticallReturnData = (
  returnData: { success: boolean; returnData: string }[],
  calls: {
    contract: Contract;
    fns: (string | { fn: string; args: (string | BigNumberish | boolean)[] })[];
  }[]
) => {
  let flatPtr = 0;
  const flatMap: (string | BigNumberish | boolean)[] = [];
  calls.forEach((call) =>
    flatMap.push(
      ...call.fns.map((fn) => {
        const data = call.contract.interface.decodeFunctionResult(
          typeof fn === "string" ? fn : fn.fn,
          returnData[((flatPtr += 1), flatPtr - 1)].returnData
        );
        // If the call returned one element, just get rid of the array wrapper and return the data directly.
        // If the call returned multiple elements, we want to preserve that structure
        return data.length === 1 ? data[0] : data;
      })
    )
  );

  return flatMap;
};

/**
 * @notice Get the response from a call to multicall for the given fns on a contract @ address
 * @param calls {target, contract, fns} The address, contract and fns we want to call via multicall
 * @returns all results returned in an array
 */
export async function callMulticallContract(
  multicall: Contract,
  calls: {
    target: string;
    contract: Contract;
    fns: (string | { fn: string; args: (string | BigNumberish)[] })[];
  }[],
  blockTag: number | "latest" = "latest"
) {
  const { returnData } =
    (await multicall.tryBlockAndAggregate(false, getMulticallData(calls), {
      blockTag:
        blockTag === "latest" ? "latest" : `0x${(+blockTag).toString(16)}`,
    })) || {};

  return decodeMulticallReturnData(returnData, calls);
}
