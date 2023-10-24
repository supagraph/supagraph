// Use ethers for event typings
import type { ethers } from "ethers";

// return as ethers Event data (ignore strings and numbers returning an empty obj instead)
export const toEventData = (data: ethers.Event | string | number) => {
  if (!data || typeof data === "string" || typeof data === "number") {
    return {} as ethers.Event;
  }
  return data;
};
