// Call an operation with a set number of retries
export const withRetries = async <T>(
  operation: (retries: number) => Promise<T>,
  onError?: (e: any, retries: number) => Promise<void> | void,
  finalise?: () => void,
  maxRetries: number = 3,
  retryDelay: number | (() => number) = 1000
): Promise<T> => {
  let retries = 0;
  let response: T;
  // keep attempting upto retry limit
  while (retries < maxRetries) {
    try {
      // call the operation stating how many attempts have been made so far
      response = await operation(retries);

      // break the while loop on success
      if (response) {
        break;
      }
    } catch (error) {
      // catch any errors
      retries += 1;

      // handle error fn
      if (onError) onError(error, retries);

      // timeout with a delay
      if (retries < maxRetries) {
        await new Promise((resolve) => {
          setTimeout(
            resolve,
            // set the retryDelay dynamically if provided as a func
            typeof retryDelay === "number" ? retryDelay : retryDelay()
          );
        });
      } else {
        // handle when maximum retries are reached, e.g., log an error or throw an exception.
        throw error;
      }
    } finally {
      // if finalise is provided
      if (finalise) finalise();
    }
  }

  // return the block with transactions
  return response;
};
