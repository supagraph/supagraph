// get camelCase of string
export const toCamelCase = (input: string) => {
  const camelIn = input?.split("") || [""];
  const camelOut = input && camelIn.shift()!.toLowerCase() + camelIn.join("");

  return !input ? input : camelOut;
};

export default toCamelCase;
