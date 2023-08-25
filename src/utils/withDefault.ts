// Provide a default value and type the response
export const withDefault = <T>(value: unknown, defaultValue: T) => {
  // if the default is a number, parse value as int
  if (typeof defaultValue === "number") {
    // eslint-disable-next-line no-param-reassign
    value = parseInt((value as string) || "0", 10);
  }
  // if default is a bool, prase the bool
  if (typeof defaultValue === "boolean" && typeof value === "string") {
    value = value === "true";
  }
  // return as the same type as defaultValue
  return (value || defaultValue) as T;
};

export default withDefault;
