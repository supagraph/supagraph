// Provide a default value and type the response
export const withDefault = <T>(value: unknown, defaultValue: T) => {
  // if default is a bool, prase the bool
  if (typeof defaultValue === "boolean" && typeof value === "string") {
    value = value === "true";
    // value is cast by check for "true"
    return value;
  }
  // if the default is a number, parse value as int
  if (typeof defaultValue === "number" && typeof value === "string") {
    // eslint-disable-next-line no-param-reassign
    value = parseInt((value as string) || "0", 10);
  }

  // return as the same type as defaultValue
  return (value || defaultValue) as T;
};

export default withDefault;
