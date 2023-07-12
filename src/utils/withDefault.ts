// provide a default value and type the response
export const withDefault = <T>(value: unknown, defaultValue: T) => {
  // if the default is a number, parse value as int
  if (typeof defaultValue === "number") {
    // eslint-disable-next-line no-param-reassign
    value = parseInt((value as string) || "0", 10);
  }
  // return as the same type as defaultValue
  return (value || defaultValue) as T;
};

export default withDefault;
