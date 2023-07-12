// All valid value types in the graphql context
export const VALUE_TYPES = [
  "ID",
  "Int",
  "Decimal",
  "Boolean",
  "String",
  "Bytes",
  "BigInt",
  "BigDecimal",
  "Timestamp",
];

// All numericals share the same filter options (ID, BigInt, BigDecimal)
export const NUMERIC_FILTERS = [
  "_gt",
  "_gte",
  "_in",
  "_lt",
  "_lte",
  "_not",
  "_not_in",
];

// Strings and Bytes share the same filter set
export const STRING_FILTERS = [
  "_contains",
  "_in",
  "_not",
  "_not_contains",
  "_not_in",
  "_starts_with",
  "_ends_with",
];

// All others fallback to using the fullset... we also use these to strip the keyName in where args (this arr is deliberately ordered...)
export const ALL_FILTERS = [
  "_gte",
  "_gt",
  "_lte",
  "_lt",
  "_not_contains_nocase",
  "_not_contains",
  "_not_ends_with_nocase",
  "_not_ends_with",
  "_not_starts_with_nocase",
  "_not_starts_with",
  "_ends_with_nocase",
  "_ends_with",
  "_starts_with_nocase",
  "_starts_with",
  "_not_in",
  "_not",
  "_in",
  "_contains_nocase",
  "_contains",
];

// Map of all the filters
export const FILTER_TYPE_MAP = {
  ID: STRING_FILTERS,
  Int: NUMERIC_FILTERS,
  String: STRING_FILTERS,
  Bytes: STRING_FILTERS,
  Decimal: NUMERIC_FILTERS,
  BigInt: NUMERIC_FILTERS,
  BigDecimal: NUMERIC_FILTERS,
  Timestamp: NUMERIC_FILTERS,
  default: ALL_FILTERS,
};
