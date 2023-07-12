// General supagraph typings and constants
import { Where, EntityRecord } from "./types";
import {
  ALL_FILTERS,
  STRING_FILTERS,
  NUMERIC_FILTERS,
  FILTER_TYPE_MAP,
} from "./constants";
import { BigDecimal } from "./scalars/BigDecimal";

// All numericals share the same filter options (BigInt, BigDecimal etc...)
export { ALL_FILTERS, NUMERIC_FILTERS, STRING_FILTERS, FILTER_TYPE_MAP };

// Resolve each of the defined filters accordingly...
export const resolveFilters = (
  matched: boolean,
  child: EntityRecord,
  where_: Where | string,
  rawArg: string
) => {
  // keep track of if we've matched or not
  let isMatch = matched;

  // if the rawArg we're checking is defined as an object or string then skip this resolve attempt...
  if (
    typeof where_ !== "object" ||
    (typeof where_ === "object" && typeof where_[rawArg] === "object")
  ) {
    return isMatch;
  }

  // literal match in where_ clause...
  if (isMatch && typeof where_ === "object" && where_?.[`${rawArg}`]) {
    isMatch =
      typeof child?.[rawArg] === "string" &&
      typeof where_?.[rawArg] === "string"
        ? (child?.[rawArg] as string)?.toLowerCase() ===
          (where_?.[rawArg] as string)?.toLowerCase()
        : child?.[rawArg] === where_?.[rawArg];
  }

  // place contains and other filters here...
  if (isMatch && where_?.[`${rawArg}_contains`]) {
    isMatch = (child?.[rawArg] as string).includes(
      where_[`${rawArg}_contains`] as string
    );
  }
  if (isMatch && where_?.[`${rawArg}_contains_nocase`]) {
    isMatch = (child?.[rawArg] as string)
      .toLowerCase()
      .includes((where_[`${rawArg}_contains_nocase`] as string).toLowerCase());
  }
  if (isMatch && where_?.[`${rawArg}_not_contains`]) {
    isMatch = !(child?.[rawArg] as string).includes(
      where_[`${rawArg}_not_contains`] as string
    );
  }
  if (isMatch && where_?.[`${rawArg}_not_contains_nocase`]) {
    isMatch = !(child?.[rawArg] as string)
      .toLowerCase()
      .includes(
        (where_[`${rawArg}_not_contains_nocase`] as string).toLowerCase()
      );
  }
  if (isMatch && where_?.[`${rawArg}_gt`]) {
    if (!Number.isNaN(parseFloat(where_[`${rawArg}_gt`] as string))) {
      isMatch = BigDecimal.gt(
        (child?.[rawArg] as string) || "0.0",
        where_[`${rawArg}_gt`] as string
      );
    } else {
      isMatch = (child?.[rawArg] as string) > where_[`${rawArg}_gt`];
    }
  }
  if (isMatch && where_?.[`${rawArg}_gte`]) {
    if (!Number.isNaN(parseFloat(where_[`${rawArg}_gte`] as string))) {
      isMatch = BigDecimal.gte(
        (child?.[rawArg] as string) || "0.0",
        where_[`${rawArg}_gte`] as string
      );
    } else {
      isMatch = (child?.[rawArg] as string) >= where_[`${rawArg}_gte`];
    }
  }
  if (isMatch && where_?.[`${rawArg}_in`]) {
    isMatch = (where_[`${rawArg}_in`] as unknown as string[]).includes(
      child?.[rawArg] as string
    );
  }
  if (isMatch && where_?.[`${rawArg}_not_in`]) {
    isMatch = !(where_[`${rawArg}_not_in`] as unknown as string[]).includes(
      child?.[rawArg] as string
    );
  }
  if (isMatch && where_?.[`${rawArg}_lt`]) {
    if (!Number.isNaN(parseFloat(where_[`${rawArg}_lt`] as string))) {
      isMatch = BigDecimal.lt(
        (child?.[rawArg] as string) || "0.0",
        where_[`${rawArg}_lt`] as string
      );
    } else {
      isMatch = (child?.[rawArg] as string) < where_[`${rawArg}_lt`];
    }
  }
  if (isMatch && where_?.[`${rawArg}_lte`]) {
    if (!Number.isNaN(parseFloat(where_[`${rawArg}_lte`] as string))) {
      isMatch = BigDecimal.lte(
        (child?.[rawArg] as string) || "0.0",
        where_[`${rawArg}_lte`] as string
      );
    } else {
      isMatch = (child?.[rawArg] as string) <= where_[`${rawArg}_lte`];
    }
  }
  if (isMatch && where_?.[`${rawArg}_not`]) {
    isMatch = (child?.[rawArg] as string) !== where_[`${rawArg}_not`];
  }
  if (isMatch && where_?.[`${rawArg}_ends_with`]) {
    const split = (child?.[rawArg] as string).split(
      new RegExp(
        `${(where_[`${rawArg}_ends_with`] as string).replace(
          /[/\-\\^$*+?.()|[\]{}]/g,
          "\\$&"
        )}$`
      )
    );
    isMatch = split.length === 2 && split[1].length === 0;
  }
  if (isMatch && where_?.[`${rawArg}_ends_with_nocase`]) {
    const split = (child?.[rawArg] as string)
      .toLowerCase()
      .split(
        new RegExp(
          `${(where_[`${rawArg}_ends_with_nocase`] as string)
            .toLowerCase()
            .replace(/[/\-\\^$*+?.()|[\]{}]/g, "\\$&")}$`
        )
      );
    isMatch = split.length === 2 && split[1].length === 0;
  }
  if (isMatch && where_?.[`${rawArg}_starts_with`]) {
    const split = (child?.[rawArg] as string).split(
      new RegExp(
        `^${(where_[`${rawArg}_starts_with`] as string).replace(
          /[/\-\\^$*+?.()|[\]{}]/g,
          "\\$&"
        )}`
      )
    );
    isMatch = split.length === 2 && split[0].length === 0;
  }
  if (isMatch && where_?.[`${rawArg}_starts_with_nocase`]) {
    const split = (child?.[rawArg] as string)
      .toLowerCase()
      .split(
        new RegExp(
          `${(where_[`${rawArg}_starts_with_nocase`] as string)
            .toLowerCase()
            .replace(/[/\-\\^$*+?.()|[\]{}]/g, "\\$&")}$`
        )
      );
    isMatch = split.length === 2 && split[0].length === 0;
  }
  if (isMatch && where_?.[`${rawArg}_not_ends_with`]) {
    const split = (child?.[rawArg] as string).split(
      new RegExp(
        `${(where_[`${rawArg}_not_ends_with`] as string).replace(
          /[/\-\\^$*+?.()|[\]{}]/g,
          "\\$&"
        )}$`
      )
    );
    isMatch = !(split.length === 2 && split[1].length === 0);
  }
  if (isMatch && where_?.[`${rawArg}_not_ends_with_nocase`]) {
    const split = (child?.[rawArg] as string)
      .toLowerCase()
      .split(
        new RegExp(
          `${(where_[`${rawArg}_not_ends_with_nocase`] as string)
            .toLowerCase()
            .replace(/[/\-\\^$*+?.()|[\]{}]/g, "\\$&")}$`
        )
      );
    isMatch = !(split.length === 2 && split[1].length === 0);
  }
  if (isMatch && where_?.[`${rawArg}_not_starts_with`]) {
    const split = (child?.[rawArg] as string).split(
      new RegExp(
        `^${(where_[`${rawArg}_not_starts_with`] as string).replace(
          /[/\-\\^$*+?.()|[\]{}]/g,
          "\\$&"
        )}`
      )
    );
    isMatch = !(split.length === 2 && split[0].length === 0);
  }
  if (isMatch && where_?.[`${rawArg}_not_starts_with_nocase`]) {
    const split = (child?.[rawArg] as string)
      .toLowerCase()
      .split(
        new RegExp(
          `${(where_[`${rawArg}_not_starts_with_nocase`] as string)
            .toLowerCase()
            .replace(/[/\-\\^$*+?.()|[\]{}]/g, "\\$&")}$`
        )
      );
    isMatch = !(split.length === 2 && split[0].length === 0);
  }

  return isMatch;
};

// resolve matches for the child against the where clause
export const checkFilterClause = (
  matched: boolean,
  child: EntityRecord | string,
  where_: Where | string
) => {
  // keep track of if we've matched or not
  let isMatch = matched;

  // is equal to...
  if (isMatch && where_ && typeof where_ !== "object") {
    // literal check on field
    isMatch =
      (typeof child === "string" &&
        typeof where_ === "string" &&
        child?.toLowerCase() === where_?.toLowerCase()) ||
      child === where_;
  } else if (isMatch && where_ && typeof where_ === "object") {
    // nested check on where_ clause...
    isMatch = Object.keys(where_).reduce((match, arg) => {
      // reassign to avoid reassigning props of params
      let isMatched = match;
      // get the raw argument
      const rawArg = ALL_FILTERS.reduce(
        (prop, filter) => prop.replace(filter, ""),
        arg
      );
      // when arg points to terminal value...
      if (typeof where_[arg] !== "object") {
        // we need to match against the discovered content - if array then filter - else check literal
        if (Array.isArray(child)) {
          // many children...
          isMatched =
            match &&
            child.reduce(
              (match2: boolean, nestedChild: Record<string, unknown>) =>
                match2 && resolveFilters(match2, nestedChild, where_, rawArg),
              match
            );
        } else if (child) {
          // literal match in where_ clause...
          isMatched =
            match &&
            resolveFilters(match, child as EntityRecord, where_, rawArg);
        }
      } else if (match && typeof where_[arg] === "object") {
        // resolve nested
        isMatched = (match &&
          child.length &&
          (child as unknown as Record<string, unknown>[]).reduce(
            (match2: boolean, _child: Record<string, unknown>) =>
              match2 && resolveFilters(match2, _child, where_, rawArg),
            match
          )) as boolean;
      }

      // did we match?
      return isMatched;
    }, isMatch as boolean);
  }

  return isMatch;
};
