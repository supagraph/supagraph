/* eslint-disable no-nested-ternary, no-underscore-dangle, no-restricted-syntax */
/* eslint-disable no-await-in-loop, @typescript-eslint/naming-convention */
import {
  Key,
  Where,
  Entities,
  Operation,
  SimpleSchema,
  EntityRecord,
  Args,
  WhereInterface,
} from "./types";

// perform filters on the resolved entity result
import { checkFilterClause, resolveFilters } from "./where";

// async filter method
export const filter = async (
  arr: { [key: string]: unknown }[],
  callback: (item: { [key: string]: unknown }) => Promise<unknown>
): Promise<{ [key: string]: unknown }[]> => {
  // eslint-disable-next-line symbol-description
  const falsy = Symbol();
  return (
    await Promise.all(
      arr.map(async (item) => ((await callback(item)) ? item : falsy))
    )
  ).filter((i) => i !== falsy) as { [key: string]: unknown }[];
};

// perform a case-insensitve match against the left and right (checking for objects on the rhs)
export const caseInsensitiveMatch = (
  left: string,
  right: string | { id: string } | { id: string }[]
) =>
  // derived side will always be linked via an ID ref
  left?.toLowerCase() ===
  // check that it matches the target side
  (Array.isArray(right) && right[0].id
    ? right[0].id?.toLowerCase()
    : typeof right === "object" && (right as { id: string }).id
    ? (right as { id: string }).id?.toLowerCase()
    : typeof right === "string"
    ? right?.toLowerCase()
    : right);

// for SingularQueries we expect one result and it must match against the required id arg
export const createSingularQuery = (
  entities_: () => Entities | Promise<Entities>,
  _schema: SimpleSchema,
  key: Key
) => {
  // clean any multi/required markers from the key.type
  const entity = key.type.replace(/\[|\]|!/g, "");

  // return a runtime resolver to filter to a single entity
  return async (...props: [EntityRecord, Args, unknown, unknown]) => {
    // pull parent & args - these are the run time state props relating to our current position/query
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const [_parent, args] = props;

    // resolve entities (they might not be given as a promise but we resolve before using just in case)
    const _entities = await Promise.resolve(entities_());

    // default to this being an empty array of entities if unavailable
    let from = (await Promise.resolve(_entities[entity])) || [];

    // use a function call to get the entities
    if (typeof from === "function") {
      from = (
        await Promise.resolve(
          Object.values(
            (await from(key, props, Operation.FIND)) || []
          ) as Record<string, unknown>[]
        )
      ).filter((v) => v);
    }

    // resolve any indexed keys
    if (!Array.isArray(from)) {
      from = Object.values(from || {}) || [];
    }

    // use the required ID field to find the entity
    const match: {
      [key: string]: unknown[] | string | unknown;
    } | null =
      from?.find((item) => {
        return (
          (item.id as string)?.toLowerCase?.() ===
          (args.id as string)?.toLowerCase?.()
        );
      }) || null;

    return match;
  };
};

// for MultiQueries we expect an array of results and they can be paginated, filtered and sorted
export const createMultiQuery = (
  entities_: () => Entities | Promise<Entities>,
  _schema: SimpleSchema,
  key: Key
) => {
  // clean any multi/required markers from the key.type
  const entity = key.type.replace(/\[|\]|!/g, "");

  // return a runtime resolver to filter to multiple entities
  return async (...props: [EntityRecord, Args, unknown, unknown]) => {
    // if we resolve the values we should limit the result
    let disableSkips = false;
    // pull parent & args - these are the run time state props relating to our current position/query
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const [_, args, context, ast] = props;

    // resolve entities (they might not be given as a promise but we resolve before using just in case)
    const _entities = await Promise.resolve(entities_());

    // default to this being an empty array of entities if unavailable
    let from = (await Promise.resolve(_entities[entity])) || [];

    // use a function call to get the entities
    if (typeof from === "function") {
      // disable limits in this query (read from an attribute on the resolver function)
      disableSkips = (from as unknown as { disableSkips: boolean })
        .disableSkips;
      // resolve the from
      from = await Promise.resolve(
        from(key, props, Operation.FILTER) as Record<string, unknown>[]
      );
    }

    // resolve any indexed keys
    if (!Array.isArray(from)) {
      from = Object.values(from || {}) || [];
    }

    // shallow copy all of the results in to matches
    let matches: {
      [key: string]: unknown[] | unknown;
    }[] = [...((from as Record<string, unknown>[]) || [])];

    // extract pagination args
    const { first, skip, orderBy, orderDirection } = args as unknown as {
      first: string;
      skip: string;
      orderBy: string;
      orderDirection: "asc" | "desc";
    };

    // perform any filtering...
    if (args.where) {
      // async filter so that we can perform joins as we filter
      matches = await filter(
        matches,
        async (child: Record<string, unknown>) => {
          // assume a match unless one of the where clauses excludes it...
          let isMatch = true;

          // shallow copy of child so that we dont modify the source
          const clone = { ...child } as Record<string, unknown>;

          // check each of the keys against any where clauses...
          for (const key_ of _schema[entity]) {
            if (typeof key_ !== "string") {
              // get the join type without multi markers
              const joinType = key_.type.replace(/\[|\]|!/g, "");
              // collect where clause on nested property
              // - we only need to join the children at this level if a where clause exists to filter the parent based on the childs content
              const where =
                args.where &&
                (args.where[`${key_.name}_`] || args.where[`${key_.name}`]);

              // check for un-entitied children - we will need the full set to resolve the where clause
              if (clone[key_.name] && _entities[key_.type] && where) {
                // gather resolver for entity type
                const query = createSingularQuery(entities_, _schema, key_);
                // resolve child
                clone[key_.name] = [
                  await query(
                    clone,
                    {
                      id: clone[key_.name] as string,
                      where: where as WhereInterface,
                    },
                    context,
                    ast
                  ),
                ] as Record<string, unknown>[];
              } else if (!clone[key_.name] && _entities[joinType] && where) {
                // gather resolver for multi entity type
                const query = createMultiQuery(entities_, _schema, key_);
                // resolve children
                clone[key_.name] = (await query(
                  clone,
                  { where: where as Where },
                  context,
                  ast
                )) as unknown as Record<string, unknown>[];
              }

              // if theres a where clause matching the newly constructed entity list, then check it for matches
              if (Array.isArray(clone[key.name]) && where) {
                // check for nested filter clauses
                isMatch = checkFilterClause(
                  isMatch,
                  clone[key_.name] as Record<string, unknown>,
                  where as WhereInterface
                );
              }

              // resolve any filters applied at the root of the clause
              isMatch = resolveFilters(
                isMatch,
                clone as Record<string, unknown>,
                args.where as Where,
                key_.name
              );
            }
          }

          return isMatch;
        }
      );
    }

    // apply the order (direction & orderBy - both are enums)
    if (orderBy || orderDirection) {
      // fix the direction by prefixing a negation (treat all sorts as numric)
      const direction = orderDirection === "desc" ? "-" : "";
      // sort the matches by the given order
      matches = matches.sort((a: EntityRecord, b: EntityRecord) =>
        // holds a string value - sort using localeCompare
        orderBy
          ? parseFloat(
              direction +
                String((a[orderBy as string] as string) || "").localeCompare(
                  String((b[orderBy as string] as string) || ""),
                  undefined,
                  { numeric: true }
                )
            )
          : // holds a single entity - sort on the entities id
          orderBy &&
            typeof (a[orderBy as string] as { id: string })?.id === "string"
          ? parseFloat(
              direction +
                (a[orderBy as string] as { id: string }).id.localeCompare(
                  (b[orderBy as string] as { id: string }).id as string,
                  undefined,
                  { numeric: true }
                )
            )
          : // holds array of items (sort based on length)
          orderBy &&
            Array.isArray(a[orderBy as string]) &&
            Array.isArray(b[orderBy as string])
          ? parseFloat(
              direction +
                ((a[orderBy as string] as unknown[]).length -
                  (b[orderBy as string] as unknown[]).length)
            )
          : // default to string/id
            parseFloat(
              direction +
                (
                  (typeof a === "string" ? a : a?.id || "0") as string
                ).localeCompare(
                  (typeof b === "string" ? b : b?.id || "0") as string
                )
            )
      );
    }

    // take a shallow copy of the matches to manipulate into result set
    let results = [...matches];

    // disable limits and offset if this has already been applied in the resolver
    if (!disableSkips) {
      // limit the result
      if (typeof skip !== "undefined") {
        results.splice(0, parseInt(skip as string, 10) || 0);
      }
      // never show more than 500 entries so as not to trigger the exceeds 4mb limit of nextjs
      if (typeof first !== "undefined" || results.length > 500) {
        results = results.slice(
          0,
          Math.min(500, parseInt(first || "25", 10) || 25)
        );
      }
    } else if (results.length > Math.min(500, parseInt(first, 10) || 500)) {
      // never show more than 500 entries so as not to trigger the exceeds 4mb limit of nextjs
      if (typeof first !== "undefined" || results.length > 500) {
        results = results.slice(0, Math.min(500, parseInt(first, 10) || 500));
      }
    }

    return results;
  };
};

// tidy up a query
export const tidyDefaultQuery = (query: string) => {
  // want to remove some leading whitespace
  return query
    .split("\n")
    .map((i) => i.replace(/\s{4}/, ""))
    .join("\n")
    .replace(/^\n/, "");
};
