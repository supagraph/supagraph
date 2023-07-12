/* eslint-disable no-param-reassign, no-underscore-dangle */
/* eslint-disable @typescript-eslint/naming-convention */
import {
  createSchema,
  createYoga,
  Plugin,
  YogaInitialContext,
  YogaServerInstance,
} from "graphql-yoga";
import { GraphQLByte, GraphQLTimestamp } from "graphql-scalars";

import { DocumentNode, parse } from "graphql";
import { ServerResponse } from "http";
import { GraphQLBigInt } from "./scalars/BigInt";
import { GraphQLBigDecimal } from "./scalars/BigDecimal";

import { toPlural } from "../utils/toPlural";
import { toCamelCase } from "../utils/toCamelCase";

import { createDefs, readSchema } from "./schema";
import {
  caseInsensitiveMatch,
  createMultiQuery,
  createSingularQuery,
  tidyDefaultQuery,
} from "./queries";

import {
  Key,
  Args,
  Entities,
  Operation,
  EntityRecord,
  SimpleSchema,
} from "./types";

// Only these value types can be supplied as schema'd types
import { VALUE_TYPES } from "./constants";

// Use our a wrapper around renderGraphiQL to set icons etc...
import { renderGraphiQL } from "./graphiql";

// Mongo helpers
export { createQuery, unwindResult } from "./mongo";

// Export default resolvers for mongo/in-memory(tmp) db stores
export { mongoResolver, memoryResolver } from "./resolvers";

// Opeartion enum
export { Operation } from "./types";

// expose types
export type {
  Key,
  Args,
  Where,
  Entities,
  EntityRecord,
  SimpleSchema,
} from "./types";

// expose graphqls parse directly
export { parse } from "graphql";

// convert arrays to types
export type ArrayElementType<T> = T extends ReadonlyArray<infer U> ? U : never;
export type ElementTypeOfArray<T extends ReadonlyArray<any>> = ArrayElementType<
  T[number]
>;

// create new yoga definition tied to entities collection(s)
export const createSupagraph = <
  TServerContext extends Record<string, any>,
  TUserContext extends Record<string, any>
>({
  schema,
  entities,
  plugins,
  graphqlEndpoint,
  name,
  icon,
  title,
  defaultQuery,
  headers,
  revalidate = 0,
}: {
  schema: DocumentNode | string;
  entities:
    | Entities
    | Promise<Entities>
    | ((
        schema: SimpleSchema,
        typeDefs: string
      ) => Entities | Promise<Entities>);
  plugins?: Array<
    Plugin<YogaInitialContext> | Plugin | Record<string, unknown>
  >;
  graphqlEndpoint?: string;
  icon?: string;
  title?: string;
  name?: string;
  defaultQuery?: string;
  headers?: Record<string, string>;
  revalidate?: number;
}): YogaServerInstance<TServerContext, TUserContext> => {
  // read the original schema
  const _schema = readSchema(
    typeof schema === "string" ? parse(schema) : schema
  );

  // construct new definitions
  const typeDefs = createDefs(_schema);

  // we can't await this now, but we can replace the entities with a promise to be resolved before we start extracting data ...
  const _entities = async () =>
    Promise.resolve().then(async () => {
      // use revalidation time to kill the cached entities
      if (
        revalidate &&
        (entities as Entities & { __revalidated: Date }).__revalidated &&
        (
          entities as Entities & { __revalidated: Date }
        ).__revalidated.getTime() +
          revalidate * 1000 <
          new Date().getTime()
      ) {
        // clear the resolution to force revalidation
        delete (entities as { __resolved?: Awaited<Entities> }).__resolved;
      }

      // were going to resolve the entities (optionally provided as a fn or a promise) then default any missing values
      (entities as Entities & { __resolved: Awaited<Entities> }).__resolved =
        (entities as Entities & { __resolved: Awaited<Entities> }).__resolved ||
        (await new Promise((resolve_) => {
          // store last validated time
          (entities as Entities & { __revalidated: Date }).__revalidated =
            new Date();
          // resolve the entities by calling/resolving
          resolve_(
            typeof entities !== "function"
              ? entities
              : entities(_schema, typeDefs)
          );
        }));
      // we'll use the schema to guide the properties we set in the resolved entities
      return Object.keys(_schema)
        .filter((entity) => {
          return (
            !entity.match(/-plural-form$/) && !entity.match(/-single-form$/)
          );
        })
        .reduce((finalEntities, entity) => {
          // only include entities defined in the schema
          finalEntities[entity] =
            (entities as Entities & { __resolved: Awaited<Entities> })
              .__resolved[entity] || [];
          return finalEntities;
        }, {} as Entities);
    }) as Promise<Entities>;

  // define a new server from the discovered schema and entities
  const handler = createYoga<TServerContext, TUserContext>({
    landingPage: false,
    plugins: plugins || [],
    graphqlEndpoint:
      graphqlEndpoint || (name ? `/subgraphs/name/${name}` : `/graphql`),
    graphiql: {
      title,
      defaultQuery: defaultQuery ? tidyDefaultQuery(defaultQuery) : `query {}`,
    },
    // append options to the renderGraphiQL setup
    renderGraphiQL: (opts) =>
      renderGraphiQL({
        ...opts,
        // pass icon if defined
        icon,
        // set the playground page title...
        title: title || "Supagraph Playground",
      }),
    // construct the schema from the typeDefs and _schema
    schema: createSchema({
      typeDefs,
      resolvers: {
        Bytes: GraphQLByte,
        BigInt: GraphQLBigInt,
        BigDecimal: GraphQLBigDecimal,
        Timestamp: GraphQLTimestamp,
        Query: {
          _meta: () => ({}),
          ...Object.keys(_schema)
            .filter((entity) => {
              return (
                !entity.match(/-plural-form$/) && !entity.match(/-single-form$/)
              );
            })
            .reduce(
              (carr, entity) => {
                // lowerCase the first char on single/plural form
                const singleForm = toCamelCase(
                  (_schema[`${entity}-single-form`] as string) || entity
                );
                const pluralForm = toCamelCase(
                  (_schema[`${entity}-plural-form`] as string) ||
                    toPlural(entity)
                );

                // create resolvers to carryout the querying for each type (single/multi)
                carr[singleForm] = createSingularQuery(_entities, _schema, {
                  name: "id",
                  type: entity as ElementTypeOfArray<typeof VALUE_TYPES>,
                });
                carr[pluralForm] = createMultiQuery(_entities, _schema, {
                  name: "id",
                  type: entity as ElementTypeOfArray<typeof VALUE_TYPES>,
                });

                // returns all _schema defined query resolvers
                return carr;
              },
              {} as Record<
                string,
                (...props: [EntityRecord, Args, unknown, unknown]) => Promise<
                  | {
                      [key: string]: unknown;
                    }
                  | {
                      [key: string]: unknown;
                    }[]
                  | null
                >
              >
            ),
        },
        // place refs for recursive value resolution
        ...Object.keys(_schema)
          .filter((entity) => {
            return (
              !entity.match(/-plural-form$/) && !entity.match(/-single-form$/)
            );
          })
          .reduce((carr, entity) => {
            // create a resolver for each key on the entity to resolve joins in both directions
            carr[entity] = (_schema[entity] as Key[]).reduce((args, key) => {
              // check if we're joining to an Entity type
              const joinType = key.type.replace(/\[|\]|!/g, "");
              // check how we're going to perform the match (array of results - use filter - else use find)
              const operation =
                key.type[0] === "[" ? Operation.FILTER : Operation.FIND;

              // if we're resolving an entity type (will be defined in top level of the schema)
              if (_schema[joinType]) {
                // check for entity as mapped from the schema in the set-up
                args[key.name] = async (
                  parent: EntityRecord,
                  props: Args,
                  context: any,
                  ast: any
                ) => {
                  // get these in the clear (we make this a promise in the first step so we will always need to resolve it)
                  const resEntities = await Promise.resolve(_entities());

                  // default to this being an array of entities
                  let from = resEntities[joinType];

                  // use a function call to get the entities
                  if (typeof from === "function") {
                    from = await Promise.resolve(
                      from(key, [parent, props, context, ast], operation)
                    );
                  }

                  // if theres any filter criteria on the key.name
                  if (
                    args.where ||
                    args.skip ||
                    args.first ||
                    args.orderBy ||
                    args.orderDirection
                  ) {
                    // check for nested filter clauses and resolve through entity defined resolvers...
                    const query =
                      key.type[0] === "["
                        ? createMultiQuery(_entities, _schema, key)
                        : createSingularQuery(_entities, _schema, key);

                    // pass the query terms through the constructed and resolved resolvers
                    return query(
                      parent,
                      key.type[0] === "["
                        ? props
                        : ({ id: parent[key.name], ...props } as Args),
                      context,
                      ast
                    );
                  }

                  // we run either filter or find based on if we're finding an array of matches or a singular match
                  return from[operation]((item) => {
                    // check the items derived field matches the parent field
                    if (key.derivedFrom) {
                      return (
                        item &&
                        caseInsensitiveMatch(
                          typeof item[key.derivedFrom] === "object"
                            ? (item[key.derivedFrom] as { id: string }).id
                            : (item[key.derivedFrom] as string),
                          parent as string | { id: string }
                        )
                      );
                    }
                    // check the item.id matches the parents key.name value
                    return (
                      item &&
                      caseInsensitiveMatch(
                        item.id as string,
                        (typeof parent === "object"
                          ? parent[key.name]
                          : parent) as string | { id: string }
                      )
                    );
                  });
                };
              }
              return args;
            }, {} as Record<string, unknown>);
            return carr;
          }, {} as Record<string, unknown>),
      },
    }),
  });

  // add the provided headers to a response
  const addHeaders = (hres: Response) => {
    // add to return response
    if (headers && hres && hres.headers) {
      Object.keys(headers).forEach((key) => {
        hres.headers.set(key, headers[key]);
      });
    }
    return hres;
  };

  // wrap the handler to allow for the addition of headers via setHeader
  function handle(
    ...args: Parameters<typeof handler>
  ): Promise<ReturnType<typeof handler>> | ReturnType<typeof handler> {
    // add to serverResponse before calling
    if (
      headers &&
      args[1] &&
      (args[1] as unknown as ServerResponse).setHeader &&
      typeof (args[1] as unknown as ServerResponse).setHeader === "function"
    ) {
      Object.keys(headers).forEach((key) => {
        (args[1] as unknown as ServerResponse).setHeader(key, headers[key]);
      });
    }
    // run the handler
    const hres = handler(...args);

    // if its a promise, resolve and add headers to the response
    if (hres instanceof Promise) {
      return hres.then((res) => addHeaders(res));
    }

    // add headers to the response immediately
    return addHeaders(hres);
  }

  return handle as typeof handler;
};
