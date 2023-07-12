import { DocumentNode } from "graphql";

import { Plugin, YogaInitialContext, YogaServerInstance } from "graphql-yoga";
import { FILTER_TYPE_MAP } from "./constants";

export type { parse } from "graphql";

export interface WhereInterface {
  [key: string]: number | string | WhereInterface;
}
export type Where = WhereInterface;

export type EntityRecord = Record<
  string,
  string | { id: string } | unknown | unknown[]
>;

export type Args = {
  id?: string;
  where?: Where;
  skip?: string;
  first?: string;
  orderBy?: string;
  orderDirection?: string;
};

export enum Operation {
  "FIND" = "find",
  "FILTER" = "filter",
}

export type Entities = Record<
  string,
  | Record<string, unknown>[]
  | ((
      key: Key,
      props: [EntityRecord, Args, unknown, unknown],
      operation: Operation
    ) => Record<string, unknown>[] | Promise<Record<string, unknown>[]>)
>;

export type Key = {
  name: string;
  type: keyof typeof FILTER_TYPE_MAP;
  derivedFrom?: string;
};

export declare type SimpleSchema = Record<string, Key[] | string>;

export declare const createSupagraph: <
  TServerContext extends Record<string, any>,
  TUserContext extends Record<string, any>
>({
  schema,
  entities,
  plugins,
  graphqlEndpoint,
  name,
  defaultQuery,
}: {
  schema: DocumentNode;
  entities: Entities | Promise<Entities>;
  plugins?: Array<
    Plugin<YogaInitialContext> | Plugin | Record<string, unknown>
  >;
  graphqlEndpoint?: string;
  name?: string;
  defaultQuery?: string;
}) => YogaServerInstance<TServerContext, TUserContext>;
