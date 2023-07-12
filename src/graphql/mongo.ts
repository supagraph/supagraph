/* eslint-disable no-restricted-syntax, @typescript-eslint/no-use-before-define */

// Import types from graphql
import type {
  FieldNode,
  ArgumentNode,
  DirectiveNode,
  SelectionNode,
  FragmentDefinitionNode,
  GraphQLResolveInfo,
} from "graphql";

// General supagraph typings and constants
import { Key, SimpleSchema } from "./types";

// Only these types may defined as schema'd types
import { VALUE_TYPES } from "./constants";

// We want to control the casing of the entities as we collect them
import { toCamelCase } from "../utils/toCamelCase";

// Interface graphql filters
export interface GraphqlFilter {
  [key: string]: any;
}

// Partial type to wrap objects as we merge
export type DeepPartial<T> = {
  [P in keyof T]?: DeepPartial<T[P]>;
};

// Interface for a fragment item
export interface FragmentItem {
  [name: string]: FragmentDefinitionNode;
}

// Interface for field name mapping
export interface FieldNamesMap {
  [name: string]: string;
}

// Interface for field list options
export interface FieldsListOptions {
  path?: string;
  transform?: FieldNamesMap;
  withDirectives?: boolean;
  keepParentField?: boolean;
  variables?: Record<string, unknown>;
  skip?: string[];
  map?: MapResult;
}

// Interface for variable values
export interface VariablesValues {
  [name: string]: any;
}

// Interface for field projection
export interface FieldsProjection {
  [name: string]: 1;
}

// Type definitions for MapResult and MapResultKey
export type MapResult = { [key: string]: MapResultKey };
export type MapResultKey = false | MapResult;

// Type definitions for SkipValue and SkipMap
export type SkipMap = { [key: string]: SkipValue };
export type SkipValue = boolean | SkipMap;

// WalkTreeOptions interface
export interface WalkTreeOptions {
  fragments: FragmentItem;
  vars: any;
  withVars?: boolean;
}

// Mapping to allow for conversion from graphql filter to MongoOperator
export const graphqlToMongoOperatorMap: Record<string, string> = {
  // numerical filters
  _gt: "$gt",
  _gte: "$gte",
  _in: "$in",
  _lt: "$lt",
  _lte: "$lte",
  _not: "$ne",
  _not_in: "$nin",

  // string filters
  _contains: "$regex",
  _not_contains: "$not",
  _starts_with: "$regex",
  _ends_with: "$regex",

  // other filters
  _not_contains_nocase: "$not",
  _not_ends_with_nocase: "$not",
  _not_ends_with: "$not",
  _not_starts_with_nocase: "$not",
  _not_starts_with: "$not",
  _ends_with_nocase: "$regex",
  _starts_with_nocase: "$regex",
  _contains_nocase: "$regex",
};

// Mapping of flags so that we can switch to case insensitive when we need to
export const regexFlagsMap: Record<string, string> = {
  _contains_nocase: "i",
  _not_contains_nocase: "i",
  _starts_with_nocase: "i",
  _ends_with_nocase: "i",
};

// Wildcard replacement regexp
const REPLACEMENT_REGEXP = /\*/g;

// Perform a deep merge on two objects (copy right to left)
function deepMerge<T>(target: T, source: DeepPartial<T>): T {
  const merged: T = { ...target };

  // iterate every key in the source and apply it to the target and recurse on any objects
  for (const key in source) {
    if (Object.prototype.hasOwnProperty.call(source, key)) {
      const sourceValue = source[key];
      const targetValue = merged[key];

      // if the source is an array we can place by key, recursive deepMerge on objects
      if (
        typeof sourceValue === "object" &&
        !Array.isArray(sourceValue) &&
        sourceValue !== null
      ) {
        // source is an object...
        if (
          typeof targetValue === "object" &&
          !Array.isArray(targetValue) &&
          targetValue !== null
        ) {
          // merge all branches
          merged[key] = deepMerge(targetValue, sourceValue);
        } else {
          // start the target fresh
          merged[key] = deepMerge(
            {} as T[Extract<keyof T, string>],
            sourceValue
          );
        }
      } else {
        // should we be deepMerging objects in arrays? (if target === object)
        merged[key] = sourceValue as T[Extract<keyof T, string>];
      }
    }
  }

  // return all merged objects
  return merged;
}

// Retrieve child nodes from a selection node or fragment definition
function getNodes(
  selection: FragmentDefinitionNode | SelectionNode
): ReadonlyArray<SelectionNode> {
  return (
    (selection as any)?.selectionSet?.selections ||
    ([] as ReadonlyArray<SelectionNode>)
  );
}

// Get a specific branch from the tree based on the path
function getNode(tree: MapResult, path?: string): MapResult {
  // default res to the full tree
  let res = tree;

  // if no path is provided, return the entire tree
  if (!path) {
    return res;
  }

  // iterate over each field name in the path
  for (const fieldName of path.split(".")) {
    // retrieve the branch corresponding to the field name
    const branch = (res as Record<string, any>).fields[fieldName];

    // if the branch doesn't exist, return an empty object
    if (!branch) {
      return {};
    }

    // update the tree to the branch for the next iteration
    res = branch;
  }

  return res;
}

// Retrieve arguments from a selection node or fragment definition
function getArgs(
  selection: FragmentDefinitionNode | SelectionNode,
  variables: Record<string, unknown> = {}
): Record<string, any> {
  // console.log(JSON.stringify(selection, null, 2));
  return (
    (selection as any)?.arguments || ([] as ReadonlyArray<ArgumentNode>)
  ).reduce(function doReduce(
    carr: { [x: string]: any },
    arg: {
      name: { value: string | number };
      value: {
        name?: any;
        kind: string;
        value: any;
        values?: { [x: string]: any }[];
        fields?: { [x: string]: any }[];
      };
    }
  ): { [x: string]: any } {
    // console.log(arg.name.value, !arg.value.value && arg.value.fields);
    return {
      ...carr,
      [arg.name.value]:
        (arg.value.kind === "Variable"
          ? variables[arg.value.name.value]
          : arg.value.value) ||
        (arg.value.kind === "ListValue" &&
          arg.value.values &&
          arg.value.values.map((val) => {
            return val.value;
          })) ||
        arg.value.fields?.reduce((nested, child) => {
          return doReduce(
            nested,
            child as {
              name: { value: string | number };
              value: {
                kind: string;
                value: any;
                values?: { [x: string]: any }[];
                fields?: { [x: string]: any }[];
              };
            }
          );
        }, {} as { [x: string]: any }),
    };
  }, {} as Record<string, any>);
}

// Generate a map of the selected fields from GraphQLResolveInfo
function fieldsMap(
  info: GraphQLResolveInfo,
  options?: FieldsListOptions
): MapResult {
  // verify the GraphQLResolveInfo object and retrieve the field node
  const fieldNode = verifyInfo(info);

  // if the field node is not available, return an empty object
  if (!fieldNode) {
    return {};
  }

  // parse the options to extract path, withDirectives, and skip values
  const { path, withDirectives, skip, variables } = parseOptions(options);

  // construct the initial tree structure
  const tree = {
    // place the top level args into the structure
    args: getArgs(fieldNode, variables),
    // walk the tree collecting nodes and args
    fields: walkTree(
      getNodes(fieldNode),
      {},
      {
        fragments: info.fragments,
        vars: info.variableValues || variables,
        withVars: withDirectives,
      },
      skipMap(skip || [])
    ),
  };

  // retrieve the desired node from the tree based on the specified path
  return getNode(tree as MapResult, path);
}

// Create a list of the selected fields from GraphQLResolveInfo
function fieldsList(
  info: GraphQLResolveInfo,
  options: FieldsListOptions = {}
): string[] {
  const { map, path } = parseOptions(options);
  // map from the given fields
  const fieldMap =
    ((map && path && getNode(map, path)) || map)?.fields ||
    fieldsMap(info, options).fields;

  // reconstruct the fields with any transforms
  return Object.keys(fieldMap || {}).map(
    (field: string) => (options.transform || {})[field] || field
  );
}

// Create a list of the selected args from GraphQLResolveInfo
function fieldsArgs(
  info: GraphQLResolveInfo,
  options: FieldsListOptions = {}
): Record<string, any> {
  const { map, path } = parseOptions(options);
  // map from the given fields
  const argMap =
    ((map && path && getNode(map, path)) || map)?.args ||
    fieldsMap(info, options).args;

  // reconstruct the args with any transforms
  return Object.keys(argMap || {}).reduce(
    (carr: Record<string, any>, field: string) => {
      return {
        ...carr,
        [field]:
          (options.transform || {})[field] ||
          (argMap as Record<string, any>)[field],
      };
    },
    {} as Record<string, any>
  );
}

// Extract values with either are or are not entities
function getValues(schema: SimpleSchema, entity: string, matches = true) {
  return (
    (schema[entity] as Key[])?.filter((vals) => {
      return vals.type && (VALUE_TYPES.indexOf(vals.type) === -1) !== matches;
    }) || []
  );
}

// Check the value of a directive argument
function checkValue(name: string, value: boolean): boolean {
  return name === "skip" ? !value : (name === "include" && value) || true;
}

// Create a skip tree based on skip patterns
function skipMap(skip: string[]): SkipMap {
  const tree: SkipMap = {};

  // iterate over each skip pattern
  for (const pattern of skip) {
    const props = pattern.split(".");
    let propTree: SkipMap = tree;

    // iterate over each property in the pattern
    for (let i = 0, s = props.length; i < s; i += 1) {
      const prop = props[i];
      const all = props[i + 1] === "*";

      // create the property tree if it doesn't exist
      if (!propTree[prop]) {
        propTree[prop] = i === s - 1 || all ? true : {};
        if (all) i += 1;
      }

      propTree = propTree[prop] as SkipMap;
    }
  }

  return tree;
}

// Verify if a node should be skipped based on skip patterns
function verifySkip(node: string, skip: SkipValue): SkipValue {
  // if skip is falsy, return false to indicate the node should not be skipped
  if (!skip) {
    return false;
  }

  // check if the node exists in the skip map
  if ((skip as SkipMap)[node]) {
    return (skip as SkipMap)[node];
  }

  let nodeTree: SkipValue = false;
  const patterns = Object.keys(skip).filter((pattern) => pattern.includes("*"));

  // iterate over patterns that include wildcard "*"
  for (const pattern of patterns) {
    const replacement: RegExp = new RegExp(
      pattern.replace(REPLACEMENT_REGEXP, ".*")
    );

    // check if the node matches the pattern
    if (replacement.test(node)) {
      nodeTree = (skip as SkipMap)[pattern];

      // if the pattern matches and the node tree is true, skip the node
      if (nodeTree === true) {
        break;
      }
    }
  }

  return nodeTree;
}

// Verify an inline fragment and walk its child nodes
function verifyFragment(
  node: SelectionNode,
  root: MapResult | MapResultKey,
  opts: WalkTreeOptions,
  skip: SkipValue
): boolean {
  // check if the node is an inline fragment
  if (node.kind === "InlineFragment") {
    // retrieve the child nodes of the inline fragment
    const nodes = getNodes(node);

    // if there are child nodes, recursively walk the tree using them
    if (nodes.length) {
      walkTree(nodes, root, opts, skip);
    }

    // return true to indicate successful verification
    return true;
  }

  // if the node is not an inline fragment, return false
  return false;
}

// Verify and retrieve the field node from GraphQLResolveInfo
function verifyInfo(info: GraphQLResolveInfo): SelectionNode | null {
  const res = info;

  // nothing to verify...
  if (!res) {
    return null;
  }

  // if 'fieldNodes' property is missing but 'fieldASTs' property is present (backward compatibility), assign 'fieldASTs' to 'fieldNodes'
  if (!res.fieldNodes && (res as any).fieldASTs) {
    (res as any).fieldNodes = (res as any).fieldASTs;
  }

  if (!res.fieldNodes) {
    return null;
  }

  // call the 'verifyFieldNode' function and return its result
  return verifyFieldNode(res);
}

// Verify and retrieve the field node with a matching field name
function verifyFieldNode(info: GraphQLResolveInfo): FieldNode | null {
  const fieldNode: FieldNode | undefined = info.fieldNodes.find(
    (node: FieldNode) => node && node.name && node.name.value === info.fieldName
  );

  // if the field node is not found or it doesn't have a selection set, return null
  if (!(fieldNode && fieldNode.selectionSet)) {
    return null;
  }

  // return the field node with a matching field name
  return fieldNode;
}

// Verify a directive
function verifyDirective(
  directive: DirectiveNode,
  vars: VariablesValues
): boolean {
  // retrieve the name of the directive
  const directiveName: string = directive.name.value;

  // if the directive is not "include" or "skip", no further verification is needed
  if (!["include", "skip"].includes(directiveName)) {
    return true;
  }

  // retrieve arguments on the directive
  let args: any[] = directive.arguments as any[];

  // if there are no arguments, initialize an empty array
  if (!(args && args.length)) {
    args = [];
  }

  // iterate over each argument and verify its validity
  for (const arg of args) {
    if (!verifyDirectiveArg(directiveName, arg, vars)) {
      // if any argument fails verification, return false
      return false;
    }
  }

  // all arguments passed verification, return true
  return true;
}

// Verify directives
function verifyDirectives(
  directives: ReadonlyArray<DirectiveNode> | undefined,
  vars: VariablesValues
): boolean {
  // if there are no directives or the directives array is empty, return true
  if (!directives || !directives.length) {
    return true;
  }

  // use the provided variables or initialize an empty object
  const useVars = vars || {};

  // iterate over each directive and verify its validity
  for (const directive of directives) {
    // if any directive fails verification, return false
    if (!verifyDirective(directive, useVars)) return false;
  }

  // all directives passed verification, return true
  return true;
}

// Verify the value of a directive argument
function verifyDirectiveArg(
  name: string,
  arg: ArgumentNode,
  vars: VariablesValues
): boolean {
  // eslint-disable-next-line default-case
  switch (arg.value.kind) {
    case "BooleanValue":
      return checkValue(name, arg.value.value);
    case "Variable":
      return checkValue(name, vars[arg.value.name.value]);
  }

  return true;
}

// Extract values using dot-delimited key
function getByDotNotation(
  obj: Record<string, unknown>[],
  keys: string[]
): any[] {
  // destructure the keys array to get the current key and the remaining keys
  const [currentKey, ...remainingKeys] = keys;

  // if there are no more remaining keys, we have reached the final key
  if (remainingKeys.length === 0) {
    // use flatMap to extract values from each object in the final branch
    return obj.flatMap((nestedObj) =>
      Array.isArray(nestedObj[currentKey])
        ? // if the value at the current key is an array, return the array as is
          (nestedObj[currentKey] as unknown[])
        : // otherwise, wrap the value in an array to ensure a consistent return type for the flatMap
          [nestedObj[currentKey]]
    );
  }

  // if there are remaining keys, recursively call the function on nested objects
  return obj.flatMap((nestedObj) =>
    getByDotNotation(
      nestedObj[currentKey] as Record<string, unknown>[],
      remainingKeys
    )
  );
}

// Parse the fields list options and set default values
function parseOptions(options?: FieldsListOptions): FieldsListOptions {
  const res = options;

  if (!res) {
    // If options is null or undefined, return an object with withDirectives set to true
    return {
      withDirectives: true,
    };
  }

  if (res.withDirectives === undefined) {
    // If withDirectives property is not specified, set it to true
    res.withDirectives = true;
  }

  // Return the parsed options object
  return res;
}

// Generate the MongoDB filter object based on the GraphQL filter object
function generateMongoFilter(filter: GraphqlFilter): Record<string, any> {
  const mongoFilter: Record<string, any> = {};

  // Iterate through each key in the filter object
  // eslint-disable-next-line guard-for-in
  for (const key in filter) {
    const value = filter[key];

    if (value !== undefined) {
      // Map the GraphQL operator to the corresponding MongoDB operator
      const operator = graphqlToMongoOperatorMap[key];

      if (operator) {
        if (operator === "$regex") {
          // Handle regex-based operators with special transformations
          const regexFlags = regexFlagsMap[key];
          let regexValue = value;

          if (key === "_contains" || key === "_not_contains") {
            regexValue = `.*${regexValue}.*`;
          } else if (key === "_starts_with" || key === "_not_starts_with") {
            regexValue = `^${regexValue}`;
          } else if (key === "_ends_with" || key === "_not_ends_with") {
            regexValue = `${regexValue}$`;
          }

          // Create a RegExp object with the transformed value and flags
          mongoFilter[operator] = new RegExp(regexValue, regexFlags);
        } else {
          // Assign the value directly for non-regex operators
          mongoFilter[operator] = value;
        }
      }
    }
  }

  return mongoFilter;
}

// Collate all args for all conditions in the ast marked against this entity - mutate useArgStore as a side-effect to allow lookbehinds
function collateWheres(
  schema: SimpleSchema,
  entity: string,
  ast: any,
  usePath?: string,
  useMap?: MapResult,
  useArgStore: Record<string, unknown> = {}
) {
  // start the current path with an empty response object
  const argWheres: Record<string, Key> = {};

  // should be indexed in the schema - we can use this to guide the query joins
  const map = useMap || fieldsMap(ast);
  // get the args at this level (we also want all args from prev levels to be collected)
  const args = fieldsArgs(ast, { map, path: usePath });
  // use the fields to guide what can be referenced as an entity
  const entityFields = getValues(schema, entity, false) || [];

  // place the args into the arg store by ref. (deepMerge to join at all depths)
  // eslint-disable-next-line no-param-reassign
  useArgStore[toCamelCase(entity)] = deepMerge(
    useArgStore[toCamelCase(entity)] || {},
    args.where || {}
  );

  // place the args into an object, when we follow the next branch, supply the args[entity] to move the collection deeper, as we discover entities we fill the forward checks
  entityFields.forEach((key) => {
    if (
      // I think we need to be matching the field name here instead of entities
      (useArgStore[toCamelCase(entity)] as Record<string, unknown>)?.[
        key.name
      ] ||
      args?.where?.[key.name] ||
      Object.keys(graphqlToMongoOperatorMap)
        .map((arg) => entity + arg)
        .find((argKey) => args?.where?.[argKey])
    ) {
      argWheres[key.type.replace(/\[|\]|!/g, "")] = key;
    }
  });

  // return as an array of keys (to be merged with entityFields to produce full query paths to resolve these wheres)
  return Object.values(argWheres);
}

// Create a match expression from a set of args.where args
function createArgs(args: Record<string, any>) {
  return {
    // deconstruct the where - this is going to need to join any descendents by dot-delims and construct $operations from filters
    $match: {
      ...Object.keys(args.where || []).reduce(
        // define a named function so we can keep track of the useKey as we recurse all descendents through this reduction
        (function doReduce(
          useKey: string,
          where: Record<string, unknown>
        ): (
          carr: Record<string, unknown>,
          argKey: string
        ) => Record<string, unknown> {
          // return a reduce function to collect all nested filters into a flat object
          return (all, key) => {
            // split the current key - if it has an _ in it then its a filter.
            const splitKey = key.split("_");
            // check if there is a filter available at the end of the current key
            const hasFilter =
              splitKey.length >= 2 &&
              graphqlToMongoOperatorMap[`_${splitKey[splitKey.length - 1]}`];

            // return all results
            return {
              // keep everything collected so far
              ...all,
              // spread all descendents into the same object
              ...(typeof where[key] !== "object"
                ? // check for a filter or a final match value
                  {
                    [`${useKey ? `${useKey}.` : ``}${
                      hasFilter ? key.substring(0, key.lastIndexOf("_")) : key
                    }`]: hasFilter
                      ? generateMongoFilter({
                          [`_${splitKey[splitKey.length - 1]}`]: where[key],
                        })
                      : where[key],
                  }
                : // check for nested filters defined in the query
                  {
                    // recurse through doReduce and hydrate everything in to the parent
                    ...Object.keys(
                      where[key] as Record<string, unknown>
                    ).reduce(
                      // establish position and call the doReduce method to flatten this level into the parent
                      doReduce(
                        // tag this key onto the splitKey
                        `${useKey ? `${useKey}.` : ``}${key}`,
                        // move to the next position in the object nest
                        where[key] as Record<string, unknown>
                      ),
                      // fill this object and spread it into the parent
                      {} as Record<string, unknown>
                    ),
                  }),
            };
          };
        })("", args.where),
        {}
      ),
    },
  };
}

// Produce a lookup aggregate stage for each nested entity in a query
function createLookup(
  schema: SimpleSchema,
  entity: string,
  mutable: boolean,
  ast: any,
  context: Record<string, Record<string, unknown>>,
  map: MapResult,
  useArgStore: Record<string, unknown> = {}
) {
  // return a map to return a lookup filter
  return (vals: Key): Record<string, any> => {
    // dot-delim path of current entity
    const path = entity ? `${entity}.${vals.name}` : vals.name;

    // this needs to be recursive to keep the full tree rendered
    return {
      $lookup: {
        from: toCamelCase(vals.type.replace(/\[|\]|!/g, "")),
        // supply the outer join field to the lookup pipeline
        let: {
          joinOn:
            // either this fields name or the id of the field we're deriving from
            (vals.type[0] !== "[" && `$${vals.name}`) ||
            // we're connecting to a nested entry grab from the derivedFrom field?
            (entity.indexOf(".") !== -1
              ? // grab the property where this key derives from
                `$${vals.derivedFrom}`
              : // if we're moving from the root then we'll be joining this from the id
                `$id`),
        },
        pipeline: [
          // create a query for this entity at the given path in the ast map
          ...createQuery(
            // full schema
            schema,
            // TitleCase entity name
            vals.type.replace(/\[|\]|!/g, ""),
            // pass mutable prop through
            mutable,
            // full ast
            ast,
            // the current query context
            context,
            // the path that we're currently working against
            path,
            // the full map
            map,
            // the condition under which we're joining the outer entity
            {
              $match: {
                $expr: {
                  $eq: [
                    vals.type[0] !== "[" ? "$id" : `$${vals.derivedFrom}`,
                    "$$joinOn",
                  ],
                },
              },
            },
            // use the store at the given position (I think)
            useArgStore,
            // pass derivedFrom field to add to projection for immutable projections
            vals.derivedFrom
          ),
        ].filter((v) => v),
        // use the path of the current reference to id the position
        as: path.replace(/\./g, "-"),
      },
    };
  };
}

// Recursive lookup to extract nested entities
function unwindLookup(
  schema: SimpleSchema,
  entity: string,
  result: any[],
  entities: Record<string, any>,
  entityIndex: Record<string, any>
) {
  // we return void from here because we're filling the entities and entityIndex recursively
  return (vals: Key) => {
    // the type extacted from the key
    const type = vals.type.replace(/\[|\]|!/g, "");
    // dot-delim path of current entity
    const path = entity ? `${entity}.${vals.name}` : vals.name;

    // extract all matching entires from location in results
    unwindResult(schema, type, result, path, entities, entityIndex);
  };
}

// WalkTree the GraphQL query selection nodes and build a tree structure
export function walkTree(
  nodes: ReadonlyArray<SelectionNode>,
  root: MapResult | MapResultKey,
  opts: WalkTreeOptions,
  skip: SkipValue
): MapResult | MapResultKey {
  const res: MapResult = root as MapResult;
  for (const node of nodes) {
    // skip invalid vars
    if (opts.withVars && !verifyDirectives(node.directives, opts.vars)) {
      // eslint-disable-next-line no-continue
      continue;
    }

    // skip valid fragments and start a new walk from that node
    if (verifyFragment(node, root, opts, skip)) {
      // eslint-disable-next-line no-continue
      continue;
    }

    // extract the fields name
    const name = (node as FieldNode).name.value;

    // extract opt based fragments into new walks in the root
    if (opts.fragments[name]) {
      walkTree(getNodes(opts.fragments[name]), root, opts, skip);

      // eslint-disable-next-line no-continue
      continue;
    }

    // check for descendant nodes...
    const traversalNodes = getNodes(node);
    const traversalNodesSkip = verifySkip(name, skip);

    // if we're not skipping, then continue walking the tree with the traversalNodes
    if (traversalNodesSkip !== true) {
      res[name] = res[name] || (traversalNodes.length ? {} : false);

      // if node has nodes...
      if (res && res[name] && typeof res[name] === "object") {
        // assign args to the structure
        (res[name] as Record<string, any>).args = getArgs(node, opts.vars);
        (res[name] as Record<string, any>).fields = {};

        walkTree(
          traversalNodes,
          (res as Record<string, any>)[name].fields,
          opts,
          traversalNodesSkip
        );
      }
    }
  }

  // return the response from the outer walk
  return res;
}

// Generate every combination that we can combine the current set of entities in to (when adding asc and desc to them)
function generateCombinations(
  keys: { name: string; optional?: boolean; derivedFrom?: string }[]
): Record<string, number>[] {
  // use a set so that we only record each group once
  const combinations: Set<Record<string, number>> = new Set([]);

  for (let i = 0; i < keys.length; i += 1) {
    // setup new keys when we're not working with deriveds
    if (!keys[i].derivedFrom) {
      const key: string = keys[i].name;
      const newCombinations: Record<string, number>[] = [];

      if (i === 0) {
        // if it's the first key, add a new combination with the key appended and value -1 (this is the block entry for usecase)
        newCombinations.push({ [key]: -1 });
      } else {
        // for keys after the first one, add new combinations with the key appended and value 1, -1, and without the key
        for (const combination of Array.from(combinations)) {
          newCombinations.push({ ...combination, [key]: 1 });
          newCombinations.push({ ...combination, [key]: -1 });
          // we can have a !missing check here - if an option isnt optional then don't push the old combination
          if (keys[i].optional !== false) {
            // insert the old combination (with the new entry missing)
            newCombinations.push(combination);
          } else {
            // make sure its not part of the backlog
            combinations.delete(combination);
          }
        }
      }

      // add the set to the combinations
      newCombinations.forEach((set) => {
        combinations.add(set);
      });
    }
  }

  // remove the empty combination
  return Array.from(combinations).slice(1);
}

// Generate the required indexes we need to have defined for this entity in the schema
export function generateIndexes(schema: SimpleSchema, entity: string) {
  // extract fields by type
  const valueFields = getValues(schema, entity, true) || [];
  const entityFields = getValues(schema, entity, false) || [];

  // collect all the indexes together
  const indexes = [
    // we always need block in desc
    { _block_ts: -1 },
    // these are a single level of each property on the entity (we only need to sort in one direction)
    ...[...entityFields, ...valueFields].reduce((all, key) => {
      return key.derivedFrom ? all : [...all, { [key.name]: 1 }];
    }, [] as Record<string, number>[]),
    // these are the entity specific nests of posible filter paths that could be made
    ...generateCombinations([
      // these two additional properties will be indexed as a pair in every query (unless theres a better index)
      {
        name: "_block_ts",
        optional: false,
      },
      {
        name: "id",
        optional: false,
      },
      // get every combination of keys from entityFields added to the !optional pair above
      ...entityFields,
    ]),
  ];

  // console.log(entity, indexes);

  // return the indexes
  return indexes;
}

// Construct the mongo query that will pull all documents to fulfill the graphql query
export function createQuery(
  schema: SimpleSchema,
  entity: string,
  mutable: boolean,
  ast: any,
  context: Record<string, Record<string, unknown>>,
  usePath?: string,
  useMap?: MapResult,
  useMatch?: { $match: { $expr: { $eq: string[] } } },
  useArgStore: Record<string, unknown> = {},
  extraProjection: string | false = false
) {
  // variables need to be injected into queries
  const { variables } = context.params as Record<
    string,
    { variables: Record<string, unknown> }
  >;

  // should be indexed in the schema - we can use this to guide the query joins
  const map =
    useMap ||
    fieldsMap(ast, {
      variables,
    });

  // build an aggregate on the collection
  const aggregates: Record<string, unknown>[] = [];

  // extract top-level args/fields from map
  const args = fieldsArgs(ast, { map, path: usePath, variables });
  const fields = fieldsList(ast, { map, path: usePath, variables });

  // extract fields by type
  const valueFields = getValues(schema, entity, true) || [];
  const entityFields = getValues(schema, entity, false) || [];

  // extract any additional fields defined in the args.where that might not be defined in the response query
  const argFields = collateWheres(
    schema,
    entity,
    ast,
    usePath,
    map,
    useArgStore
  );

  // collate the aggregates starting from the root entity
  aggregates.push(
    ...([
      // perform the match before the sort and groupBy to limit matches
      useMatch?.$match
        ? {
            $match: useMatch?.$match,
          }
        : false,
      // if the ids are updated then we need to groupBy on the id sorted by block/time
      ...(!mutable
        ? [
            {
              $sort: {
                // use block_ts as primary sort before grouping and limiting to first result of each id
                _block_ts: -1,
              },
            },
            {
              $group: {
                _id: "$id",
                // include all values here to inform the joins later
                [`latestDocument${toCamelCase(
                  (usePath || "").replace(/\./g, "-")
                )}`]: {
                  $first: "$$ROOT",
                },
              },
            },
            {
              // we're projecting everything being requested at this level on this entity
              $project: {
                _id: 1,
                // if we're performing a lookup then we need to add the projected "derivedFrom" for the graphql internals to unwind against
                ...(extraProjection
                  ? {
                      [extraProjection]: `$latestDocument${toCamelCase(
                        (usePath || "").replace(/\./g, "-")
                      )}.${extraProjection}`,
                    }
                  : {}),
                // we combine any fields mentioned for this entity in this or any parent arg.where aswell as everything we know we want for this req
                ...[
                  ...valueFields,
                  ...Array.from(new Set([...argFields, ...entityFields])),
                ]
                  .filter(
                    (v) =>
                      v &&
                      (fields.indexOf(v.name) !== -1 ||
                        v.type === "ID" ||
                        argFields.indexOf(v) !== -1)
                  )
                  .reduce((carr, vals) => {
                    return {
                      ...carr,
                      [vals.name]: `$latestDocument${toCamelCase(
                        (usePath || "").replace(/\./g, "-")
                      )}.${vals.name}`,
                    };
                  }, {} as Record<string, any>),
              },
            },
            {
              // sort after grouping (this is likely more expensive because we're sorting on a projection - hopefully the filter has limited the items enough)
              $sort: {
                // use given sort and direction
                [args?.orderBy || "id"]:
                  args.orderDirection === "desc" ? -1 : 1,
              },
            },
          ]
        : [
            {
              // sort the items before performing lookups/matches to make sure we're using the index
              $sort: {
                // use given sort and direction or default to id (to keep it consistent)
                [args?.orderBy || "id"]:
                  args.orderDirection === "desc" ? -1 : 1,
              },
            },
          ]),
      // for each entity on the query we need to create a join...
      ...Array.from(new Set([...entityFields, ...argFields]))
        // exclude any fields not mentioned in the query
        .filter(
          (v) =>
            v && (fields.indexOf(v.name) !== -1 || argFields.indexOf(v) !== -1)
        )
        // map the lookup keeping track of the dotDelim path of the parent entity
        .map(
          // map the keys through the response of createLookup()(key)
          createLookup(
            schema,
            usePath || "",
            mutable,
            ast,
            context,
            map,
            // any arguments defined in this object relate to args on this entity
            (useArgStore[toCamelCase(entity)] || {}) as Record<string, unknown>
          )
        ),
      // args (because the args can filter on descendents we need to do this after the join which sucks.)
      args?.where
        ? {
            $match: {
              // place the match for descending checks
              ...createArgs(args).$match,
            },
          }
        : false,
      // set the pagination offsets and limits
      {
        $skip: parseInt(args?.skip || "0", 10),
      },
      {
        $limit:
          parseInt(args?.skip || "0", 10) + parseInt(args?.first || "25", 10),
      },
      // project all fields that are being requested in the output
      ...(!mutable
        ? [
            {
              $project: {
                _id: "$id",
                id: "$id",
                ...(extraProjection
                  ? { [extraProjection]: `$${extraProjection}` }
                  : {}),
                ...valueFields
                  .filter(
                    (v) => v && fields.indexOf(v.name) !== -1 && v.type !== "ID"
                  )
                  .reduce((carr, vals) => {
                    return {
                      ...carr,
                      [vals.name]: 1,
                    };
                  }, {} as Record<string, number>),
                ...Array.from(new Set([...entityFields, ...argFields]))
                  .filter(
                    (v) =>
                      v &&
                      (fields.indexOf(v.name) !== -1 ||
                        argFields.indexOf(v) !== -1)
                  )
                  .reduce((carr, vals) => {
                    return {
                      ...carr,
                      // use the path to place the relation
                      [vals.name]: `$${(
                        (usePath ? `${usePath}.` : "") + vals.name
                      ).replace(/\./g, "-")}`,
                    };
                  }, {} as Record<string, string>),
              },
            },
          ]
        : []),
    ].filter((agg) => agg) as Record<string, unknown>[])
  );

  // console.log("aggrs", JSON.stringify(aggregates, null, 2));

  // these aggregates need to be ran through db.collection(<entity>).aggregate(aggregates, { collation: { locale: "en_US", numericOrdering: true } });
  return aggregates;
}

// Unwind the result into entity buckets to feed through graphql context
export function unwindResult(
  schema: SimpleSchema,
  entity: string,
  result: any[],
  path?: string,
  useEntites?: Record<string, any>,
  useEntityIndex?: Record<string, any>
) {
  // attempt to use supplied objs first to contain unwinds on nested result
  const entities: Record<string, any> = useEntites || {};
  const entityIndex: Record<string, any> = useEntityIndex || {};

  // extract fields by type
  const entityFields = getValues(schema, entity, false) || [];

  // select the results to iterate on
  const iterateOn = path
    ? // extract all documents from all branches on the result for this path
      getByDotNotation(result, path.split(".")) || []
    : // if theres no path than we'll start from the full result
      result;

  // run through the root and collate entities
  iterateOn?.forEach((rootEntity) => {
    const pushEntity = {
      ...Object.keys(rootEntity || {}).reduce((carr, key) => {
        const entityField = entityFields.find((field) => field.name === key);

        return {
          ...carr,
          // check for derived fields to switch between selecting single entries or collections
          ...(((entityField?.type.indexOf("[") === -1 && {
            [key]:
              typeof rootEntity[key] === "object"
                ? rootEntity[key][0].id
                : rootEntity[key],
          }) ||
            // if we're working a value we place it at key
            (!entityField
              ? {
                  [key]: rootEntity[key],
                }
              : {})) as {}),
        };
      }, {} as Record<string, any>),
    };

    // default the entity if missing
    entities[entity] = entities[entity] || [];
    entityIndex[entity] = entityIndex[entity] || [];

    // check position in index
    const index = entityIndex[entity].indexOf(pushEntity.id);

    // insert new entities
    if (index === -1) {
      entities[entity].push(pushEntity);
      entityIndex[entity].push(pushEntity.id);
    } else {
      // combine the entries in position
      entities[entity].splice(index, 1, {
        ...entities[entity][index],
        ...pushEntity,
      });
    }
  });

  // unwind the nested elements (recursively)
  if (
    entityFields &&
    entityFields.length &&
    (!path ||
      (iterateOn && iterateOn.length && iterateOn[0]?.[entityFields[0].name]))
  ) {
    entityFields.forEach((key) => {
      // unwind the nested lookups
      unwindLookup(schema, path || "", result, entities, entityIndex)(key);
    });
  }

  // these are now a flatMapped object of { entities->document[] } --> these entities need to be post filtered to satisfy the requested graph query
  return entities;
}
