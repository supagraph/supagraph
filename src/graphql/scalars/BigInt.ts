import {
  Kind,
  GraphQLScalarType,
  GraphQLScalarTypeConfig,
  GraphQLErrorExtensions,
  print,
  ASTNode,
  GraphQLError,
  Source,
  versionInfo,
} from "graphql";

interface GraphQLErrorOptions {
  nodes?: ReadonlyArray<ASTNode> | ASTNode | null;
  source?: Source;
  positions?: ReadonlyArray<number>;
  path?: ReadonlyArray<string | number>;
  originalError?: Error & {
    readonly extensions?: unknown;
  };
  extensions?: GraphQLErrorExtensions;
}

export function createGraphQLError(
  message: string,
  options?: GraphQLErrorOptions
): GraphQLError {
  if (versionInfo.major >= 17) {
    return new GraphQLError(message, options);
  }
  return new GraphQLError(message, {
    nodes: options?.nodes,
    source: options?.source,
    positions: options?.positions,
    path: options?.path,
    originalError: options?.originalError,
    extensions: options?.extensions || {},
  });
}

export function isObjectLike(
  value: unknown
): value is { [key: string]: unknown } {
  return typeof value === "object" && value !== null;
}

// Taken from https://github.com/graphql/graphql-js/blob/30b446938a9b5afeb25c642d8af1ea33f6c849f3/src/type/scalars.ts#L267

// Support serializing objects with custom valueOf() or toJSON() functions -
// a common way to represent a complex value which can be represented as
// a string (ex: MongoDB id objects).
export function serializeObject(outputValue: unknown): unknown {
  if (isObjectLike(outputValue)) {
    if (typeof outputValue.valueOf === "function") {
      const valueOfResult = outputValue.valueOf();
      if (!isObjectLike(valueOfResult)) {
        return valueOfResult;
      }
    }
    if (typeof outputValue.toJSON === "function") {
      return outputValue.toJSON();
    }
  }
  return outputValue;
}

export const GraphQLBigIntConfig: GraphQLScalarTypeConfig<
  bigint,
  bigint | typeof BigInt | string | number
> = /* #__PURE__ */ {
  name: "BigInt",
  description:
    "The `BigInt` scalar type represents non-fractional signed whole numeric values.",
  serialize(outputValue: unknown) {
    const coercedValue = serializeObject(outputValue);

    let num: BigInt = coercedValue as unknown as BigInt;

    if (
      typeof coercedValue === "object" &&
      coercedValue != null &&
      "toString" in coercedValue
    ) {
      num = BigInt(coercedValue.toString());
      if (num.toString() !== coercedValue.toString()) {
        throw createGraphQLError(
          `BigInt cannot represent non-integer value: ${coercedValue}`
        );
      }
    }

    if (typeof coercedValue === "boolean") {
      num = BigInt(coercedValue);
    }

    if (typeof coercedValue === "string" && coercedValue !== "") {
      num = BigInt(coercedValue);
      if (num.toString() !== coercedValue) {
        throw createGraphQLError(
          `BigInt cannot represent non-integer value: ${coercedValue}`
        );
      }
    }

    if (typeof coercedValue === "number") {
      if (!Number.isInteger(coercedValue)) {
        throw createGraphQLError(
          `BigInt cannot represent non-integer value: ${coercedValue}`
        );
      }
      num = BigInt(coercedValue);
    }

    if (typeof num !== "bigint") {
      throw createGraphQLError(
        `BigInt cannot represent non-integer value: ${coercedValue}`
      );
    }

    if ("toJSON" in BigInt.prototype) {
      return num;
    }

    return new Proxy({} as typeof BigInt, {
      has(_, prop) {
        if (prop === "toJSON") {
          return true;
        }
        return prop in BigInt.prototype;
      },
      get(_, prop) {
        if (prop === "toJSON") {
          return function toJSON() {
            return num.toString();
          };
        }
        if (prop === Symbol.toStringTag) {
          return num.toString();
        }
        if (prop in BigInt.prototype) {
          // @ts-ignore
          return BigInt.prototype[prop].bind(num);
        }
        return undefined;
      },
    });
  },
  parseValue(inputValue) {
    const num = BigInt(inputValue!.toString());
    if (inputValue!.toString() !== num.toString()) {
      throw createGraphQLError(`BigInt cannot represent value: ${inputValue}`);
    }
    return num;
  },
  parseLiteral(valueNode) {
    if (valueNode.kind !== Kind.INT) {
      throw createGraphQLError(
        `BigInt cannot represent non-integer value: ${print(valueNode)}`,
        { nodes: valueNode }
      );
    }
    const num = BigInt(valueNode.value!);
    if (num.toString() !== valueNode.value) {
      throw createGraphQLError(
        `BigInt cannot represent value: ${valueNode.value}`,
        { nodes: valueNode }
      );
    }
    return num;
  },
  extensions: {
    codegenScalarType: "bigint",
    jsonSchema: {
      type: "integer",
      format: "int64",
    },
  },
};

export const GraphQLBigInt: GraphQLScalarType =
  /* #__PURE__ */ new GraphQLScalarType(GraphQLBigIntConfig);
