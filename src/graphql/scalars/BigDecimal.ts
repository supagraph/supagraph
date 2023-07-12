/* eslint-disable no-nested-ternary, no-underscore-dangle */
import { GraphQLScalarType, GraphQLScalarTypeConfig } from "graphql/type";

// using built in BigInt to construct BigDecimal
export class BigDecimal {
  // Configuration: constants

  // number of decimals on all instances
  static DECIMALS = 18;

  // numbers are truncated (false) or rounded (true)
  static ROUNDED = true;

  // derived constant
  static SHIFT = BigInt(`1${"0".repeat(BigDecimal.DECIMALS)}`);

  // the internal bigInt slot
  _n!: bigint;

  constructor(value: unknown) {
    // eslint-disable-next-line no-constructor-return
    if (value instanceof BigDecimal) return value;
    const bigVal = value as { _isBigNumber: boolean; _hex: string };
    const isBigNumber =
      bigVal?._isBigNumber ||
      ((value as string).indexOf && (value as string).indexOf("0x") === 0);

    // extract left and right side of decimal point
    let [ints, decis] = String(
      // eslint-disable-next-line no-nested-ternary
      bigVal?._isBigNumber
        ? BigInt(bigVal._hex)
        : isBigNumber
        ? BigInt(value as string)
        : value || 0
    )
      .toString()
      .split(".")
      // default decis to ''
      .concat("");

    // if we started with a bigNumber - correct the decis
    if (isBigNumber) {
      decis = ints.slice(ints.length - BigDecimal.DECIMALS, ints.length);
      ints = ints.slice(0, ints.length - BigDecimal.DECIMALS);
    }

    // construct two bigInts to hold the value
    this._n =
      BigInt(
        ints +
          decis.padEnd(BigDecimal.DECIMALS, "0").slice(0, BigDecimal.DECIMALS)
      ) + BigInt(BigDecimal.ROUNDED && decis[BigDecimal.DECIMALS] >= "5");
  }

  static fromString(s: string) {
    return new BigDecimal(s);
  }

  static fromBigInt(bigint: bigint) {
    return new BigDecimal(bigint);
  }

  static plus(num1: number, num2: number) {
    return new BigDecimal(num1)._n + new BigDecimal(num2)._n;
  }

  static minus(num1: number, num2: number) {
    return new BigDecimal(num1)._n - new BigDecimal(num2)._n;
  }

  static _divRound(dividend: bigint, divisor: bigint) {
    return BigDecimal.fromBigInt(
      dividend / divisor +
        (BigDecimal.ROUNDED
          ? ((dividend * BigInt(2)) / divisor) % BigInt(2)
          : BigInt(0))
    );
  }

  static multiply(num1: number, num2: number) {
    return new BigDecimal(num1)._n * new BigDecimal(num2)._n;
  }

  static divide(num1: number, num2: number) {
    return BigDecimal._divRound(
      new BigDecimal(num1)._n,
      new BigDecimal(num2)._n
    );
  }

  static gt(num1: number | string, num2: number | string) {
    return new BigDecimal(num1)._n > new BigDecimal(num2)._n;
  }

  static gte(num1: number | string, num2: number | string) {
    return new BigDecimal(num1)._n >= new BigDecimal(num2)._n;
  }

  static lt(num1: number | string, num2: number | string) {
    return new BigDecimal(num1)._n < new BigDecimal(num2)._n;
  }

  static lte(num1: number | string, num2: number | string) {
    return new BigDecimal(num1)._n <= new BigDecimal(num2)._n;
  }

  toString() {
    const s = this._n.toString().padStart(BigDecimal.DECIMALS + 1, "0");
    const ints = s.slice(0, -BigDecimal.DECIMALS);
    const deci = s.slice(-BigDecimal.DECIMALS).replace(/\.?0+$/, "");
    return `${ints}.${deci || "0"}`;
  }
}

export const GraphQLBigDecimalConfig = {
  name: "BigDecimal",
  description:
    "The `BigDecimal` scalar type represents non-fractional signed whole numeric values.",
  serialize(outputValue: { _isBigNumber: any; toString: () => string }) {
    const coercedValue =
      String(outputValue).indexOf(".") !== -1
        ? outputValue
        : outputValue._isBigNumber
        ? `${outputValue.toString().slice(0, -BigDecimal.DECIMALS) || "0"}.${
            outputValue
              .toString()
              .slice(BigDecimal.DECIMALS)
              .replace(/0+$/, "") || "0"
          }`
        : `${outputValue}.0`;
    let num: BigDecimal = coercedValue as unknown as BigDecimal;
    if (
      typeof coercedValue === "object" &&
      coercedValue != null &&
      "toString" in coercedValue
    ) {
      num = new BigDecimal(coercedValue.toString());
      if (num.toString() !== coercedValue.toString()) {
        throw Error(
          `BigInt cannot represent non-integer value: ${coercedValue}`
        );
      }
    }
    if (typeof coercedValue === "boolean") {
      num = new BigDecimal(coercedValue ? "0" : "1");
    }
    if (typeof coercedValue === "string" && coercedValue !== "") {
      num = new BigDecimal(coercedValue);
      if (num.toString() !== coercedValue) {
        throw Error(
          `BigInt cannot represent non-integer value: ${coercedValue}`
        );
      }
    }
    if (typeof coercedValue === "number") {
      num = new BigDecimal(coercedValue);
    }
    if ("toJSON" in BigDecimal.prototype) {
      return num;
    }
    return new Proxy(
      {},
      {
        has(_, prop) {
          if (prop === "toJSON") {
            return true;
          }
          return prop in BigDecimal.prototype;
        },
        get(_, prop) {
          if (prop === "toJSON") {
            return function toJSON() {
              return num.toString().replace(/\.0$/, "");
            };
          }
          if (prop === Symbol.toStringTag) {
            return num.toString().replace(/\.0$/, "");
          }
          if (prop in BigDecimal.prototype) {
            // @ts-ignore
            return BigDecimal.prototype[prop].bind(num);
          }
          return undefined;
        },
      }
    );
  },
  parseValue(inputValue: { toString: () => unknown }) {
    const num = new BigDecimal(inputValue.toString());
    if (inputValue.toString() !== num.toString()) {
      throw Error(`BigDecimal cannot represent value: ${inputValue}`);
    }
    return num;
  },
  parseLiteral(valueNode: { value: unknown }) {
    const num = new BigDecimal(valueNode.value);
    if (num.toString() !== valueNode.value) {
      throw Error(
        `BigDecimal cannot represent value: ${valueNode.value} ${{
          nodes: valueNode,
        }}`
      );
    }
    return num;
  },
  extensions: {
    codegenScalarType: "bigdecimal",
    jsonSchema: {
      type: "integer",
      format: "int64",
    },
  },
};

export const GraphQLBigDecimal = new GraphQLScalarType(
  GraphQLBigDecimalConfig as Readonly<GraphQLScalarTypeConfig<BigDecimal, {}>>
);
