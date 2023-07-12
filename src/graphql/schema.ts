import {
  DocumentNode,
  FieldDefinitionNode,
  StringValueNode,
  NonNullTypeNode,
  ObjectTypeExtensionNode,
  OperationDefinitionNode,
} from "graphql";

// we read the parsed schemas DocumentNode into a SimpleSchema
import { Key, SimpleSchema } from "./types";
import { FILTER_TYPE_MAP, VALUE_TYPES } from "./constants";

// converts strings to plural form (using the same ruleset as graph-node)
import { toPlural } from "../utils/toPlural";
import { toCamelCase } from "../utils/toCamelCase";

// extract the field we're joining against to mark the derivedFrom
const unnestSingular = (entity: ObjectTypeExtensionNode) => {
  if (
    entity.directives &&
    entity.directives.length &&
    entity.directives[0].name.value === "queryFields"
  ) {
    return (entity.directives?.[0]?.arguments?.[0]?.value as StringValueNode)
      .value;
  }
  return undefined;
};

// extract the field we're joining against to mark the derivedFrom
const unnestPlural = (entity: ObjectTypeExtensionNode) => {
  if (
    entity.directives &&
    entity.directives.length &&
    entity.directives[0].name.value === "queryFields"
  ) {
    return (entity.directives?.[0]?.arguments?.[1]?.value as StringValueNode)
      .value;
  }
  return undefined;
};

// extract the type from the definition
const unnestType = (
  entity: FieldDefinitionNode | NonNullTypeNode,
  depth = 0
): string | undefined => {
  if (entity.type) {
    return unnestType(entity.type as NonNullTypeNode, depth + 1);
  }
  if ((entity as FieldDefinitionNode).name) {
    // if this is being represented as bytes, convert it to a string now
    const { value } = (entity as FieldDefinitionNode).name;

    // if theres a depth, we need to wrap it with [] to convey array type
    return `${depth > 2 ? "[" : ""}${value === "Bytes" ? "String" : value}${
      depth > 2 ? "!]!" : ""
    }`;
  }
  return undefined;
};

// extract the field we're joining against to mark the derivedFrom
const unnestDerivedFrom = (entity: FieldDefinitionNode) => {
  if (
    entity.directives &&
    entity.directives.length &&
    entity.directives[0].name.value === "derivedFrom"
  ) {
    return (entity.directives?.[0]?.arguments?.[0]?.value as StringValueNode)
      .value;
  }
  return undefined;
};

// read out all the properties defined in the schema and create a simple mapping
export const readSchema = (schema_: DocumentNode): SimpleSchema => {
  return schema_.definitions.reduce(
    (schema: { [x: string]: any }, item: any) => {
      // take a copy of the schema to void reassigning
      const copy = { ...schema };
      // estract the definition value
      const def = (item as OperationDefinitionNode)?.name?.value;

      copy[def!] = [];

      // iterate the collection of definitions and construct simple mapping
      copy[def!] = (item as ObjectTypeExtensionNode)?.fields?.map(
        (field: { name: { value: any } }) => ({
          name: field.name.value,
          type: unnestType(field as FieldDefinitionNode | NonNullTypeNode, 0),
          derivedFrom: unnestDerivedFrom(field as FieldDefinitionNode),
        })
      );

      // extract plural/single form definitions
      copy[`${def}-plural-form`] = unnestPlural(
        item as ObjectTypeExtensionNode
      );
      copy[`${def}-single-form`] = unnestSingular(
        item as ObjectTypeExtensionNode
      );

      return copy;
    },
    {}
  );
};

// set the key.name: key.type entry (replacing Entity types with filters)
const setType = (name: string, value: string) => {
  return `\n    ${
    // the value of the entity
    name
  }: ${
    // For entity types - replace the type with the corresponding filter
    VALUE_TYPES.includes(value)
      ? `${value}`
      : `${value.replace(/\[|\]|!/g, "")}_filter`
  }`;
};

// construct a where Input type for each entity...
export const where = (entity: string, keys: Key[]) => {
  // construct a full set of filters for each key in keys
  const entityFilters = keys.reduce(
    (
      // construct a schema as a string
      carr: string,
      // extract the key information...
      { type, name, derivedFrom }
    ) => {
      // need to check for derivedFrom in keys and place additional field (with _ suffix)
      return `${carr}${
        // if this is a derived field - add an extra filter prop with a _ suffix
        derivedFrom ? setType(`${name}_`, type) : ``
      } ${
        // map the key to its type (or filter)
        setType(`${name}`, type)
      } ${
        // map type to a set of filters and construct an entry for each...
        (FILTER_TYPE_MAP[type] || FILTER_TYPE_MAP.default)
          // map the filter against the name and accept Strings or String arrays
          .map(
            (filter: string) =>
              `\n    ${name}${filter}: ${
                // if we're checking any sort of "in" then we do it against an array of options
                filter.indexOf("_in") !== -1 ? "[String]" : "String"
              }`
          )
          .join("")
      }`;
    },
    ``
  );

  // wrap as an input - follow graphprotocol naming convention...
  return `\n  input ${entity}_filter { ${entityFilters} \n  }`;
};

// construct yoga compatible defs by defining scalars, query-types, enums, filters, and entities
export const createDefs = (_schema: SimpleSchema) => {
  // return a definition schema with filters and arguments
  return `
    # Bytes will always be converted to Strings for ease of use...
    scalar Bytes
  
    # BigInts and BigDecimals should convert to Strings (@TODO - test this assumption)
    scalar BigInt
    scalar BigDecimal
    scalar Timestamp
  
    # normal decimal numbers
    scalar Decimal
  
    # Enum to control order direction
    enum OrderDirection {
      asc
      desc
    }
  
    ${
      ``
      // need to define block props here
    }
    # Meta information about the current block
    type Meta {
      info: String
    }
  
    # Define available queries (singular and multi based on the provided entity types)
    type Query {
      _meta: Meta\n${Object.keys(_schema)
        .filter((entity) => {
          return (
            !entity.match(/-plural-form$/) && !entity.match(/-single-form$/)
          );
        })
        .map((entity: string) => {
          // lowerCase the first char on single/plural form
          const singleForm = toCamelCase(
            (_schema[`${entity}-single-form`] as string) || entity
          );
          const pluralForm = toCamelCase(
            (_schema[`${entity}-plural-form`] as string) || toPlural(entity)
          );

          // construct each of the schema definitions
          const singular = `${singleForm}(id: String!): ${entity}${`\n`}`;
          const plural = `${pluralForm}(block: BigInt, first: Int, skip: Int, orderBy: ${entity}_orderBy, orderDirection: OrderDirection, where: ${entity}_filter): [${entity}]${`\n`}`;
          const filter = `${entity}_filter(block: BigInt, first: Int, skip: Int, orderBy: ${entity}_orderBy, orderDirection: OrderDirection, where: ${entity}_filter): [${entity}]`;

          // these are deliberately spaced for formatting (contained within query (4 spaces deep))
          return `${``}    ${singular}    ${plural}    ${filter}`;
        })
        .join("\n")}
    }
    ${
      // Defines each entities filters, schema, and ordering options
      Object.keys(_schema)
        .filter((entity) => {
          return (
            !entity.match(/-plural-form$/) && !entity.match(/-single-form$/)
          );
        })
        .map((entity: string) => {
          // place each child against its value type
          const children = (_schema[entity] as Key[]).reduce((res, child) => {
            const type = child.type.replace(/\[|\]|!/g, "");
            if (_schema[type] && child.type[0] === "[") {
              return `${res}\n    ${child.name}(block: BigInt, first: Int, skip: Int, orderBy: ${type}_orderBy, orderDirection: OrderDirection, where: ${type}_filter): ${child.type}`;
            }
            if (_schema[type]) {
              return `${res}\n    ${child.name}(id: String): ${child.type}`;
            }
            return `${res}\n    ${child.name}: ${child.type}`;
          }, ``);
          // ordering available for each key in the entitys schema
          const orderBys = (_schema[entity] as Key[]).reduce((res, child) => {
            return `${res}\n    ${child.name}`;
          }, ``);

          // construct def for each kind using the entities schema
          const filterDef = `${where(entity, _schema[entity] as Key[])}\n\n`;
          const entityDef = `type ${entity} {${children}\n  }\n\n`;
          const orderByDef = `enum ${entity}_orderBy {${orderBys}\n  }`;

          // return filters, entity and ordering options (deliberately spaced for formatting)...
          return `${filterDef}  ${entityDef}  ${orderByDef}`;
        })
        .join("\n")
    }`;
};
