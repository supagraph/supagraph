// This is a typescript implementation of the inflector::Inflector pluralize rust crate https://github.com/whatisinternet/Inflector/blob/master/src/string/pluralize/mod.rs
const UNACCONTABLE_WORDS = [
  "accommodation",
  "adulthood",
  "advertising",
  "advice",
  "aggression",
  "aid",
  "air",
  "aircraft",
  "alcohol",
  "anger",
  "applause",
  "arithmetic",
  "assistance",
  "athletics",

  "bacon",
  "baggage",
  "beef",
  "biology",
  "blood",
  "botany",
  "bread",
  "butter",

  "carbon",
  "cardboard",
  "cash",
  "chalk",
  "chaos",
  "chess",
  "crossroads",
  "countryside",

  "dancing",
  "deer",
  "dignity",
  "dirt",
  "dust",

  "economics",
  "education",
  "electricity",
  "engineering",
  "enjoyment",
  "envy",
  "equipment",
  "ethics",
  "evidence",
  "evolution",

  "fame",
  "fiction",
  "flour",
  "flu",
  "food",
  "fuel",
  "fun",
  "furniture",

  "gallows",
  "garbage",
  "garlic",
  "genetics",
  "gold",
  "golf",
  "gossip",
  "grammar",
  "gratitude",
  "grief",
  "guilt",
  "gymnastics",

  "happiness",
  "hardware",
  "harm",
  "hate",
  "hatred",
  "health",
  "heat",
  "help",
  "homework",
  "honesty",
  "honey",
  "hospitality",
  "housework",
  "humour",
  "hunger",
  "hydrogen",

  "ice",
  "importance",
  "inflation",
  "information",
  "innocence",
  "iron",
  "irony",

  "jam",
  "jewelry",
  "judo",

  "karate",
  "knowledge",

  "lack",
  "laughter",
  "lava",
  "leather",
  "leisure",
  "lightning",
  "linguine",
  "linguini",
  "linguistics",
  "literature",
  "litter",
  "livestock",
  "logic",
  "loneliness",
  "luck",
  "luggage",

  "macaroni",
  "machinery",
  "magic",
  "management",
  "mankind",
  "marble",
  "mathematics",
  "mayonnaise",
  "measles",
  "methane",
  "milk",
  "money",
  "mud",
  "music",
  "mumps",

  "nature",
  "news",
  "nitrogen",
  "nonsense",
  "nurture",
  "nutrition",

  "obedience",
  "obesity",
  "oxygen",

  "pasta",
  "patience",
  "physics",
  "poetry",
  "pollution",
  "poverty",
  "pride",
  "psychology",
  "publicity",
  "punctuation",

  "quartz",

  "racism",
  "relaxation",
  "reliability",
  "research",
  "respect",
  "revenge",
  "rice",
  "rubbish",
  "rum",

  "safety",
  "scenery",
  "seafood",
  "seaside",
  "series",
  "shame",
  "sheep",
  "shopping",
  "sleep",
  "smoke",
  "smoking",
  "snow",
  "soap",
  "software",
  "soil",
  "spaghetti",
  "species",
  "steam",
  "stuff",
  "stupidity",
  "sunshine",
  "symmetry",

  "tennis",
  "thirst",
  "thunder",
  "timber",
  "traffic",
  "transportation",
  "trust",

  "underwear",
  "unemployment",
  "unity",

  "validity",
  "veal",
  "vegetation",
  "vegetarianism",
  "vengeance",
  "violence",
  "vitality",

  "warmth",
  "wealth",
  "weather",
  "welfare",
  "wheat",
  "wildlife",
  "wisdom",
  "yoga",

  "zinc",
  "zoology",
];

const rules = [
  { expr: /(\w*)s$/, suffix: "s" },
  { expr: /(\w*([^aeiou]ese))$/, suffix: "" },
  { expr: /(\w*(ax|test))is$/, suffix: "es" },
  { expr: /(\w*(alias|[^aou]us|tlas|gas|ris))$/, suffix: "es" },
  { expr: /(\w*(e[mn]u))s?$/, suffix: "s" },
  { expr: /(\w*([^l]ias|[aeiou]las|[emjzr]as|[iu]am))$/, suffix: "" },
  {
    expr: /(\w*(alumn|syllab|octop|vir|radi|nucle|fung|cact|stimul|termin|bacill|foc|uter|loc|strat))(?:us|i)$/,
    suffix: "i",
  },
  { expr: /(\w*(alumn|alg|vertebr))(?:a|ae)$/, suffix: "ae" },
  { expr: /(\w*(seraph|cherub))(?:im)?$/, suffix: "im" },
  { expr: /(\w*(her|at|gr))o$/, suffix: "oes" },
  {
    expr: /(\w*(agend|addend|millenni|dat|extrem|bacteri|desiderat|strat|candelabr|errat|ov|symposi|curricul|automat|quor))(?:a|um)$/,
    suffix: "a",
  },
  {
    expr: /(\w*(apheli|hyperbat|periheli|asyndet|noumen|phenomen|criteri|organ|prolegomen|hedr|automat))(?:a|on)$/,
    suffix: "a",
  },
  { expr: /(\w*)sis$/, suffix: "ses" },
  { expr: /(\w*(kni|wi|li))fe$/, suffix: "ves" },
  { expr: /(\w*(ar|l|ea|eo|oa|hoo))f$/, suffix: "ves" },
  { expr: /(\w*([^aeiouy]|qu))y$/, suffix: "ies" },
  { expr: /(\w*([^ch][ieo][ln]))ey$/, suffix: "ies" },
  { expr: /(\w*(x|ch|ss|sh|zz)es)$/, suffix: "" },
  { expr: /(\w*(x|ch|ss|sh|zz))$/, suffix: "es" },
  { expr: /(\w*(matr|cod|mur|sil|vert|ind|append))(?:ix|ex)$/, suffix: "ices" },
  { expr: /(\w*(m|l)(?:ice|ouse))$/, suffix: "ice" },
  { expr: /(\w*(pe)(?:rson|ople))$/, suffix: "ople" },
  { expr: /(\w*(child))(?:ren)?$/, suffix: "ren" },
  { expr: /(\w*eaux)$/, suffix: "" },
];

const specials = {
  ox: "oxen",
  man: "men",
  woman: "women",
  die: "dice",
  yes: "yeses",
  foot: "feet",
  eave: "eaves",
  goose: "geese",
  tooth: "teeth",
  quiz: "quizzes",
};

export const toPlural = (non_plural_string: string): string => {
  if (UNACCONTABLE_WORDS.includes(non_plural_string)) {
    return non_plural_string;
  }
  if (specials[non_plural_string as keyof typeof specials]) {
    return specials[non_plural_string as keyof typeof specials];
  }

  // run through the rules and carry out the match
  // eslint-disable-next-line no-restricted-syntax
  for (const rule of rules.reverse()) {
    const match = non_plural_string.match(rule.expr);
    // check the match on [1] then [0] so that we can step to the right criteria (this avoids bug in `mouse` regexp)
    if (match && match[1] === non_plural_string) {
      return `${match[2]}${rule.suffix}`;
    }
    if (match && match[0] === non_plural_string) {
      return `${match[1]}${rule.suffix}`;
    }
  }

  // fallback to adding a single s
  return `${non_plural_string}s`;
};

export default toPlural;
