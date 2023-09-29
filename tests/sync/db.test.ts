import { DB } from "../../src/sync/db";

// Mock dotenv.config() since it's not related to testing DB class
jest.mock("dotenv", () => ({
  config: jest.fn(),
}));

describe("DB", () => {
  beforeEach(() => {
    // Clear all mock calls and instances before each test
    jest.clearAllMocks();
  });

  it("should create an instance of DB", async () => {
    const db = new DB({});
    expect(db).toBeInstanceOf(DB);
  });

  it("should initialize with the given kv data", async () => {
    const kvData = {
      exampleRef: {
        id1: { data: "value1" },
        id2: { data: "value2" },
      },
    };
    const db = new DB(kvData);

    expect(db.kv).toEqual(kvData);
  });

  it("should get a value using the get method", async () => {
    const db = new DB({});
    db.kv = {
      exampleRef: {
        id1: { data: "value1" },
      },
    };

    const value = await db.get("exampleRef.id1");
    expect(value).toEqual({ data: "value1" });
  });

  it("should put a value using the put method", async () => {
    const db = new DB({});
    await db.put("exampleRef.id1", { data: "new-value" });

    expect(db.kv).toEqual({
      exampleRef: {
        id1: { data: "new-value" },
      },
    });
  });

  it("should delete a value using the del method", async () => {
    const db = new DB({});
    db.kv = {
      exampleRef: {
        id1: { data: "value1" },
      },
    };
    await db.del("exampleRef.id1");

    expect(db.kv).toEqual({
      exampleRef: {},
    });
  });

  it("should perform batch operations", async () => {
    const db = new DB({});
    const batchData: {
      type: "put" | "del";
      key: string;
      value?: Record<string, unknown>;
    }[] = [
      { type: "put", key: "exampleRef.id1", value: { data: "value1" } },
      { type: "put", key: "exampleRef.id2", value: { data: "value2" } },
      { type: "del", key: "exampleRef.id3" },
    ];
    await db.batch(batchData);

    expect(db.kv).toEqual({
      exampleRef: {
        id1: { data: "value1" },
        id2: { data: "value2" },
      },
    });
  });
});
