import { MongoClient, Collection, Document } from "mongodb";

import { Mongo } from "@/sync/tooling/persistence/mongo";
import { NotFound } from "@/sync/tooling/persistence/db";

// Mock the mongodb methods and classes
jest.mock("mongodb");

describe("Mongo", () => {
  let mockClient: MongoClient;
  let mockDb: ReturnType<MongoClient["db"]>;
  let mockCollection: Collection<Document>;

  beforeEach(() => {
    // Create mock instances for each test
    mockCollection = {
      findOne: jest.fn(() => ({ id: "id2" })),
      updateOne: jest.fn(),
      replaceOne: jest.fn(),
      bulkWrite: jest.fn(),
      deleteOne: jest.fn(),
    } as unknown as Collection<Document>;

    mockDb = {
      collection: jest.fn(() => mockCollection),
    } as unknown as ReturnType<MongoClient["db"]>;

    mockClient = {
      db: jest.fn(() => mockDb) as MongoClient["db"],
    } as unknown as MongoClient;
  });

  it("should create an instance of Mongo", async () => {
    const db = new Mongo(mockClient, "testDb", {});
    expect(db).toBeInstanceOf(Mongo);
  });

  it("should initialize with the given kv data", async () => {
    const kvData = {
      exampleRef: {
        id1: { data: "value1" },
        id2: { data: "value2" },
      },
    };
    const db = new Mongo(mockClient, "testDb", kvData);

    expect(db.kv).toEqual(kvData);
  });

  it("should update kv data", async () => {
    const kvData = {
      exampleRef: {
        id1: { data: "value1" },
        id2: { data: "value2" },
      },
    };
    const db = new Mongo(mockClient, "testDb", {});

    await db.update({ kv: kvData });

    expect(db.kv).toEqual(kvData);
  });

  it("should get a value using the get method", async () => {
    // connect to the mockClient
    const db = new Mongo(mockClient, "testDb", {
      "exampleRef": {
        "id1": { id: "id1" }
      }
    });

    // get an entry from cache
    const value1 = await db.get("exampleRef.id1");

    // expect to have attempt a query
    expect(value1).toStrictEqual({ id: "id1" });

    // get an entry from db
    await db.get("exampleRef.id2");

    // expect to have attempt a query
    expect(mockCollection.findOne).toHaveBeenCalledWith(
      { id: "id2" },
      { sort: { _block_ts: -1 } }
    );
  });

  it("should throw an error if value doesnt exist", async () => {
    // Create mock instance that returns null to signify the value is missing
    mockCollection = {
      findOne: jest.fn(() => null),
    } as unknown as Collection<Document>;

    // connect to the mockClient
    const db = new Mongo(mockClient, "testDb", {});

    // expect error to be thrown
    expect(async () => await db.get("exampleRef.id1")).rejects.toThrowError(NotFound);
  });

  it("should put a value using the put method", async () => {
    // connect to the mockClient
    const db = new Mongo(mockClient, "testDb", {});
    // put an entry
    await db.put("exampleRef.id1", {
      id: "id1",
      data: "new-value",
    });

    // expect to have attempt a query
    expect(mockCollection.replaceOne).toHaveBeenCalledWith(
      {
        id: "id1",
        _block_ts: undefined,
        _block_num: undefined,
        _chain_id: undefined,
      },
      {
        id: "id1",
        data: "new-value",
        _block_ts: undefined,
        _block_num: undefined,
        _chain_id: undefined,
      },
      {
        upsert: true,
      }
    );
  });

  it("should prevent put using the put method if engine is in readOnly mode", async () => {
    // connect to the mockClient
    const db = new Mongo(mockClient, "testDb", {}, false, { readOnly: true });
    // put an entry
    await db.put("exampleRef.id1", {
      id: "id1",
      data: "new-value",
    });
    // expect to have attempt a query
    expect(mockCollection.replaceOne).not.toHaveBeenCalled();
  });

  it("should perform batch operations", async () => {
    const db = new Mongo(mockClient, "testDb", {});

    const batchData: {
      type: "put" | "del";
      key: string;
      value?: Record<string, unknown>;
    }[] = [
      {
        type: "put",
        key: "exampleRef.id1",
        value: { id: "id1", data: "value1" },
      },
      {
        type: "put",
        key: "exampleRef.id2",
        value: { id: "id2", data: "value2" },
      },
      { type: "del", key: "exampleRef.id3" },
    ];
    await db.batch(batchData);

    expect(mockCollection.bulkWrite).toHaveBeenCalledWith(
      [
        {
          replaceOne: {
            filter: {
              id: "id1",
              _block_ts: undefined,
              _block_num: undefined,
              _chain_id: undefined,
            },
            replacement: {
              id: "id1",
              data: "value1",
              _block_ts: undefined,
              _block_num: undefined,
              _chain_id: undefined,
            },
            upsert: true,
          },
        },
        {
          replaceOne: {
            filter: {
              id: "id2",
              _block_ts: undefined,
              _block_num: undefined,
              _chain_id: undefined,
            },
            replacement: {
              id: "id2",
              data: "value2",
              _block_ts: undefined,
              _block_num: undefined,
              _chain_id: undefined,
            },
            upsert: true,
          },
        },
        {
          deleteMany: {
            filter: { id: "id3" },
          },
        },
      ],
      {
        ordered: false,
        forceServerObjectId: true,
      }
    );
  });
  
  it("should prevent batch operations if engine is in readOnly mode", async () => {
    const db = new Mongo(mockClient, "testDb", {}, false, { readOnly: true });
  
    const batchData: {
      type: "put" | "del";
      key: string;
      value?: Record<string, unknown>;
    }[] = [
      {
        type: "put",
        key: "exampleRef.id1",
        value: { id: "id1", data: "value1" },
      },
      {
        type: "put",
        key: "exampleRef.id2",
        value: { id: "id2", data: "value2" },
      },
      { type: "del", key: "exampleRef.id3" },
    ];
    await db.batch(batchData);
  
    expect(mockCollection.bulkWrite).not.toHaveBeenCalled();
  });
});
