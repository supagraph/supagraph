// store.test.ts
import { DB } from "@/sync/tooling/persistence/db";
import { Stage } from "@/sync/tooling/persistence/stage";
import { Store, Entity, getEngine } from "@/sync/tooling/persistence/store";

describe("Store", () => {
  let db: DB;

  beforeEach(async () => {
    // Create a stub for the DB class
    db = new DB({});

    // fetch the engine so that we can replace it each run
    const engine = await getEngine();

    // set the engine
    engine.db = db;
    // wrap the db in a checkpoint staging db
    engine.stage = new Stage(engine.db);
    // place the engine against the db for access by ref
    engine.db.engine = engine as { newDb: boolean };
  });

  it("should save entity", async () => {
    // set spy on put
    db.put = jest.fn();

    // Call save on a new entity
    const entity = new Entity("test", "id", []);
    await entity.save();

    // Assert DB.set was called with the entity
    expect(db.put).toBeCalledWith("test.id", { id: "id" });
  });

  it("should save entity with values", async () => {
    // set spy on put
    db.put = jest.fn();

    // Call save on a new entity
    const entity = await Store.get<{ id: string; val: string }>("test", "id");
    entity.set("val", "test");
    await entity.save();

    // Assert DB.set was called with the entity
    expect(db.put).toBeCalledWith("test.id", { id: "id", val: "test" });
  });
});

it("should copy entity with values", async () => {
  // Call save on a new entity and compare returned instances
  const entity = await Store.get<{ id: string; val: string }>("test", "id");
  entity.set("val", "test");

  // note that this returns a new copy of the entity
  const entity2 = await entity.save();

  // as does calling copy
  const entity3 = entity.copy();

  // Assert values match match expectations
  expect(entity.val).not.toEqual(entity2.val);
  expect(entity2).toEqual(entity3);
  expect(entity2).not.toBe(entity3);
  expect(entity2.val).toEqual(entity3.val);
});
