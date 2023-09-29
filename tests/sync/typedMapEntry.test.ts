import { TypedMapEntry } from "../../src/sync/typedMapEntry";

describe("TypedMapEntry", () => {
  it("should create an instance with correct key and value", () => {
    const entry = new TypedMapEntry<string, number>("one", 1);

    expect(entry.key).toBe("one");
    expect(entry.value).toBe(1);
  });
});
