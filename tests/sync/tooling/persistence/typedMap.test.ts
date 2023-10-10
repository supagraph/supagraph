import { TypedMap } from "@/sync/tooling/persistence/typedMap";

describe("TypedMap", () => {
  it("should set and get values correctly", () => {
    const map = new TypedMap<string, number>();
    map.set("one", 1);
    map.set("two", 2);

    expect(map.get("one")).toBe(1);
    expect(map.get("two")).toBe(2);
  });

  it("should return null for non-existing entries", () => {
    const map = new TypedMap<string, number>();

    expect(map.getEntry("nonExisting")).toBeNull();
  });

  it("should throw an error for non-existing entries with mustGetEntry", () => {
    const map = new TypedMap<string, number>();

    expect(() => map.mustGetEntry("nonExisting")).toThrowError(
      "Entry for key nonExisting does not exist in TypedMap"
    );
  });

  it("should check if an entry is set correctly", () => {
    const map = new TypedMap<string, number>();
    map.set("key", 42);

    expect(map.isSet("key")).toBe(true);
    expect(map.isSet("nonExisting")).toBe(false);
  });

  it("should return the correct value with mustGet", () => {
    const map = new TypedMap<string, number>();
    map.set("key", 42);

    expect(map.mustGet("key")).toBe(42);
    expect(() => map.mustGet("nonExisting")).toThrowError(
      "Value for key nonExisting does not exist in TypedMap"
    );
  });

  it("should return a valid object representation", () => {
    const map = new TypedMap<string, number>();
    map.set("one", 1);
    map.set("two", 2);

    const expected = {
      one: 1,
      two: 2,
    };

    expect(map.valueOf()).toEqual(expected);
  });
});
