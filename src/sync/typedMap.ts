import { TypedMapEntry } from "./typepMapEntry";

const assert = (condition: boolean, error: string) => {
  if (!condition) {
    throw new Error(error);
  }
};

// Typed map
export class TypedMap<K extends string | number | symbol, V> {
  entries: Array<TypedMapEntry<K, V>>;

  constructor() {
    this.entries = new Array<TypedMapEntry<K, V>>(0);
  }

  set(key: K, value: V): void {
    let entry = this.getEntry(key);
    if (entry !== null) {
      entry.value = value;
    } else {
      entry = new TypedMapEntry<K, V>(key, value);
      this.entries.push(entry);
    }
  }

  getEntry(key: K): TypedMapEntry<K, V> | null {
    for (let i: number = 0; i < this.entries.length; i += 1) {
      if (this.entries[i].key === key) {
        return this.entries[i];
      }
    }
    return null;
  }

  mustGetEntry(key: K): TypedMapEntry<K, V> {
    const entry = this.getEntry(key);
    assert(
      entry != null,
      `Entry for key ${key as string} does not exist in TypedMap`
    );
    return entry!;
  }

  get(key: K): V {
    for (let i: number = 0; i < this.entries.length; i += 1) {
      if (this.entries[i].key === key) {
        return this.entries[i].value;
      }
    }
    return `` as V;
  }

  mustGet(key: K): V {
    const value = this.get(key);
    assert(
      value != null && value !== ``,
      `Value for key ${key as string} does not exist in TypedMap`
    );
    return value as V;
  }

  isSet(key: K): boolean {
    for (let i: number = 0; i < this.entries.length; i += 1) {
      if (this.entries[i].key === key) {
        return true;
      }
    }
    return false;
  }

  valueOf() {
    // unpack the typedMap into a key-value object
    const res: Record<K, V> = {} as Record<K, V>;
    for (let i: number = 0; i < this.entries.length; i += 1) {
      res[this.entries[i].key] = this.entries[i].value;
    }
    return res;
  }
}
