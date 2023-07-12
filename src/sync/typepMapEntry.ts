// TypedMap entry
export class TypedMapEntry<K, V> {
  key: K;

  value: V;

  constructor(key: K, value: V) {
    this.key = key;
    this.value = value;
  }
}
