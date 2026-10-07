/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.common.utils.helix;

import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import javax.annotation.Nullable;


/// An immutable [SortedMap] with [String] keys in natural order, backed by a sorted key array and a parallel value
/// array.
///
/// [CompactZNRecordSerializer] uses it for the map fields of ideal states and external views: one instance per
/// distinct instance-state map, and one for the outer segment map. Lookups use binary search with an identity fast
/// path, so interned keys compare cheaply.
///
/// The map follows the [Map] and [SortedMap] contracts: [#equals], [#hashCode] and [#toString] match those of a
/// [java.util.TreeMap] with the same content, and [#keySet], [#entrySet] and [#values] are [AbstractSet] and
/// [AbstractCollection] views with the standard `equals` and `hashCode`. [#comparator] returns `null`, so
/// `new TreeMap<>(map)` and `TreeMap.putAll` copy it in linear time. Keys are never `null`; values can be `null`.
/// Every mutator throws [UnsupportedOperationException].
///
/// Thread-safety: the map is immutable and safe to share between threads once published. The cached hash codes and
/// views are computed lazily with benign races: every thread computes the same value.
public final class ImmutableSortedArrayMap<V> extends AbstractMap<String, V> implements SortedMap<String, V> {
  private static final ImmutableSortedArrayMap<?> EMPTY = new ImmutableSortedArrayMap<>(new String[0], new Object[0]);

  private final String[] _keys;
  private final Object[] _values;

  // Lazily computed, benign races. 0 means "not computed yet" (a real hash code of 0 is recomputed each time).
  private int _hashCode;
  private int _keySetHashCode;
  private KeySet _keySet;
  private EntrySet _entrySet;
  private Values _valuesView;

  /// Wraps the given arrays without copying them. The keys must be non-null, distinct and sorted in natural order,
  /// and the caller must not modify either array afterwards.
  ImmutableSortedArrayMap(String[] keys, Object[] values) {
    assert keys.length == values.length;
    _keys = keys;
    _values = values;
  }

  /// Returns the empty map.
  @SuppressWarnings("unchecked")
  public static <V> ImmutableSortedArrayMap<V> empty() {
    return (ImmutableSortedArrayMap<V>) EMPTY;
  }

  /// Returns an immutable sorted copy of the given map. Keys must be non-null.
  public static <V> ImmutableSortedArrayMap<V> copyOf(Map<String, ? extends V> map) {
    if (map instanceof ImmutableSortedArrayMap) {
      @SuppressWarnings("unchecked")
      ImmutableSortedArrayMap<V> immutableMap = (ImmutableSortedArrayMap<V>) map;
      return immutableMap;
    }
    int size = map.size();
    if (size == 0) {
      return empty();
    }
    String[] keys = new String[size];
    Object[] values = new Object[size];
    int i = 0;
    for (Map.Entry<String, ? extends V> entry : map.entrySet()) {
      keys[i] = Objects.requireNonNull(entry.getKey(), "Null key");
      values[i] = entry.getValue();
      i++;
    }
    return sortAndWrap(keys, values, size);
  }

  /// Sorts the first `size` entries of the given parallel arrays by key and wraps them. When a key appears more than
  /// once, the last value wins, like repeated [Map#put] calls. The arrays may be modified and must not be used by the
  /// caller afterwards.
  static <V> ImmutableSortedArrayMap<V> sortAndWrap(String[] keys, Object[] values, int size) {
    if (size == 0) {
      return empty();
    }
    boolean strictlySorted = true;
    for (int i = 1; i < size; i++) {
      if (keys[i - 1].compareTo(keys[i]) >= 0) {
        strictlySorted = false;
        break;
      }
    }
    if (strictlySorted) {
      if (keys.length == size) {
        return new ImmutableSortedArrayMap<>(keys, values);
      }
      return new ImmutableSortedArrayMap<>(Arrays.copyOf(keys, size), Arrays.copyOf(values, size));
    }
    // Stable sort, so the last occurrence of a duplicated key stays the last in its run.
    Integer[] order = new Integer[size];
    for (int i = 0; i < size; i++) {
      order[i] = i;
    }
    Arrays.sort(order, Comparator.comparing(i -> keys[i]));
    String[] sortedKeys = new String[size];
    Object[] sortedValues = new Object[size];
    int numUnique = 0;
    for (int i = 0; i < size; i++) {
      int index = order[i];
      String key = keys[index];
      if (numUnique > 0 && sortedKeys[numUnique - 1].equals(key)) {
        sortedValues[numUnique - 1] = values[index];
      } else {
        sortedKeys[numUnique] = key;
        sortedValues[numUnique] = values[index];
        numUnique++;
      }
    }
    if (numUnique != size) {
      sortedKeys = Arrays.copyOf(sortedKeys, numUnique);
      sortedValues = Arrays.copyOf(sortedValues, numUnique);
    }
    return new ImmutableSortedArrayMap<>(sortedKeys, sortedValues);
  }

  /// Returns the index of the key, or `-(insertionPoint + 1)` when absent.
  private int indexOf(String key) {
    int low = 0;
    int high = _keys.length - 1;
    while (low <= high) {
      int mid = (low + high) >>> 1;
      String midKey = _keys[mid];
      int cmp = midKey == key ? 0 : midKey.compareTo(key);
      if (cmp < 0) {
        low = mid + 1;
      } else if (cmp > 0) {
        high = mid - 1;
      } else {
        return mid;
      }
    }
    return -(low + 1);
  }

  /// Returns the index of the first key that is greater than or equal to the given key.
  private int lowerBound(String key) {
    int index = indexOf(key);
    return index >= 0 ? index : -(index + 1);
  }

  /// Returns the key at the given position in sorted order.
  String keyAt(int index) {
    return _keys[index];
  }

  /// Returns the value at the given position in sorted order.
  @SuppressWarnings("unchecked")
  V valueAt(int index) {
    return (V) _values[index];
  }

  @Override
  public int size() {
    return _keys.length;
  }

  @Override
  public boolean isEmpty() {
    return _keys.length == 0;
  }

  @Override
  public boolean containsKey(Object key) {
    return key instanceof String && indexOf((String) key) >= 0;
  }

  @Override
  public boolean containsValue(Object value) {
    for (Object v : _values) {
      if (v == value || (v != null && v.equals(value))) {
        return true;
      }
    }
    return false;
  }

  @Nullable
  @Override
  public V get(Object key) {
    if (!(key instanceof String)) {
      return null;
    }
    int index = indexOf((String) key);
    return index >= 0 ? valueAt(index) : null;
  }

  @Override
  public V getOrDefault(Object key, V defaultValue) {
    if (!(key instanceof String)) {
      return defaultValue;
    }
    int index = indexOf((String) key);
    return index >= 0 ? valueAt(index) : defaultValue;
  }

  @Nullable
  @Override
  public Comparator<? super String> comparator() {
    return null;
  }

  @Override
  public String firstKey() {
    if (_keys.length == 0) {
      throw new NoSuchElementException();
    }
    return _keys[0];
  }

  @Override
  public String lastKey() {
    if (_keys.length == 0) {
      throw new NoSuchElementException();
    }
    return _keys[_keys.length - 1];
  }

  @Override
  public SortedMap<String, V> subMap(String fromKey, String toKey) {
    Objects.requireNonNull(fromKey);
    Objects.requireNonNull(toKey);
    if (fromKey.compareTo(toKey) > 0) {
      throw new IllegalArgumentException("fromKey > toKey");
    }
    return range(lowerBound(fromKey), lowerBound(toKey));
  }

  @Override
  public SortedMap<String, V> headMap(String toKey) {
    return range(0, lowerBound(Objects.requireNonNull(toKey)));
  }

  @Override
  public SortedMap<String, V> tailMap(String fromKey) {
    return range(lowerBound(Objects.requireNonNull(fromKey)), _keys.length);
  }

  /// Returns the entries in `[from, to)`. The result is a copy, which is indistinguishable from a view because
  /// neither map can change.
  private SortedMap<String, V> range(int from, int to) {
    if (from == 0 && to == _keys.length) {
      return this;
    }
    if (from >= to) {
      return empty();
    }
    return new ImmutableSortedArrayMap<>(Arrays.copyOfRange(_keys, from, to), Arrays.copyOfRange(_values, from, to));
  }

  @Override
  public Set<String> keySet() {
    KeySet keySet = _keySet;
    if (keySet == null) {
      keySet = new KeySet();
      _keySet = keySet;
    }
    return keySet;
  }

  @Override
  public Collection<V> values() {
    Values values = _valuesView;
    if (values == null) {
      values = new Values();
      _valuesView = values;
    }
    return values;
  }

  @Override
  public Set<Map.Entry<String, V>> entrySet() {
    EntrySet entrySet = _entrySet;
    if (entrySet == null) {
      entrySet = new EntrySet();
      _entrySet = entrySet;
    }
    return entrySet;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof ImmutableSortedArrayMap) {
      ImmutableSortedArrayMap<?> that = (ImmutableSortedArrayMap<?>) o;
      return Arrays.equals(_keys, that._keys) && Arrays.equals(_values, that._values);
    }
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    int hashCode = _hashCode;
    if (hashCode == 0) {
      for (int i = 0; i < _keys.length; i++) {
        hashCode += _keys[i].hashCode() ^ Objects.hashCode(_values[i]);
      }
      _hashCode = hashCode;
    }
    return hashCode;
  }

  // Mutators. AbstractMap already throws for put; the remaining ones are overridden so that no default method
  // reaches a mutable code path.

  @Override
  public V put(String key, V value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public V remove(Object key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putAll(Map<? extends String, ? extends V> m) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void replaceAll(BiFunction<? super String, ? super V, ? extends V> function) {
    throw new UnsupportedOperationException();
  }

  @Override
  public V putIfAbsent(String key, V value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean remove(Object key, Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean replace(String key, V oldValue, V newValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public V replace(String key, V value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public V computeIfAbsent(String key, Function<? super String, ? extends V> mappingFunction) {
    throw new UnsupportedOperationException();
  }

  @Override
  public V computeIfPresent(String key, BiFunction<? super String, ? super V, ? extends V> remappingFunction) {
    throw new UnsupportedOperationException();
  }

  @Override
  public V compute(String key, BiFunction<? super String, ? super V, ? extends V> remappingFunction) {
    throw new UnsupportedOperationException();
  }

  @Override
  public V merge(String key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
    throw new UnsupportedOperationException();
  }

  /// Iterator over the positions of the map. `remove` throws [UnsupportedOperationException] (the default).
  private abstract class ArrayIterator<T> implements Iterator<T> {
    private int _next;

    @Override
    public boolean hasNext() {
      return _next < _keys.length;
    }

    @Override
    public T next() {
      if (_next >= _keys.length) {
        throw new NoSuchElementException();
      }
      return elementAt(_next++);
    }

    abstract T elementAt(int index);
  }

  private final class KeySet extends AbstractSet<String> {
    @Override
    public Iterator<String> iterator() {
      return new ArrayIterator<String>() {
        @Override
        String elementAt(int index) {
          return _keys[index];
        }
      };
    }

    @Override
    public int size() {
      return _keys.length;
    }

    @Override
    public boolean contains(Object o) {
      return containsKey(o);
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (o instanceof ImmutableSortedArrayMap<?>.KeySet) {
        return Arrays.equals(_keys, ((ImmutableSortedArrayMap<?>.KeySet) o).keys());
      }
      return super.equals(o);
    }

    @Override
    public int hashCode() {
      int hashCode = _keySetHashCode;
      if (hashCode == 0) {
        for (String key : _keys) {
          hashCode += key.hashCode();
        }
        _keySetHashCode = hashCode;
      }
      return hashCode;
    }

    private String[] keys() {
      return _keys;
    }

    @Override
    public boolean add(String s) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException();
    }
  }

  private final class Values extends AbstractCollection<V> {
    @Override
    public Iterator<V> iterator() {
      return new ArrayIterator<V>() {
        @Override
        V elementAt(int index) {
          return valueAt(index);
        }
      };
    }

    @Override
    public int size() {
      return _keys.length;
    }

    @Override
    public boolean contains(Object o) {
      return containsValue(o);
    }

    @Override
    public boolean add(V v) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException();
    }
  }

  private final class EntrySet extends AbstractSet<Map.Entry<String, V>> {
    @Override
    public Iterator<Map.Entry<String, V>> iterator() {
      return new ArrayIterator<Map.Entry<String, V>>() {
        @Override
        Map.Entry<String, V> elementAt(int index) {
          return new AbstractMap.SimpleImmutableEntry<>(_keys[index], valueAt(index));
        }
      };
    }

    @Override
    public int size() {
      return _keys.length;
    }

    @Override
    public boolean contains(Object o) {
      if (!(o instanceof Map.Entry)) {
        return false;
      }
      Map.Entry<?, ?> entry = (Map.Entry<?, ?>) o;
      Object key = entry.getKey();
      if (!(key instanceof String)) {
        return false;
      }
      int index = indexOf((String) key);
      return index >= 0 && Objects.equals(_values[index], entry.getValue());
    }

    @Override
    public boolean equals(Object o) {
      return super.equals(o);
    }

    /// Same as [AbstractSet#hashCode], which sums the entry hash codes, but cached by the map.
    @Override
    public int hashCode() {
      return ImmutableSortedArrayMap.this.hashCode();
    }

    @Override
    public boolean add(Map.Entry<String, V> entry) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException();
    }
  }
}
