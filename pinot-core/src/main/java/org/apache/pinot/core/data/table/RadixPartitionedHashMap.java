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
package org.apache.pinot.core.data.table;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.ws.rs.NotSupportedException;


/**
 * Radix partitioned hashtable that provides a single view for multiple hashtable that could be indexed
 */
public class RadixPartitionedHashMap<K, V> implements Map<K, V> {
  private final int _numRadixBits;
  private final int _numPartitions;
  private final int _mask;
  private final List<HashMap<K, V>> _maps;
  private int _size;
  private int _segmentId = -1;

  public RadixPartitionedHashMap(int numRadixBits, int initialCapacity, int segmentId) {
    int partitionInitialCapacity = initialCapacity >> numRadixBits;
    _numRadixBits = numRadixBits;
    _numPartitions = 1 << numRadixBits;
    _mask = _numPartitions - 1;
    _segmentId = segmentId;
    _maps = new ArrayList<>();
    _size = 0;
    for (int i = 0; i < _numPartitions; i++) {
      _maps.add(new HashMap<>(partitionInitialCapacity));
    }
  }

  public RadixPartitionedHashMap(List<HashMap<K, V>> maps, int numRadixBits) {
    _numRadixBits = numRadixBits;
    _numPartitions = 1 << numRadixBits;
    assert (maps.size() == _numPartitions);
    _mask = _numPartitions - 1;
    _maps = maps;
    _size = 0;
    for (HashMap<K, V> map : maps) {
      _size += map.size();
    }
  }

  public int getSegmentId() {
    return _segmentId;
  }

  public Map<K, V> getPartition(int i) {
    return _maps.get(i);
  }

  public int getNumPartitions() {
    return _numPartitions;
  }

  private int partition(K key) {
    return key.hashCode() & _mask;
  }

  @Override
  public int size() {
    return _size;
  }

  @Override
  public boolean isEmpty() {
    return _size == 0;
  }

  @Override
  public boolean containsKey(Object o) {
    HashMap<K, V> map = _maps.get(partition((K) o));
    return map.containsKey(o);
  }

  @Override
  public boolean containsValue(Object o) {
    throw new NotSupportedException("partitioned map does not support lookup by value");
  }

  @Override
  public V get(Object o) {
    HashMap<K, V> map = _maps.get(partition((K) o));
    return map.get(o);
  }

  @Nullable
  @Override
  public V put(K k, V v) {
    HashMap<K, V> map = _maps.get(partition(k));
    V prev = map.put(k, v);
    if (prev == null) {
      _size++;
    }
    return prev;
  }

  @Override
  public V remove(Object o) {
    HashMap<K, V> map = _maps.get(partition((K) o));
    V prev = map.remove(o);
    if (prev != null) {
      _size--;
    }
    return prev;
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> map) {
    throw new NotSupportedException("partitioned map does not support removing by value");
  }

  @Override
  public void clear() {
    for (HashMap<K, V> map : _maps) {
      map.clear();
    }
    _size = 0;
  }

  @Override
  public Set<K> keySet() {
    Set<K> set = new HashSet<>();
    _maps.forEach(m -> set.addAll(m.keySet()));
    return set;
  }

  @Override
  public Collection<V> values() {
    List<V> list = new ArrayList<>();
    _maps.forEach(m -> list.addAll(m.values()));
    return list;
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    Set<Entry<K, V>> set = new HashSet<>();
    _maps.forEach(m -> set.addAll(m.entrySet()));
    return set;
  }
}
