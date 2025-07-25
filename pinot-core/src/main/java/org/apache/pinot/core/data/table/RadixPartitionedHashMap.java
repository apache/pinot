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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.ws.rs.NotSupportedException;


/**
 * Radix partitioned hashtable that provides a single view for multiple hashtable that could be indexed
 */
public class RadixPartitionedHashMap implements Map<Key, Record> {
  private final int _numRadixBits;
  private final int _numPartitions;
  private final int _mask;
  private final TwoLevelLinearProbingRecordHashmap[] _maps;
  private int _size;
  private int _segmentId = -1;

  public RadixPartitionedHashMap(int numRadixBits, int initialCapacity, int segmentId) {
    int partitionInitialCapacity = initialCapacity >> numRadixBits;
    _numRadixBits = numRadixBits;
    _numPartitions = 1 << numRadixBits;
    _mask = _numPartitions - 1;
    _segmentId = segmentId;
    _maps = new TwoLevelLinearProbingRecordHashmap[_numPartitions];
    _size = 0;
    for (int i = 0; i < _numPartitions; i++) {
      _maps[i] = new TwoLevelLinearProbingRecordHashmap();
    }
  }

  public RadixPartitionedHashMap(TwoLevelLinearProbingRecordHashmap[] maps, int numRadixBits) {
    _numRadixBits = numRadixBits;
    _numPartitions = 1 << numRadixBits;
    assert (maps.length == _numPartitions);
    _mask = _numPartitions - 1;
    _maps = maps;
    _size = 0;
    for (TwoLevelLinearProbingRecordHashmap map : maps) {
      _size += map.size();
    }
  }

  public int getSegmentId() {
    return _segmentId;
  }

  public TwoLevelLinearProbingRecordHashmap getPartition(int i) {
    return _maps[i];
  }

  public int getNumPartitions() {
    return _numPartitions;
  }

  public int partition(Key key) {
    return key.hashCode() & _mask;
  }

  public int partitionFromHashCode(int hash) {
    return hash & _mask;
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
    TwoLevelLinearProbingRecordHashmap map = _maps[partition((Key) o)];
    return map.get((Key) o) != null;
  }

  @Override
  public boolean containsValue(Object o) {
    throw new NotSupportedException("partitioned map does not support lookup by value");
  }

  @Override
  public Record get(Object o) {
    TwoLevelLinearProbingRecordHashmap map = _maps[partition((Key) o)];
    return map.get((Key) o);
  }

  public Record putIntoPartition(Key k, Record v, int partition) {
    TwoLevelLinearProbingRecordHashmap map = _maps[partition];
    Record prev = map.put((Key) k, (Record) v);
    if (prev == null) {
      _size++;
    }
    return prev;
  }

  @Nullable
  @Override
  public Record put(Key k, Record v) {
    TwoLevelLinearProbingRecordHashmap map = _maps[partition(k)];
    Record prev = map.put(k, v);
    if (prev == null) {
      _size++;
    }
    return prev;
  }

  @Override
  public Record remove(Object o) {
    throw new NotSupportedException("don't remove");
//    TwoLevelLinearProbingRecordHashmap map = _maps[partition((Key) o)];
//    Record prev = map.remove(o);
//    if (prev != null) {
//      _size--;
//    }
//    return prev;
  }

  @Override
  public void putAll(Map<? extends Key, ? extends Record> map) {
    throw new NotSupportedException("partitioned map does not support removing by value");
  }

  @Override
  public void clear() {
    for (TwoLevelLinearProbingRecordHashmap map : _maps) {
      map.clear();
    }
    _size = 0;
  }

  @Override
  public Set<Key> keySet() {
    Set<Key> set = new HashSet<>();
    for (TwoLevelLinearProbingRecordHashmap map : _maps) {
      set.addAll(map.keys());
    }
    return set;
  }

  @Override
  public Collection<Record> values() {
    List<Record> list = new ArrayList<>();
    for (TwoLevelLinearProbingRecordHashmap map : _maps) {
      list.addAll(map.values());
    }
    return list;
  }

  @Override
  public Set<Entry<Key, Record>> entrySet() {
    Set<Entry<Key, Record>> set = new HashSet<>();
    for (TwoLevelLinearProbingRecordHashmap map : _maps) {
      set.addAll(map.entrySet());
    }
    return set;
  }

  public Iterator<IntermediateRecord> iterator() {
    return new RecordIterator();
  }

  public class RecordIterator implements Iterator<IntermediateRecord> {
    int _partitionId;
    Iterator<IntermediateRecord> _curIt;

    RecordIterator() {
      _partitionId = 0;
      _curIt = getPartition(0).iterator();
    }

    @Override
    public boolean hasNext() {
      while (true) {
        if (_curIt != null && _curIt.hasNext()) {
          return true;
        }
        _partitionId++;
        if (_partitionId >= _numPartitions) {
          return false;
        }
        _curIt = getPartition(_partitionId).iterator();
      }
    }

    @Override
    public IntermediateRecord next() {
      while (!_curIt.hasNext()) {
        _partitionId++;
        _curIt = getPartition(_partitionId).iterator();
      }
      return _curIt.next();
    }
  }

  public List<Record> getPayloads(int partition) {
    return _maps[partition].getPayloads();
  }
}
