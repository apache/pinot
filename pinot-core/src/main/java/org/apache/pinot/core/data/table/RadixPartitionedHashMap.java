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
  private final int _numPartitions;
  private final int _mask;
  private final TwoLevelLinearProbingRecordHashMap[] _maps;
  private int _size;

  public RadixPartitionedHashMap(TwoLevelLinearProbingRecordHashMap[] maps, int numRadixBits) {
    _numPartitions = 1 << numRadixBits;
    assert (maps.length == _numPartitions);
    _mask = _numPartitions - 1;
    _maps = maps;
    _size = 0;
    for (TwoLevelLinearProbingRecordHashMap map : maps) {
      _size += map.size();
    }
  }

  public TwoLevelLinearProbingRecordHashMap getPartition(int i) {
    return _maps[i];
  }

  public int getNumPartitions() {
    return _numPartitions;
  }

  public int partition(Key key) {
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
    TwoLevelLinearProbingRecordHashMap map = _maps[partition((Key) o)];
    return map.get((Key) o) != null;
  }

  @Override
  public boolean containsValue(Object o) {
    throw new NotSupportedException("partitioned map does not support lookup by value");
  }

  @Override
  public Record get(Object o) {
    TwoLevelLinearProbingRecordHashMap map = _maps[partition((Key) o)];
    return map.get((Key) o);
  }

  @Nullable
  @Override
  public Record put(Key k, Record v) {
    TwoLevelLinearProbingRecordHashMap map = _maps[partition(k)];
    Record prev = map.put(k, v);
    if (prev == null) {
      _size++;
    }
    return prev;
  }

  @Override
  public Record remove(Object o) {
    throw new NotSupportedException("don't remove");
  }

  @Override
  public void putAll(Map<? extends Key, ? extends Record> map) {
    throw new NotSupportedException("partitioned map does not support removing by value");
  }

  @Override
  public void clear() {
    for (TwoLevelLinearProbingRecordHashMap map : _maps) {
      map.clear();
    }
    _size = 0;
  }

  @Override
  public Set<Key> keySet() {
    Set<Key> set = new HashSet<>(_size);
    for (TwoLevelLinearProbingRecordHashMap map : _maps) {
      set.addAll(map.keys());
    }
    return set;
  }

  @Override
  public Collection<Record> values() {
    List<Record> list = new ArrayList<>(_size);
    for (TwoLevelLinearProbingRecordHashMap map : _maps) {
      list.addAll(map.values());
    }
    return list;
  }

  @Override
  public Set<Entry<Key, Record>> entrySet() {
    throw new NotSupportedException("Use iterator or getPayloads instead");
  }

  public List<Record> getPayloads(int partition) {
    return _maps[partition].getPayloads();
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
}
