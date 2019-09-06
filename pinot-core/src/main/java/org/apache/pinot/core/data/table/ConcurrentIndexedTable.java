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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nonnull;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.data.order.OrderByUtils;


/**
 * Thread safe {@link Table} implementation for aggregating TableRecords based on combination of keys
 */
public class ConcurrentIndexedTable extends IndexedTable {

  private ConcurrentMap<Key, Record> _lookupMap;
  private ReentrantReadWriteLock _readWriteLock;

  private Comparator<Record> _minHeapComparator;
  private Comparator<Record> _orderByComparator;

  @Override
  public void init(@Nonnull DataSchema dataSchema, List<AggregationInfo> aggregationInfos, List<SelectionSort> orderBy,
      int maxCapacity, boolean sort) {
    super.init(dataSchema, aggregationInfos, orderBy, maxCapacity, sort);

    _lookupMap = new ConcurrentHashMap<>();
    _readWriteLock = new ReentrantReadWriteLock();
    _minHeapComparator = OrderByUtils.getKeysAndValuesComparator(dataSchema, orderBy, aggregationInfos).reversed();
    _orderByComparator = OrderByUtils.getKeysAndValuesComparator(dataSchema, orderBy, aggregationInfos);
  }

  /**
   * Thread safe implementation of upsert for inserting {@link Record} into {@link Table}
   */
  @Override
  public boolean upsert(@Nonnull Record newRecord) {

    Key key = newRecord.getKey();
    Preconditions.checkNotNull(key, "Cannot upsert record with null keys");

    Record existingRecord = _lookupMap.putIfAbsent(key, newRecord);
    if (existingRecord != null) {
      _lookupMap.compute(key, (k, v) -> {
        for (int i = 0; i < _aggregationFunctions.size(); i++) {
          v.getValues()[i] = _aggregationFunctions.get(i).merge(v.getValues()[i], newRecord.getValues()[i]);
        }
        return v;
      });
    }

    // resize if exceeds capacity
    if (_lookupMap.size() >= _bufferedCapacity) {
      _readWriteLock.writeLock().lock();
      try {
        if (_lookupMap.size() >= _bufferedCapacity) {
          resize(_evictCapacity);
        }
      } finally {
        _readWriteLock.writeLock().unlock();
      }
    }
    return true;
  }

  @Override
  public boolean merge(@Nonnull Table table) {
    Iterator<Record> iterator = table.iterator();
    while (iterator.hasNext()) {
      upsert(iterator.next());
    }
    return true;
  }

  @Override
  public int size() {
    return _lookupMap.size();
  }

  @Override
  public Iterator<Record> iterator() {
    if (_sort) {
      List<Record> sortedList = new ArrayList<>(_lookupMap.values());
      sortedList.sort(_orderByComparator);
      return sortedList.iterator();
    }
    return _lookupMap.values().iterator();
  }

  private void resize(int trimToSize) {

    if (_lookupMap.size() > trimToSize) {

      // make min heap of elements to evict
      int heapSize = _lookupMap.size() - trimToSize;
      PriorityQueue<Record> minHeap = new PriorityQueue<>(heapSize, _minHeapComparator);

      for (Record record : _lookupMap.values()) {
        if (minHeap.size() < heapSize) {
          minHeap.offer(record);
        } else {
          Record peek = minHeap.peek();
          if (minHeap.comparator().compare(record, peek) < 0) {
            minHeap.poll();
            minHeap.offer(record);
          }
        }
      }

      for (Record evictRecord : minHeap) {
        _lookupMap.remove(evictRecord.getKey());
      }
    }
  }

  @Override
  public void finish() {
    resize(_maxCapacity);
  }

  @Override
  public DataSchema getDataSchema() {
    return _dataSchema;
  }
}
