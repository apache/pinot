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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
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
  private Comparator<Record> _minHeapComparator;
  private ReentrantReadWriteLock _readWriteLock;
  private Lock[] _locks;
  private static final int NUM_KEYS = 10000;

  @Override
  public void init(@Nonnull DataSchema dataSchema, List<AggregationInfo> aggregationInfos, List<SelectionSort> orderBy,
      int maxCapacity) {
    super.init(dataSchema, aggregationInfos, orderBy, maxCapacity);

    _minHeapComparator =
        OrderByUtils.getKeysAndValuesComparator(dataSchema, orderBy, aggregationInfos).reversed();
    _lookupMap = new ConcurrentHashMap<>();

    _readWriteLock = new ReentrantReadWriteLock();
    _locks = new Lock[NUM_KEYS];
    for (int i = 0; i < NUM_KEYS; i ++) {
      _locks[i] = new ReentrantLock();
    }
  }

  /**
   * Thread safe implementation of upsert for inserting {@link Record} into {@link Table}
   */
  @Override
  public boolean upsert(@Nonnull Record newRecord) {

    Key key = newRecord.getKey();
    Preconditions.checkNotNull(key, "Cannot upsert record with null keys");

    // synchronize on same key
    int lockNum = Math.abs(key.hashCode()) % NUM_KEYS;
    _locks[lockNum].lock();

    Record existingRecord = _lookupMap.get(key);
    if (existingRecord == null) {

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

      // insert new record
      _readWriteLock.readLock().lock();
      try {
        _lookupMap.put(key, newRecord);
      } finally {
        _readWriteLock.readLock().unlock();
      }

    } else {

      // update old record
      _readWriteLock.readLock().lock();
      try {
        aggregate(existingRecord, newRecord);
      } finally {
        _readWriteLock.readLock().unlock();
      }
    }

    _locks[lockNum].unlock();

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
    return _lookupMap.values().iterator();
  }

  private void resize(int trimToSize) {

    if (_lookupMap.size() > trimToSize) {

      // make heap of elements to evict
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
}
