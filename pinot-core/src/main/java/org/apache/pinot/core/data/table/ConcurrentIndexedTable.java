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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nonnull;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.data.order.OrderByUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Thread safe {@link Table} implementation for aggregating TableRecords based on combination of keys
 */
public class ConcurrentIndexedTable extends IndexedTable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrentIndexedTable.class);

  private ConcurrentMap<Key, Record> _lookupMap;
  private ReentrantReadWriteLock _readWriteLock;

  private boolean _isOrderBy;
  private Comparator<Record> _resizeOrderByComparator;
  private Comparator<Record> _finishOrderByComparator;
  private int[] _aggregationIndexes;
  private Iterator<Record> _iterator;

  private AtomicBoolean _noMoreNewRecords = new AtomicBoolean();
  private final AtomicInteger _numResizes = new AtomicInteger();
  private final AtomicLong _resizeTime = new AtomicLong();

  /**
   * Initializes the data structures and comparators needed for this Table
   * @param dataSchema data schema of the record's keys and values
   * @param aggregationInfos aggregation infos for the aggregations in record's values
   * @param orderBy list of {@link SelectionSort} defining the order by
   * @param capacity the max number of records to hold
   */
  @Override
  public void init(@Nonnull DataSchema dataSchema, List<AggregationInfo> aggregationInfos, List<SelectionSort> orderBy,
      int capacity) {
    super.init(dataSchema, aggregationInfos, orderBy, capacity);

    _lookupMap = new ConcurrentHashMap<>();
    _readWriteLock = new ReentrantReadWriteLock();
    _isOrderBy = CollectionUtils.isNotEmpty(orderBy);
    if (_isOrderBy) {
      // get indices of aggregations to extract final results upfront
      // FIXME: at instance level, extract final results only if intermediate result is non-comparable, instead of for all aggregations
      _aggregationIndexes = OrderByUtils.getAggregationIndexes(orderBy, aggregationInfos);
      // resize comparator doesn't need to extract final results, because it will be done before hand when adding Records to the PQ.
      _resizeOrderByComparator = OrderByUtils.getKeysAndValuesComparator(dataSchema, orderBy, aggregationInfos, false);
      // finish comparator needs to extract final results, as it cannot be done before hand. The _lookupMap will get modified if it is done before hand
      _finishOrderByComparator = OrderByUtils.getKeysAndValuesComparator(dataSchema, orderBy, aggregationInfos, true);
    }
  }

  /**
   * Thread safe implementation of upsert for inserting {@link Record} into {@link Table}
   */
  @Override
  public boolean upsert(@Nonnull Record newRecord) {

    Key key = newRecord.getKey();
    Preconditions.checkNotNull(key, "Cannot upsert record with null keys");

    if (_noMoreNewRecords.get()) { // allow only existing record updates
      _lookupMap.computeIfPresent(key, (k, v) -> {
        for (int i = 0; i < _numAggregations; i++) {
          v.getValues()[i] = _aggregationFunctions.get(i).merge(v.getValues()[i], newRecord.getValues()[i]);
        }
        return v;
      });
    } else { // allow all records

      _readWriteLock.readLock().lock();
      try {
        _lookupMap.compute(key, (k, v) -> {
          if (v == null) {
            return newRecord;
          } else {
            for (int i = 0; i < _numAggregations; i++) {
              v.getValues()[i] = _aggregationFunctions.get(i).merge(v.getValues()[i], newRecord.getValues()[i]);
            }
            return v;
          }
        });
      } finally {
        _readWriteLock.readLock().unlock();
      }

      // resize if exceeds capacity
      if (_lookupMap.size() >= _bufferedCapacity) {
        if (_isOrderBy) {
          // reached capacity, resize
          _readWriteLock.writeLock().lock();
          try {
            if (_lookupMap.size() >= _bufferedCapacity) {
              resize(_maxCapacity);
            }
          } finally {
            _readWriteLock.writeLock().unlock();
          }
        } else {
          // reached capacity and no order by. No more new records will be accepted
          _noMoreNewRecords.set(true);
        }
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
    return _iterator;
  }

  private void resize(int trimToSize) {

    if (_lookupMap.size() > trimToSize) {
      long startTime = System.currentTimeMillis();

      if (_isOrderBy) {
        // drop bottom

        // make min heap of elements to evict
        int heapSize = _lookupMap.size() - trimToSize;
        PriorityQueue<Record> minHeap = new PriorityQueue<>(heapSize, _resizeOrderByComparator);

        for (Record record : _lookupMap.values()) {

          // extract final results before hand for comparisons on aggregations
          // FIXME: at instance level, extract final results only if intermediate result is non-comparable, instead of for all aggregations
          if (_aggregationIndexes.length > 0) {
            Object[] values = record.getValues();
            for (int index : _aggregationIndexes) {
              values[index] = _aggregationFunctions.get(index).extractFinalResult(values[index]);
            }
          }
          if (minHeap.size() < heapSize) {
            minHeap.offer(record);
          } else {
            Record peek = minHeap.peek();
            if (minHeap.comparator().compare(peek, record) < 0) {
              minHeap.poll();
              minHeap.offer(record);
            }
          }
        }

        for (Record evictRecord : minHeap) {
          _lookupMap.remove(evictRecord.getKey());
        }
      } else {
        // drop randomly

        int numRecordsToDrop = _lookupMap.size() - trimToSize;
        for (Key evictKey : _lookupMap.keySet()) {
          _lookupMap.remove(evictKey);
          numRecordsToDrop --;
          if (numRecordsToDrop == 0) {
            break;
          }
        }
      }
      long endTime = System.currentTimeMillis();
      long timeElapsed = endTime - startTime;

      _numResizes.incrementAndGet();
      _resizeTime.addAndGet(timeElapsed);
    }
  }

  @Override
  public void finish(boolean sort) {
    resize(_maxCapacity);
    int numResizes = _numResizes.get();
    long resizeTime = _resizeTime.get();
    LOGGER.debug("Num resizes : {}, Total time spent in resizing : {}, Avg resize time : {}", numResizes, resizeTime,
        numResizes == 0 ? 0 : resizeTime / numResizes);

    _iterator = _lookupMap.values().iterator();
    if (sort && _isOrderBy) {
      // TODO: in this final sort, we can optimize again by extracting final results before hand.
      // This could be done by adding another parameter to finish(sort, extractFinalResults)
      // The caller then does not have to extract final results again, if extractFinalResults=true was passed to finish.
      // Typically, at instance level, we will not need to sort, nor do we want final results - finish(true, true).
      // At broker level we need to sort, and we also want final results - finish(true, true)
      List<Record> sortedList = new ArrayList<>(_lookupMap.values());
      sortedList.sort(_finishOrderByComparator);
      _iterator = sortedList.iterator();
    }
  }

  @Override
  public DataSchema getDataSchema() {
    return _dataSchema;
  }
}
