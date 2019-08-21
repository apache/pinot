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
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nonnull;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.data.order.OrderByUtils;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;


/**
 * {@link Table} implementation for aggregating TableRecords based on combination of keys
 */
public class IndexedTable implements Table {

  // When table reaches max capacity, we will allow 20% more records to get inserted (bufferedCapacity)
  // If records beyond bufferedCapacity are received, the table will undergo sort and evict upto evictCapacity (10% more than capacity)
  // This is to ensure that for a small number beyond capacity, a fair chance is given to all records which have the potential to climb up the order
  /** Factor used to add buffer to maxCapacity of the Collection used **/
  private static final double BUFFER_FACTOR = 1.2;
  /** Factor used to decide eviction threshold **/
  private static final double EVICTION_FACTOR = 1.1;

  private List<Record> _records;
  private ConcurrentMap<Record, Integer> _lookupTable;
  private ReentrantReadWriteLock _readWriteLock;
  private AtomicInteger _numRecords = new AtomicInteger();

  private DataSchema _dataSchema;
  private List<AggregationInfo> _aggregationInfos;
  private List<AggregationFunction> _aggregationFunctions;
  private List<SelectionSort> _orderBy;
  private int _evictCapacity;
  private int _bufferedCapacity;

  @Override
  public void init(@Nonnull DataSchema dataSchema, List<AggregationInfo> aggregationInfos, List<SelectionSort> orderBy,
      int maxCapacity) {
    _dataSchema = dataSchema;
    _aggregationInfos = aggregationInfos;
    _orderBy = orderBy;

    _bufferedCapacity = (int) (maxCapacity * BUFFER_FACTOR);
    _evictCapacity = (int) (maxCapacity * EVICTION_FACTOR);

    _records = Collections.synchronizedList(new ArrayList<>(_bufferedCapacity));
    _lookupTable = new ConcurrentHashMap<>(_bufferedCapacity);
    _readWriteLock = new ReentrantReadWriteLock();

    _aggregationFunctions = new ArrayList<>(aggregationInfos.size());
    for (AggregationInfo aggregationInfo : aggregationInfos) {
      _aggregationFunctions.add(
          AggregationFunctionUtils.getAggregationFunctionContext(aggregationInfo).getAggregationFunction());
    }
  }

  @Override
  public boolean upsert(@Nonnull Record newRecord) {

    Object[] keys = newRecord.getKeys();
    Preconditions.checkNotNull(keys, "Cannot upsert record with null keys");

    _lookupTable.compute(newRecord, (k, index) -> {
      if (index == null && size() >= _bufferedCapacity) {
        // It is possible that the table has more records than _bufferedCapacity momentarily
        // For eg. if capacity = 10, and current size = 9.
        // Multiple threads reach this check, they all see size() < _bufferedCapacity, and they all add new elements
        // This is okay, because when the next new element is received, the table will be resized.
        // Momentarily, we will have extra elements, but they will never exceed num parallel threads (which is very low)
        // In order to avoid this, each new upsert needs to acquire the write lock, and we will pay a performance penalty
        _readWriteLock.writeLock().lock();
        try {
          if (size() >= _bufferedCapacity) {
            resize();
          }
        } finally {
          _readWriteLock.writeLock().unlock();
        }
      }

      _readWriteLock.readLock().lock();
      try {
        if (index == null) {
          index = _numRecords.getAndIncrement();
          _records.add(index, newRecord);
        } else {
          Record existingRecord = _records.get(index);
          aggregate(existingRecord, newRecord);
        }
      } finally {
        _readWriteLock.readLock().unlock();
      }
      return index;
    });

    return true;
  }

  private void aggregate(Record existingRecord, Record newRecord) {
    for (int i = 0; i < _aggregationFunctions.size(); i++) {
      existingRecord.getValues()[i] =
          _aggregationFunctions.get(i).merge(existingRecord.getValues()[i], newRecord.getValues()[i]);
    }
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
    return _numRecords.get();
  }

  @Override
  public Iterator<Record> iterator() {
    return _records.iterator();
  }

  private void resize() {
    // sort
    if (CollectionUtils.isNotEmpty(_orderBy)) {
      Comparator<Record> comparator;
      comparator = OrderByUtils.getKeysAndValuesComparator(_dataSchema, _orderBy, _aggregationInfos);
      _records.sort(comparator);
    }

    // evict lowest
    _records = Collections.synchronizedList(new ArrayList<>(_records.subList(0, _evictCapacity)));
    _numRecords.set(_records.size());

    // rebuild lookup table
    _lookupTable.clear();
    for (int i = 0; i < _records.size(); i++) {
      _lookupTable.put(_records.get(i), i);
    }
  }
}
