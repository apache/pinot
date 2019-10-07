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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.data.order.OrderByUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link Table} implementation for aggregating TableRecords based on combination of keys
 */
@NotThreadSafe
public class SimpleIndexedTable extends IndexedTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleIndexedTable.class);

  private List<Record> _records;
  private Map<Key, Integer> _lookupTable;

  private boolean _isOrderBy;
  private Comparator<Record> _orderByComparator;
  private Iterator<Record> _iterator;

  private boolean _noMoreNewRecords = false;
  private int _numResizes = 0;
  private long _resizeTime = 0;

  /**
   * Initializes the data structures and comparators needed for this Table
   * @param dataSchema data schema of the record's keys and values
   * @param aggregationInfos aggregation infors for the aggregations in record'd values
   * @param orderBy list of {@link SelectionSort} defining the order by
   * @param capacity the max number of records to hold
   */
  @Override
  public void init(@Nonnull DataSchema dataSchema, List<AggregationInfo> aggregationInfos, List<SelectionSort> orderBy,
      int capacity) {
    super.init(dataSchema, aggregationInfos, orderBy, capacity);

    _records = new ArrayList<>(capacity);
    _lookupTable = new HashMap<>(capacity);

    _isOrderBy = CollectionUtils.isNotEmpty(orderBy);
    if (_isOrderBy) {
      // final results not extracted upfront
      _orderByComparator = OrderByUtils.getKeysAndValuesComparator(dataSchema, orderBy, aggregationInfos, true);
    }
  }

  /**
   * Non thread safe implementation of upsert to insert {@link Record} into the {@link Table}
   */
  @Override
  public boolean upsert(@Nonnull Record newRecord) {
    Key keys = newRecord.getKey();
    Preconditions.checkNotNull(keys, "Cannot upsert record with null keys");

    Integer index = _lookupTable.get(keys);
    if (_noMoreNewRecords) { // only update existing records
      if (index != null) {
        Record existingRecord = _records.get(index);
        for (int i = 0; i < _numAggregations; i++) {
          existingRecord.getValues()[i] = _aggregationFunctions.get(i).merge(existingRecord.getValues()[i], newRecord.getValues()[i]);
        }
      }
    } else { // allow all records
      if (index == null) {
        index = size();
        _lookupTable.put(keys, index);
        _records.add(index, newRecord);
      } else {
        Record existingRecord = _records.get(index);
        for (int i = 0; i < _numAggregations; i++) {
          existingRecord.getValues()[i] = _aggregationFunctions.get(i).merge(existingRecord.getValues()[i], newRecord.getValues()[i]);
        }
      }

      if (size() >= _bufferedCapacity) {
        if (_isOrderBy) { // capacity reached, order and resize
          sortAndResize(_maxCapacity);
        } else { // capacity reached, but no order by. Allow no more records
          _noMoreNewRecords = true;
        }
      }
    }
    return true;
  }

  private void sortAndResize(int trimToSize) {
    long startTime = System.currentTimeMillis();

    // sort
    if (_isOrderBy) {
      _records.sort(_orderByComparator);
    }

    // evict lowest (or whatever's at the bottom if sort didnt happen)
    if (_records.size() > trimToSize) {
      _records = new ArrayList<>(_records.subList(0, trimToSize));
    }

    // rebuild lookup table
    _lookupTable.clear();
    for (int i = 0; i < _records.size(); i++) {
      _lookupTable.put(_records.get(i).getKey(), i);
    }

    long endTime = System.currentTimeMillis();
    long timeElapsed = endTime - startTime;

    _numResizes++;
    _resizeTime += timeElapsed;
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
    return _records.size();
  }

  @Override
  public Iterator<Record> iterator() {
    return _iterator;
  }

  @Override
  public void finish(boolean sort) {
    sortAndResize(_maxCapacity);
    LOGGER.info("Num resizes : {}, Total time spent in resizing : {}, Avg resize time : {}", _numResizes, _resizeTime,
        _numResizes == 0 ? 0 : _resizeTime / _numResizes);

    _iterator = _records.iterator();
  }

  @Override
  public DataSchema getDataSchema() {
    return _dataSchema;
  }
}
