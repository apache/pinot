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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link Table} implementation for aggregating TableRecords based on combination of keys
 */
@NotThreadSafe
public class SimpleIndexedTable extends IndexedTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleIndexedTable.class);

  private final Map<Key, Record> _lookupMap;
  private Iterator<Record> _iterator;

  private boolean _noMoreNewRecords = false;
  private int _numResizes = 0;
  private long _resizeTime = 0;

  public SimpleIndexedTable(DataSchema dataSchema, QueryContext queryContext, int capacity) {
    super(dataSchema, queryContext, capacity);

    _lookupMap = new HashMap<>();
  }

  /**
   * Non thread safe implementation of upsert to insert {@link Record} into the {@link Table}
   */
  @Override
  public boolean upsert(Key key, Record newRecord) {
    Preconditions.checkNotNull(key, "Cannot upsert record with null keys");

    if (_noMoreNewRecords) { // allow only existing record updates
      _lookupMap.computeIfPresent(key, (k, v) -> {
        Object[] existingValues = v.getValues();
        Object[] newValues = newRecord.getValues();
        int aggNum = 0;
        for (int i = _numKeyColumns; i < _numColumns; i++) {
          existingValues[i] = _aggregationFunctions[aggNum++].merge(existingValues[i], newValues[i]);
        }
        return v;
      });
    } else { // allow all records

      _lookupMap.compute(key, (k, v) -> {
        if (v == null) {
          return newRecord;
        } else {
          Object[] existingValues = v.getValues();
          Object[] newValues = newRecord.getValues();
          int aggNum = 0;
          for (int i = _numKeyColumns; i < _numColumns; i++) {
            existingValues[i] = _aggregationFunctions[aggNum++].merge(existingValues[i], newValues[i]);
          }
          return v;
        }
      });

      if (_lookupMap.size() >= _maxCapacity) {
        if (_hasOrderBy) {
          // reached max capacity, resize
          resize(_capacity);
        } else {
          // reached max capacity and no order by. No more new records will be accepted
          _noMoreNewRecords = true;
        }
      }
    }
    return true;
  }

  private void resize(int trimToSize) {

    long startTime = System.currentTimeMillis();

    _tableResizer.resizeRecordsMap(_lookupMap, trimToSize);

    long endTime = System.currentTimeMillis();
    long timeElapsed = endTime - startTime;

    _numResizes++;
    _resizeTime += timeElapsed;
  }

  private List<Record> resizeAndSort(int trimToSize) {

    long startTime = System.currentTimeMillis();

    List<Record> sortedRecords = _tableResizer.resizeAndSortRecordsMap(_lookupMap, trimToSize);

    long endTime = System.currentTimeMillis();
    long timeElapsed = endTime - startTime;

    _numResizes++;
    _resizeTime += timeElapsed;

    return sortedRecords;
  }

  @Override
  public int size() {
    return _lookupMap.size();
  }

  @Override
  public Iterator<Record> iterator() {
    return _iterator;
  }

  @Override
  public void finish(boolean sort) {

    if (_hasOrderBy) {

      if (sort) {
        List<Record> sortedRecords = resizeAndSort(_capacity);
        _iterator = sortedRecords.iterator();
      } else {
        resize(_capacity);
      }
      LOGGER
          .debug("Num resizes : {}, Total time spent in resizing : {}, Avg resize time : {}", _numResizes, _resizeTime,
              _numResizes == 0 ? 0 : _resizeTime / _numResizes);
    }

    if (_iterator == null) {
      _iterator = _lookupMap.values().iterator();
    }
  }
}
