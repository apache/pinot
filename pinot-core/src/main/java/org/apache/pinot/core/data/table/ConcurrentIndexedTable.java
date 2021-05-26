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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Thread safe {@link Table} implementation for aggregating Records based on combination of keys
 */
public class ConcurrentIndexedTable extends IndexedTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrentIndexedTable.class);


  protected volatile ConcurrentMap<Key, Record> _lookupMap;
  protected final AtomicBoolean _noMoreNewRecords = new AtomicBoolean();
  private Iterator<Record> _iterator;
  private final ReentrantReadWriteLock _readWriteLock;
  private final AtomicInteger _numResizes = new AtomicInteger();
  private final AtomicLong _resizeTimeMs = new AtomicLong();

  public ConcurrentIndexedTable(DataSchema dataSchema, QueryContext queryContext, int trimSize,
      int trimThreshold) {
    super(dataSchema, queryContext, trimSize, trimThreshold);
    _lookupMap = new ConcurrentHashMap<>();
    _readWriteLock = new ReentrantReadWriteLock();
  }

  /**
   * Thread safe implementation of upsert for inserting {@link Record} into {@link Table}
   */
  @Override
  public boolean upsert(Key key, Record newRecord) {
    Preconditions.checkNotNull(key, "Cannot upsert record with null keys");
    if (_noMoreNewRecords.get()) { // allow only existing record updates
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

      _readWriteLock.readLock().lock();
      try {
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
      } finally {
        _readWriteLock.readLock().unlock();
      }

      // resize if exceeds trim threshold
      if (_lookupMap.size() >= _trimThreshold) {
        if (_hasOrderBy) {
          // reached capacity, resize
          _readWriteLock.writeLock().lock();
          try {
            if (_lookupMap.size() >= _trimThreshold) {
              resize(_trimSize);
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
  public int size() {
    return _sortedRecords == null ? _lookupMap.size() : _sortedRecords.size();
  }

  @Override
  public Iterator<Record> iterator() {
    return _iterator;
  }

  private void resize(int trimToSize) {
    long startTime = System.currentTimeMillis();
    // when the resizer trims using a PQ, it will return a new trimmed map.
    // the reference held by the indexed table needs to be updated. this is also
    // the reason why it is volatile since the thread doing the resize will result in
    // a new reference
    _lookupMap = (ConcurrentMap)_tableResizer.resizeRecordsMap(_lookupMap, trimToSize);
    long endTime = System.currentTimeMillis();
    long timeElapsed = endTime - startTime;
    _numResizes.incrementAndGet();
    _resizeTimeMs.addAndGet(timeElapsed);
  }

  private List<Record> resizeAndSort(int trimToSize) {
    long startTime = System.currentTimeMillis();
    List<Record> sortedRecords = _tableResizer.sortRecordsMap(_lookupMap, trimToSize);
    long endTime = System.currentTimeMillis();
    long timeElapsed = endTime - startTime;
    _numResizes.incrementAndGet();
    _resizeTimeMs.addAndGet(timeElapsed);
    return sortedRecords;
  }

  @Override
  public void finish(boolean sort) {
    if (_hasOrderBy) {
      if (sort) {
        _sortedRecords = resizeAndSort(_trimSize);
        _iterator = _sortedRecords.iterator();
      } else {
        resize(_trimSize);
      }
      int numResizes = _numResizes.get();
      long resizeTime = _resizeTimeMs.get();
      LOGGER.debug(
          "Num resizes : {}, Total time spent in resizing : {}, Avg resize time : {}, trimSize: {}, trimThreshold: {}",
          numResizes, resizeTime, numResizes == 0 ? 0 : resizeTime / numResizes, _trimSize, _trimThreshold);
    }
    if (_iterator == null) {
      _iterator = _lookupMap.values().iterator();
    }
  }

  @Override
  public int getNumResizes() {
    return _numResizes.get();
  }

  @Override
  public long getResizeTimeMs() {
    return _resizeTimeMs.get();
  }
}
