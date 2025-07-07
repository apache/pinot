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

import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.request.context.QueryContext;


/**
 * {@link Table} thread-safe implementation for orderby group key case, sorted on group keys and only `trimSize` groups
 * are kept
 */
@NotThreadSafe
public class ConcurrentSortedIndexedTable extends IndexedTable {
  private final ReadWriteLock _lock = new ReentrantReadWriteLock();
  /**
   * resizeThreshold controls when does the resize triggers. Once triggered, resize will always resize to
   * {@code resultSize}. Using a larger threshold reduces frequency of holding the write lock but increases intermediate
   * result size. This is defaulted to min(2 * limit, trimSize).
   */
  private final int _resizeThreshold;

  public ConcurrentSortedIndexedTable(DataSchema dataSchema, boolean hasFinalInput,
      QueryContext queryContext, int resultSize, int trimSize, ExecutorService executorService,
      Comparator<Key> keyComparator) {
    super(dataSchema, hasFinalInput, queryContext, resultSize, resultSize, Integer.MAX_VALUE,
        new ConcurrentSkipListMap<>(keyComparator),
        executorService);
    /// default to 2 * resultSize
    _resizeThreshold = Math.min(2 * resultSize, trimSize);
  }

  public ConcurrentSortedIndexedTable(DataSchema dataSchema, boolean hasFinalInput,
      QueryContext queryContext, int resultSize, ExecutorService executorService,
      Comparator<Key> keyComparator) {
    super(dataSchema, hasFinalInput, queryContext, resultSize, resultSize, Integer.MAX_VALUE,
        new ConcurrentSkipListMap<>(keyComparator),
        executorService);
    /// default to 2 * resultSize
    _resizeThreshold = 2 * resultSize;
  }

  @Override
  public boolean upsert(Key key, Record record) {
    _lock.readLock().lock();
    try {
      addOrUpdateRecord(key, record);
    } finally {
      _lock.readLock().unlock();
    }
    // trim when result size is too large
    if (_lookupMap.size() > _resizeThreshold) {
      _lock.writeLock().lock();
      try {
        if (_lookupMap.size() > _resizeThreshold) {
          _numResizes++;
          while (_lookupMap.size() > _resultSize) {
            ((ConcurrentSkipListMap<Key, Record>) _lookupMap).pollLastEntry();
          }
        }
      } finally {
        _lock.writeLock().unlock();
      }
    }
    return true;
  }

  /**
   * Adds a record with new key or updates a record with existing key.
   * NOTE: {@code compute} method of {@code ConcurrentSkipListMap} is not atomic, thus it's not used
   */
  protected void addOrUpdateRecord(Key key, Record newRecord) {
    Record existingRecord = _lookupMap.putIfAbsent(key, newRecord);
    if (existingRecord == null) {
      // if no key was associated
      return;
    }
    while (true) {
      Record oldRecord = _lookupMap.get(key);
      Record updatedRecord = updateRecord(oldRecord.copy(), newRecord);
      if (_lookupMap.replace(key, oldRecord, updatedRecord)) {
        return;
      }
    }
  }

  @Override
  public void finish(boolean sort) {
    if (_lookupMap.size() > _resultSize) {
      _numResizes++;
      while (_lookupMap.size() > _resultSize) {
        ((ConcurrentSkipListMap<Key, Record>) _lookupMap).pollLastEntry();
      }
    }
    // don't sort again since the map itself is sorted
    super.finish(false);
  }
}
