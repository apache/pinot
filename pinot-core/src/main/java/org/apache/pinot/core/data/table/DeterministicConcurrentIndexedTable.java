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


import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.request.context.QueryContext;


public class DeterministicConcurrentIndexedTable extends IndexedTable {
  private final AtomicBoolean _noMoreNewRecords = new AtomicBoolean();
  private final ReentrantReadWriteLock _readWriteLock = new ReentrantReadWriteLock();

  public DeterministicConcurrentIndexedTable(DataSchema dataSchema, boolean hasFinalInput,
      QueryContext queryContext, int resultSize,
      int trimSize, int trimThreshold, int initialCapacity, ExecutorService executorService) {
    super(dataSchema, hasFinalInput, queryContext, resultSize, trimSize, trimThreshold,
        new ConcurrentSkipListMap<>(), executorService);
  }

  /**
   * Thread safe implementation of upsert for inserting {@link Record} into {@link Table}
   */
  @Override
  public boolean upsert(Key key, Record record) {
    if (_hasOrderBy) {
      upsertWithOrderBy(key, record);
    } else {
      upsertWithoutOrderBy(key, record);
    }
    return true;
  }

  protected void upsertWithOrderBy(Key key, Record record) {
    _readWriteLock.readLock().lock();
    try {
      addOrUpdateRecord(key, record);
    } finally {
      _readWriteLock.readLock().unlock();
    }

    if (_lookupMap.size() >= _trimThreshold) {
      _readWriteLock.writeLock().lock();
      try {
        if (_lookupMap.size() >= _trimThreshold) {
          resize();
        }
      } finally {
        _readWriteLock.writeLock().unlock();
      }
    }
  }

  protected void upsertWithoutOrderBy(Key key, Record record) {
    if (_noMoreNewRecords.get()) {
      updateExistingRecord(key, record);
    } else {
      addOrUpdateRecord(key, record);
      if (_lookupMap.size() > _resultSize) {
        ((ConcurrentSkipListMap<Key, Record>) _lookupMap).pollLastEntry();
        _noMoreNewRecords.set(_lookupMap.size() >= _resultSize);
      }
    }
  }
}
