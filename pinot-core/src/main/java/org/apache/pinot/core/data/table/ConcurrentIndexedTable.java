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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.request.context.QueryContext;


/**
 * Thread safe {@link Table} implementation for aggregating Records based on combination of keys
 */
@SuppressWarnings("unchecked")
public class ConcurrentIndexedTable extends IndexedTable {
  private final AtomicBoolean _noMoreNewRecords = new AtomicBoolean();
  private final ReentrantReadWriteLock _readWriteLock = new ReentrantReadWriteLock();

  public ConcurrentIndexedTable(DataSchema dataSchema, QueryContext queryContext, int trimSize, int trimThreshold) {
    super(dataSchema, queryContext, trimSize, trimThreshold, new ConcurrentHashMap<>());
  }

  /**
   * Thread safe implementation of upsert for inserting {@link Record} into {@link Table}
   */
  @Override
  public boolean upsert(Key key, Record newRecord) {
    Preconditions.checkNotNull(key, "Cannot upsert record with null keys");
    if (_noMoreNewRecords.get()) {
      // allow only existing record updates
      _lookupMap.computeIfPresent(key, (k, v) -> {
        Object[] existingValues = v.getValues();
        Object[] newValues = newRecord.getValues();
        int aggNum = 0;
        for (int i = _numKeyColumns; i < _numColumns; i++) {
          existingValues[i] = _aggregationFunctions[aggNum++].merge(existingValues[i], newValues[i]);
        }
        return v;
      });
    } else {
      // allow all records
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
              resize();
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
}
