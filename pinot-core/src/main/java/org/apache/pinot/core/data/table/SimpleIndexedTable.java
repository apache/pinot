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
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.request.context.QueryContext;


/**
 * {@link Table} implementation for aggregating TableRecords based on combination of keys
 */
@SuppressWarnings("unchecked")
@NotThreadSafe
public class SimpleIndexedTable extends IndexedTable {
  private boolean _noMoreNewRecords = false;

  public SimpleIndexedTable(DataSchema dataSchema, QueryContext queryContext, int trimSize, int trimThreshold) {
    super(dataSchema, queryContext, trimSize, trimThreshold, new HashMap<>());
  }

  /**
   * Non thread safe implementation of upsert to insert {@link Record} into the {@link Table}
   */
  @Override
  public boolean upsert(Key key, Record newRecord) {
    Preconditions.checkNotNull(key, "Cannot upsert record with null keys");
    if (_noMoreNewRecords) {
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

      if (_lookupMap.size() >= _trimThreshold) {
        if (_hasOrderBy) {
          // reached max capacity, resize
          resize();
        } else {
          // reached max capacity and no order by. No more new records will be accepted
          _noMoreNewRecords = true;
        }
      }
    }
    return true;
  }
}
