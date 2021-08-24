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

import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.request.context.QueryContext;


/**
 * Another version of {@link ConcurrentIndexedTable} used for use cases
 * configured to have infinite group by (num group limit to se 1B or higher).
 * For such cases, there won't be any resizing/trimming during upsert since
 * trimThreshold is very high. Thus, we can avoid the overhead of readLock's
 * lock and unlock operations which are otherwise used in ConcurrentIndexedTable
 * to prevent race conditions between 2 threads doing upsert and one of them ends
 * up trimming from upsert. For use cases with very large number of groups, we had
 * noticed that load-unlock overhead was > 1sec and this specialized concurrent
 * indexed table avoids that by overriding just the upsert method
 */
public class UnboundedConcurrentIndexedTable extends ConcurrentIndexedTable {

  public UnboundedConcurrentIndexedTable(DataSchema dataSchema, QueryContext queryContext, int trimSize,
      int trimThreshold) {
    super(dataSchema, queryContext, trimSize, trimThreshold);
  }

  @Override
  public boolean upsert(Key key, Record newRecord) {
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

      if (_lookupMap.size() >= _trimSize && !_hasOrderBy) {
        // reached capacity and no order by. No more new records will be accepted
        _noMoreNewRecords.set(true);
      }
    }
    return true;
  }
}
