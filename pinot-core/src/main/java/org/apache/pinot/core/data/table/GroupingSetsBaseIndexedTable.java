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

import java.util.concurrent.ExecutorService;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.request.context.QueryContext;


/// Combine table for BASE grouping-set records. The instance planner proves that all local base keys fit the
/// group limit before selecting this path. If that bound is ever violated, fail the query instead of retaining
/// unbounded aggregation intermediates or returning incomplete totals.
public class GroupingSetsBaseIndexedTable extends UnboundedConcurrentIndexedTable {
  private volatile boolean _full;

  public GroupingSetsBaseIndexedTable(DataSchema dataSchema, QueryContext queryContext, int resultSize,
      int initialCapacity, ExecutorService executorService) {
    super(dataSchema, false, queryContext, resultSize, initialCapacity, executorService);
  }

  @Override
  protected void upsertWithoutOrderBy(Key key, Record record) {
    if (_full) {
      if (!updateExistingRecordIfPresent(key, record)) {
        throw new IllegalStateException("Grouping-set base groups exceeded the planned group limit");
      }
    } else {
      addOrUpdateRecord(key, record);
      if (_resultSize != Integer.MAX_VALUE && _lookupMap.size() >= _resultSize) {
        _full = true;
      }
    }
  }
}
