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

  public UnboundedConcurrentIndexedTable(DataSchema dataSchema, boolean hasFinalInput, QueryContext queryContext,
      int resultSize, int initialCapacity, ExecutorService executorService) {
    super(dataSchema, hasFinalInput, queryContext, resultSize, Integer.MAX_VALUE, Integer.MAX_VALUE, initialCapacity,
        executorService);
  }

  @Override
  protected void upsertWithOrderBy(Key key, Record record) {
    addOrUpdateRecord(key, record);
  }
}
