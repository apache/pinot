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

import java.util.Map;
import java.util.concurrent.ExecutorService;
import javax.ws.rs.NotSupportedException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.request.context.QueryContext;


/**
 * IndexedTable wrapper for RadixPartitionedHashMap,
 * used for and stitching hash tables together in phase 2
 */
public class PartitionedIndexedTable extends IndexedTable {
  public PartitionedIndexedTable(DataSchema dataSchema, boolean hasFinalInput, QueryContext queryContext,
      int resultSize, int trimSize, int trimThreshold, RadixPartitionedHashMap<Key, Record> map, ExecutorService executorService) {
    super(dataSchema, hasFinalInput, queryContext, resultSize, trimSize, trimThreshold, map,
        executorService);
  }

  public Map<Key, Record> getPartition(int i) {
    RadixPartitionedHashMap<Key, Record> map = (RadixPartitionedHashMap<Key, Record>) _lookupMap;
    return map.getPartition(i);
  }

  @Override
  public boolean upsert(Key key, Record record) {
    throw new NotSupportedException("should not finish on PartitionedIndexedTable");
  }
}
