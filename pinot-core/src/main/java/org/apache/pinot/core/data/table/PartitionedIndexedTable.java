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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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
  private PartitionedIndexedTable(DataSchema dataSchema, boolean hasFinalInput, QueryContext queryContext,
      int resultSize, RadixPartitionedHashMap<Key, Record> map, ExecutorService executorService) {
    super(dataSchema, hasFinalInput, queryContext, resultSize, Integer.MAX_VALUE, Integer.MAX_VALUE, map,
        executorService);
  }

  public static PartitionedIndexedTable create(DataSchema dataSchema, boolean hasFinalInput, QueryContext queryContext,
      int resultSize, RadixPartitionedHashMap<Key, Record> map, ExecutorService executorService) {
    // trim map to resultSize entries
    boolean enough = false;
    int needed = resultSize;
    for (int i = 0; i < map.getNumPartitions(); i++) {
      Map<Key, Record> m = map.getPartition(i);
      int size = m.size();
      if (enough) {
        m.clear();
        continue;
      }
      if (size >= needed) {
        // clear maps from this on
        enough = true;
        Iterator<Key> it = m.keySet().iterator();
        List<Key> keyToRemove = new ArrayList<>();
        for (int j = 0; j < size - needed; j++) {
          keyToRemove.add(it.next());
        }
        for (Key key : keyToRemove) {
          m.remove(key);
        }
      } else {
        needed -= m.size();
      }
    }
    return new PartitionedIndexedTable(dataSchema, hasFinalInput, queryContext, resultSize, map, executorService);
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
