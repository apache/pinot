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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Helper class for trimming and sorting records in the IndexedTable
 */
class RandomIndexedTableResizer extends IndexedTableResizer {

  /**
   * Resize recordsMap to trimToSize randomly
   */
  @Override
  void resizeRecordsMap(Map<Key, Record> recordsMap, int trimToSize) {

    int numRecordsToDrop = recordsMap.size() - trimToSize;
    if (numRecordsToDrop > 0) {
      Set<Key> evictKeys = new HashSet<>(numRecordsToDrop);
      Iterator<Key> iterator = recordsMap.keySet().iterator();
      while (numRecordsToDrop > 0) {
        evictKeys.add(iterator.next());
        numRecordsToDrop --;
      }
      recordsMap.keySet().removeAll(evictKeys);
    }
  }

  /**
   * Call to sort is a no-op in this case
   */
  @Override
  List<Record> sortRecordsMap(Map<Key, Record> recordsMap) {
    if (recordsMap.size() == 0) {
      return Collections.emptyList();
    }
    return new ArrayList<>(recordsMap.values());
  }
}
