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
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.request.context.QueryContext;


/**
 * {@link Table} implementations for orderby group key case, sorted on group keys and only `trimSize` groups are kept
 */
@NotThreadSafe
public class SimpleSortedIndexedTable extends IndexedTable {
  Comparator<Key> _keyComparator;

  public SimpleSortedIndexedTable(DataSchema dataSchema, boolean hasFinalInput,
      QueryContext queryContext, int resultSize, ExecutorService executorService, Comparator<Key> keyComparator) {
    super(dataSchema, hasFinalInput, queryContext, resultSize, resultSize, Integer.MAX_VALUE,
        new TreeMap<>(keyComparator),
        executorService);
    _keyComparator = keyComparator;
  }

  @Override
  public boolean upsert(Key key, Record record) {
    TreeMap<Key, Record> map = (TreeMap<Key, Record>) _lookupMap;
    if (map.size() < _resultSize) {
      addOrUpdateRecord(key, record);
      return true;
    }
    // if size is full, evict the smallest key
    Key lastKey = map.lastKey();
    if (lastKey == null || _keyComparator.compare(key, lastKey) < 0) {
      // check lastKey should not be null unless _resultSize = 0;
      assert (lastKey != null || _resultSize == 0);
      return true;
    }
    addOrUpdateRecord(key, record);
    if (map.size() > _resultSize) {
      map.remove(map.lastKey());
    }
    return true;
  }

  @Override
  public void finish(boolean sort) {
    // don't sort again since the map itself is sorted
    super.finish(false);
  }
}
