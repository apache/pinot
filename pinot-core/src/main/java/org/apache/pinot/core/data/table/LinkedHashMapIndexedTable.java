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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.request.context.QueryContext;


public class LinkedHashMapIndexedTable extends IndexedTable {
  private int _numMergedBlocks;
  private final int _desiredNumMergedBlocks;

  /**
   * Constructor for the IndexedTable.
   *
   * @param dataSchema      Data schema of the table
   * @param hasFinalInput   Whether the input is the final aggregate result
   * @param queryContext    Query context
   * @param resultSize      Number of records to keep in the final result after calling
   *                        {@link #finish(boolean, boolean)}
   * @param trimSize        Number of records to keep when trimming the table
   * @param trimThreshold   Trim the table when the number of records exceeds the threshold
   * @param executorService
   */
  public LinkedHashMapIndexedTable(DataSchema dataSchema, boolean hasFinalInput,
      QueryContext queryContext, int resultSize, int trimSize,
      int trimThreshold, ExecutorService executorService, int numMergedBlocks, int desiredNumMergedBlocks) {
    super(dataSchema, hasFinalInput, queryContext, resultSize, trimSize, trimThreshold, new LinkedHashMap<>(),
        executorService);
    _desiredNumMergedBlocks = desiredNumMergedBlocks;
    _numMergedBlocks = numMergedBlocks;
  }

  public boolean isSatisfied() {
    return _numMergedBlocks == _desiredNumMergedBlocks;
  }

  ///  merge another sorted LinkedHashMapIndexedTable into this
  public LinkedHashMapIndexedTable merge(LinkedHashMapIndexedTable other, Comparator<Key> comparator,
      QueryContext queryContext, ExecutorService executorService) {
    assert (other._resultSize == _resultSize);
    int limit = _resultSize;
    LinkedHashMap<Key, Record> thisMap = (LinkedHashMap<Key, Record>) _lookupMap;
    LinkedHashMap<Key, Record> thatMap = (LinkedHashMap<Key, Record>) other._lookupMap;
    if (thisMap.isEmpty()) {
      other._numMergedBlocks += _numMergedBlocks;
      return other;
    }
    if (thatMap.isEmpty()) {
      _numMergedBlocks += other._numMergedBlocks;
      return this;
    }
    assert (_numMergedBlocks >= 1);
    assert (other._numMergedBlocks >= 1);
    LinkedHashMapIndexedTable newTable =
        new LinkedHashMapIndexedTable(getDataSchema(), _hasFinalInput, queryContext, _resultSize, _trimSize,
            _trimThreshold, executorService, _numMergedBlocks + other._numMergedBlocks, _desiredNumMergedBlocks);
    newTable = mergeSortedMaps(thisMap, thatMap, comparator, limit, newTable);
    // TODO: add stats?
    return newTable;
  }

  public LinkedHashMapIndexedTable mergeSortedMaps(
      LinkedHashMap<Key, Record> map1, LinkedHashMap<Key, Record> map2,
      Comparator<Key> comparator, int limit, LinkedHashMapIndexedTable newTable) {
    assert (comparator != null);

    Iterator<Map.Entry<Key, Record>> iter1 = map1.entrySet().iterator();
    Iterator<Map.Entry<Key, Record>> iter2 = map2.entrySet().iterator();

    Map.Entry<Key, Record> entry1 = iter1.hasNext() ? iter1.next() : null;
    Map.Entry<Key, Record> entry2 = iter2.hasNext() ? iter2.next() : null;

    while (entry1 != null && entry2 != null) {
      int cmp = comparator.compare(entry1.getKey(), entry2.getKey());
      if (cmp < 0) {
        newTable.upsert(entry1.getKey(), entry1.getValue());
        entry1 = iter1.hasNext() ? iter1.next() : null;
      } else if (cmp == 0) {
        // merge results using aggregation function, this might update entry1.value in place
        newTable.upsert(entry1.getKey(), entry1.getValue());
        newTable.upsert(entry2.getKey(), entry2.getValue());
        entry1 = iter1.hasNext() ? iter1.next() : null;
        entry2 = iter2.hasNext() ? iter2.next() : null;
      } else {
        newTable.upsert(entry2.getKey(), entry2.getValue());
        entry2 = iter2.hasNext() ? iter2.next() : null;
      }
      if (newTable.size() == limit) {
        return newTable;
      }
    }

    while (entry1 != null) {
      newTable.upsert(entry1.getKey(), entry1.getValue());
      entry1 = iter1.hasNext() ? iter1.next() : null;
      if (newTable.size() == limit) {
        return newTable;
      }
    }

    while (entry2 != null) {
      newTable.upsert(entry2.getKey(), entry2.getValue());
      entry2 = iter2.hasNext() ? iter2.next() : null;
      if (newTable.size() == limit) {
        return newTable;
      }
    }

    return newTable;
  }

  ///  insert in desired order
  @Override
  public boolean upsert(Key key, Record record) {
    if (_lookupMap.size() < _resultSize) {
      addOrUpdateRecord(key, record);
    } else {
      updateExistingRecord(key, record);
    }
    return true;
  }

  @Override
  public void finish(boolean sort) {
    // don't sort since this is already sorted
    super.finish(false);
  }
}
