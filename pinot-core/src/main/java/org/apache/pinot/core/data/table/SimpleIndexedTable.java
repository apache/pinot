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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.data.order.OrderByUtils;


/**
 * {@link Table} implementation for aggregating TableRecords based on combination of keys
 */
@NotThreadSafe
public class SimpleIndexedTable extends IndexedTable {

  private List<Record> _records;
  private Map<Record, Integer> _lookupTable;

  @Override
  public void init(@Nonnull DataSchema dataSchema, List<AggregationInfo> aggregationInfos, List<SelectionSort> orderBy,
      int maxCapacity) {
    super.init(dataSchema, aggregationInfos, orderBy, maxCapacity);

    _records = new ArrayList<>(maxCapacity);
    _lookupTable = new HashMap<>(maxCapacity);
  }

  /**
   * Non thread safe implementation of upsert to insert {@link Record} into the {@link Table}
   */
  @Override
  public boolean upsert(@Nonnull Record newRecord) {
    Object[] keys = newRecord.getKeys();
    Preconditions.checkNotNull(keys, "Cannot upsert record with null keys");

    Integer index = _lookupTable.get(newRecord);
    if (index == null) {
      if (size() >= _bufferedCapacity) {
        resize(_evictCapacity);
      }
      index = size();
      _lookupTable.put(newRecord, index);
      _records.add(index, newRecord);
    } else {
      Record existingRecord = _records.get(index);
      aggregate(existingRecord, newRecord);
    }
    return true;
  }

  private void resize(int trimToSize) {
    // sort
    if (CollectionUtils.isNotEmpty(_orderBy)) {
      Comparator<Record> comparator;
      comparator = OrderByUtils.getKeysAndValuesComparator(_dataSchema, _orderBy, _aggregationInfos);
      _records.sort(comparator);
    }

    // evict lowest
    if (_records.size() > trimToSize) {
      _records = new ArrayList<>(_records.subList(0, trimToSize));
    }

    // rebuild lookup table
    _lookupTable.clear();
    for (int i = 0; i < _records.size(); i++) {
      _lookupTable.put(_records.get(i), i);
    }
  }


  @Override
  public boolean merge(@Nonnull Table table) {
    Iterator<Record> iterator = table.iterator();
    while (iterator.hasNext()) {
      upsert(iterator.next());
    }
    return true;
  }

  @Override
  public int size() {
    return _records.size();
  }

  @Override
  public Iterator<Record> iterator() {
    return _records.iterator();
  }

  @Override
  public void finish() {
    resize(_maxCapacity);
  }
}
