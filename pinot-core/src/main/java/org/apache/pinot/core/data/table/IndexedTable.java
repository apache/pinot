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

import java.util.Arrays;
import java.util.List;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.utils.DataSchema;


/**
 * Base implementation of Map-based Table for indexed lookup
 */
public abstract class IndexedTable extends BaseTable {

  private KeyExtractor _keyExtractor;
  int _numKeyColumns;

  /**
   * Initializes the variables and comparators needed for the table
   */
  IndexedTable(DataSchema dataSchema, List<AggregationInfo> aggregationInfos, List<SelectionSort> orderBy, int capacity) {
    super(dataSchema, aggregationInfos, orderBy, capacity);

    _numKeyColumns = dataSchema.size() - _numAggregations;
    _keyExtractor = new KeyExtractor(_numKeyColumns);
  }

  @Override
  public boolean upsert(Record newRecord) {
    Key key = _keyExtractor.extractKey(newRecord);
    return upsert(key, newRecord);
  }

  /**
   * Extractor for key component of a Record
   * The assumption is that the keys will always be before the aggregations in the Record. It is the caller's responsibility to ensure that.
   * This will help us avoid index lookups, while extracting keys, and also while aggregating the values
   */
  private static class KeyExtractor {
    private int _keyIndexes;

    KeyExtractor(int keyIndexes) {
      _keyIndexes = keyIndexes;
    }

    /**
     * Returns the Key from the Record
     */
    Key extractKey(Record record) {
      Object[] keys = Arrays.copyOf(record.getColumns(), _keyIndexes);
      return new Key(keys);
    }
  }
}
