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
package org.apache.pinot.core.data;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.EqualityUtils;


/**
 * Table of data records to pass data between operators
 */
public interface IndexedTable {

  void init(@Nonnull DataSchema dataSchema, List<AggregationInfo> aggregationInfos, List<SelectionSort> orderBy,
      int maxCapacity);

  boolean upsert(@Nonnull TableRecord record);

  boolean merge(@Nonnull IndexedTable table);

  int size();

  Iterator<TableRecord> iterator();

  boolean sort();

  class TableRecord {
    Object[] _keys;
    Object[] _values;

    public TableRecord(Object[] keys, Object[] values) {
      _keys = keys;
      _values = values;
    }

    public Object[] getKeys() {
      return _keys;
    }

    public Object[] getValues() {
      return _values;
    }

    @Override
    public boolean equals(Object o) {
      if (EqualityUtils.isSameReference(this, o)) {
        return true;
      }

      if (EqualityUtils.isNullOrNotSameClass(this, o)) {
        return false;
      }

      TableRecord that = (TableRecord) o;
      return Arrays.deepEquals(_keys, that._keys);
    }

    @Override
    public int hashCode() {
      return Arrays.deepHashCode(_keys);
    }
  }
}
