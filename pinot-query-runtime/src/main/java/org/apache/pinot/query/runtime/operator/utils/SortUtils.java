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
package org.apache.pinot.query.runtime.operator.utils;

import java.util.Comparator;
import java.util.List;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.pinot.common.utils.DataSchema;


public class SortUtils {
  private SortUtils() {
  }

  public static class SortComparator implements Comparator<Object[]> {
    private final int _numFields;
    private final int[] _valueIndices;
    private final int[] _multipliers;
    private final int[] _nullsMultipliers;
    private final boolean[] _useDoubleComparison;

    /**
     * Sort comparator for use with priority queues.
     *
     * @param dataSchema data schema to use
     * @param collations collations to sort on
     * @param reverse 'true' if the opposite sort direction should be used as what is specified
     */
    public SortComparator(DataSchema dataSchema, List<RelFieldCollation> collations, boolean reverse) {
      DataSchema.ColumnDataType[] columnDataTypes = dataSchema.getColumnDataTypes();
      _numFields = collations.size();
      _valueIndices = new int[_numFields];
      _multipliers = new int[_numFields];
      _nullsMultipliers = new int[_numFields];
      _useDoubleComparison = new boolean[_numFields];
      for (int i = 0; i < _numFields; i++) {
        RelFieldCollation collation = collations.get(i);
        _valueIndices[i] = collation.getFieldIndex();
        int multiplier = collation.direction == Direction.ASCENDING ? 1 : -1;
        _multipliers[i] = reverse ? -multiplier : multiplier;
        boolean nullsLast =
            collation.nullDirection == NullDirection.LAST || (collation.nullDirection == NullDirection.UNSPECIFIED
                && collation.direction == Direction.ASCENDING);
        int nullsMultiplier = nullsLast ? 1 : -1;
        _nullsMultipliers[i] = reverse ? -nullsMultiplier : nullsMultiplier;
        _useDoubleComparison[i] = columnDataTypes[_valueIndices[i]].isNumber();
      }
    }

    @Override
    public int compare(Object[] o1, Object[] o2) {
      for (int i = 0; i < _numFields; i++) {
        int index = _valueIndices[i];
        Object v1 = o1[index];
        Object v2 = o2[index];
        if (v1 == null) {
          if (v2 == null) {
            continue;
          }
          return _nullsMultipliers[i];
        }
        if (v2 == null) {
          return -_nullsMultipliers[i];
        }
        int result;
        if (_useDoubleComparison[i]) {
          result = Double.compare(((Number) v1).doubleValue(), ((Number) v2).doubleValue());
        } else {
          //noinspection rawtypes,unchecked
          result = ((Comparable) v1).compareTo(v2);
        }
        if (result != 0) {
          return result * _multipliers[i];
        }
      }
      return 0;
    }
  }
}
