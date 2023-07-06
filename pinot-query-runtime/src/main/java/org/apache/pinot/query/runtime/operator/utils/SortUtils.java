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
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;


public class SortUtils {
  private SortUtils() {
  }

  public static class SortComparator implements Comparator<Object[]> {
    private final int _size;
    private final int[] _valueIndices;
    private final int[] _multipliers;
    private final int[] _nullsMultipliers;
    private final boolean[] _useDoubleComparison;

    /**
     * Sort comparator for use with priority queues.
     *
     * @param collationKeys collation keys to sort on
     * @param collationDirections collation direction for each collation key to sort on
     * @param collationNullDirections collation direction for NULL values in each collation key to sort on
     * @param dataSchema data schema to use
     * @param switchDirections 'true' if the opposite sort direction should be used as what is specified
     */
    public SortComparator(List<RexExpression> collationKeys, List<Direction> collationDirections,
        List<NullDirection> collationNullDirections, DataSchema dataSchema, boolean switchDirections) {
      DataSchema.ColumnDataType[] columnDataTypes = dataSchema.getColumnDataTypes();
      _size = collationKeys.size();
      _valueIndices = new int[_size];
      _multipliers = new int[_size];
      _nullsMultipliers = new int[_size];
      _useDoubleComparison = new boolean[_size];
      for (int i = 0; i < _size; i++) {
        _valueIndices[i] = ((RexExpression.InputRef) collationKeys.get(i)).getIndex();
        int multiplier = collationDirections.get(i) == Direction.ASCENDING ? 1 : -1;
        _multipliers[i] = switchDirections ? -multiplier : multiplier;
        int nullsMultiplier = collationNullDirections.get(i) == NullDirection.LAST ? 1 : -1;
        _nullsMultipliers[i] = switchDirections ? -nullsMultiplier : nullsMultiplier;
        _useDoubleComparison[i] = columnDataTypes[_valueIndices[i]].isNumber();
      }
    }

    @Override
    public int compare(Object[] o1, Object[] o2) {
      for (int i = 0; i < _size; i++) {
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
          //noinspection unchecked
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
