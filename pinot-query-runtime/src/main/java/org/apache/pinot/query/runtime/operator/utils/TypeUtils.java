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

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;


public class TypeUtils {
  private TypeUtils() {
  }

  /**
   * Converts value to the desired stored {@link ColumnDataType}. This is used to convert rows generated from
   * single-stage engine to be used in multi-stage engine.
   * TODO: Revisit to see if we should use original type instead of stored type
   */
  public static Object convert(Object value, ColumnDataType storedType) {
    switch (storedType) {
      case INT:
        return ((Number) value).intValue();
      case LONG:
        return ((Number) value).longValue();
      case FLOAT:
        return ((Number) value).floatValue();
      case DOUBLE:
        return ((Number) value).doubleValue();
      // For AggregationFunctions that return serialized custom object, e.g. DistinctCountRawHLLAggregationFunction
      case STRING:
        return value.toString();
      case LONG_ARRAY:
        if (value instanceof LongArrayList) {
          // For FunnelCountAggregationFunction
          return ((LongArrayList) value).elements();
        } else {
          return value;
        }
      case DOUBLE_ARRAY:
        if (value instanceof DoubleArrayList) {
          // For HistogramAggregationFunction
          return ((DoubleArrayList) value).elements();
        } else {
          return value;
        }
      // TODO: Add more conversions
      default:
        return value;
    }
  }

  /**
   * Converts row to the desired stored {@link ColumnDataType}s in-place. This is used to convert rows generated from
   * single-stage engine to be used in multi-stage engine.
   */
  public static void convertRow(Object[] row, ColumnDataType[] outputStoredTypes) {
    int numColumns = row.length;
    for (int colId = 0; colId < numColumns; colId++) {
      Object value = row[colId];
      if (value != null) {
        row[colId] = convert(value, outputStoredTypes[colId]);
      }
    }
  }
}
