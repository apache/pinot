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
package org.apache.pinot.core.data.order;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.comparators.ComparableComparator;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.utils.BytesUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.primitive.ByteArray;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;

import static org.apache.pinot.common.utils.DataSchema.*;


/**
 * Helper methods to perform order by of list of {@link Record}
 */
public final class OrderByUtils {

  private OrderByUtils() {
  }

  /**
   * Constructs a comparator for ordering by the keys in the TableRecord::keys
   */
  public static Comparator<Record> getKeysComparator(DataSchema dataSchema, List<SelectionSort> orderBy) {

    Map<String, Integer> columnIndexMap = new HashMap<>();
    Map<String, ColumnDataType> columnDataTypeMap = new HashMap<>();
    for (int i = 0; i < dataSchema.size(); i++) {
      columnIndexMap.put(dataSchema.getColumnName(i), i);
      columnDataTypeMap.put(dataSchema.getColumnName(i), dataSchema.getColumnDataType(i));
    }

    Comparator<Record> globalComparator = null;

    for (SelectionSort orderByInfo : orderBy) {
      String column = orderByInfo.getColumn();
      boolean ascending = orderByInfo.isIsAsc();
      int index = columnIndexMap.get(column);
      ColumnDataType columnDataType = columnDataTypeMap.get(column);

      Comparator<Record> comparator = getKeysComparator(ascending, index, columnDataType);
      if (globalComparator == null) {
        globalComparator = comparator;
      } else {
        globalComparator = globalComparator.thenComparing(comparator);
      }
    }
    return globalComparator;
  }

  /**
   * Constructs a comparator for ordering by the non-aggregation values in the Record::values
   */
  public static Comparator<Record> getValuesComparator(DataSchema dataSchema, List<SelectionSort> orderBy) {

    Map<String, Integer> columnIndexMap = new HashMap<>();
    Map<String, ColumnDataType> columnDataTypeMap = new HashMap<>();
    for (int i = 0; i < dataSchema.size(); i++) {
      columnIndexMap.put(dataSchema.getColumnName(i), i);
      columnDataTypeMap.put(dataSchema.getColumnName(i), dataSchema.getColumnDataType(i));
    }

    Comparator<Record> globalComparator = null;

    for (SelectionSort orderByInfo : orderBy) {
      String column = orderByInfo.getColumn();
      boolean ascending = orderByInfo.isIsAsc();
      int index = columnIndexMap.get(column);
      ColumnDataType columnDataType = columnDataTypeMap.get(column);

      Comparator<Record> comparator = getValuesComparator(ascending, index, columnDataType);
      if (globalComparator == null) {
        globalComparator = comparator;
      } else {
        globalComparator = globalComparator.thenComparing(comparator);
      }
    }
    return globalComparator;
  }

  /**
   * Constructs the comparator for ordering by a combination of keys from {@link Record::_keys}
   * and aggregation values from {@link Record::values}
   */
  public static Comparator<Record> getKeysAndValuesComparator(DataSchema dataSchema, List<SelectionSort> orderBy,
      List<AggregationInfo> aggregationInfos) {

    int numKeys = dataSchema.size() - aggregationInfos.size();
    Map<String, Integer> keyIndexMap = new HashMap<>();
    Map<String, ColumnDataType> keyColumnDataTypeMap = new HashMap<>();
    for (int i = 0; i < numKeys; i++) {
      keyIndexMap.put(dataSchema.getColumnName(i), i);
      keyColumnDataTypeMap.put(dataSchema.getColumnName(i), dataSchema.getColumnDataType(i));
    }

    Map<String, Integer> aggregationColumnToIndex = new HashMap<>(aggregationInfos.size());
    Map<String, AggregationInfo> aggregationColumnToInfo = new HashMap<>(aggregationInfos.size());
    for (int i = 0; i < aggregationInfos.size(); i++) {
      AggregationInfo aggregationInfo = aggregationInfos.get(i);
      String aggregationColumn =
          aggregationInfo.getAggregationType().toLowerCase() + "(" + AggregationFunctionUtils.getColumn(aggregationInfo)
              + ")";
      aggregationColumnToIndex.put(aggregationColumn, i);
      aggregationColumnToInfo.put(aggregationColumn, aggregationInfo);
    }

    Comparator<Record> globalComparator = null;

    for (SelectionSort orderByInfo : orderBy) {
      Comparator<Record> comparator;

      String column = orderByInfo.getColumn();
      boolean ascending = orderByInfo.isIsAsc();

      if (keyIndexMap.containsKey(column)) {
        int index = keyIndexMap.get(column);
        ColumnDataType columnDataType = keyColumnDataTypeMap.get(column);
        comparator = OrderByUtils.getKeysComparator(ascending, index, columnDataType);
      } else if (aggregationColumnToIndex.containsKey(column)) {
        int index = aggregationColumnToIndex.get(column);
        AggregationFunction aggregationFunction =
            AggregationFunctionUtils.getAggregationFunctionContext(aggregationColumnToInfo.get(column))
                .getAggregationFunction();
        comparator = getAggregationComparator(ascending, index, aggregationFunction);
      } else {
        throw new UnsupportedOperationException(
            "Currently only support order by on group by columns or aggregations, already in query");
      }

      if (globalComparator == null) {
        globalComparator = comparator;
      } else {
        globalComparator = globalComparator.thenComparing(comparator);
      }
    }
    return globalComparator;
  }

  private static Comparator<Record> getKeysComparator(boolean ascending, int index, ColumnDataType columnDataType) {
    Comparator<Record> comparator;
    switch (columnDataType) {
      case INT:
        if (ascending) {
          comparator = Comparator.comparingInt(o -> (Integer) o.getKey().getColumns()[index]);
        } else {
          comparator = (o1, o2) -> Integer.compare((Integer) o2.getKey().getColumns()[index], (Integer) o1.getKey().getColumns()[index]);
        }
        break;
      case LONG:
        if (ascending) {
          comparator = Comparator.comparingLong(o -> (Long) o.getKey().getColumns()[index]);
        } else {
          comparator = (o1, o2) -> Long.compare((Long) o2.getKey().getColumns()[index], (Long) o1.getKey().getColumns()[index]);
        }
        break;
      case FLOAT:
        if (ascending) {
          comparator = (o1, o2) -> Float.compare((Float) o1.getKey().getColumns()[index], (Float) o2.getKey().getColumns()[index]);
        } else {
          comparator = (o1, o2) -> Float.compare((Float) o2.getKey().getColumns()[index], (Float) o1.getKey().getColumns()[index]);
        }
        break;
      case DOUBLE:
        if (ascending) {
          comparator = Comparator.comparingDouble(o -> (Double) o.getKey().getColumns()[index]);
        } else {
          comparator = (o1, o2) -> Double.compare((Double) o2.getKey().getColumns()[index], (Double) o1.getKey().getColumns()[index]);
        }
        break;
      case BYTES:
        if (ascending) {
          comparator = (o1, o2) -> ByteArray.compare(BytesUtils.toBytes(o1.getKey().getColumns()[index]),
              BytesUtils.toBytes(o2.getKey().getColumns()[index]));
        } else {
          comparator = (o1, o2) -> ByteArray.compare(BytesUtils.toBytes(o2.getKey().getColumns()[index]),
              BytesUtils.toBytes(o1.getKey().getColumns()[index]));
        }
        break;
      case STRING:
      default:
        if (ascending) {
          comparator = Comparator.comparing(o -> (String) o.getKey().getColumns()[index]);
        } else {
          comparator = (o1, o2) -> ((String) o2.getKey().getColumns()[index]).compareTo((String) o1.getKey().getColumns()[index]);
        }
        break;
    }
    return comparator;
  }

  private static Comparator<Record> getValuesComparator(boolean ascending, int index, ColumnDataType columnDataType) {
    Comparator<Record> comparator;
    switch (columnDataType) {
      case INT:
        if (ascending) {
          comparator = Comparator.comparingInt(o -> (Integer) o.getValues()[index]);
        } else {
          comparator = (o1, o2) -> Integer.compare((Integer) o2.getValues()[index], (Integer) o1.getValues()[index]);
        }
        break;
      case LONG:
        if (ascending) {
          comparator = Comparator.comparingLong(o -> (Long) o.getValues()[index]);
        } else {
          comparator = (o1, o2) -> Long.compare((Long) o2.getValues()[index], (Long) o1.getValues()[index]);
        }
        break;
      case FLOAT:
        if (ascending) {
          comparator = (o1, o2) -> Float.compare((Float) o1.getValues()[index], (Float) o2.getValues()[index]);
        } else {
          comparator = (o1, o2) -> Float.compare((Float) o2.getValues()[index], (Float) o1.getValues()[index]);
        }
        break;
      case DOUBLE:
        if (ascending) {
          comparator = Comparator.comparingDouble(o -> (Double) o.getValues()[index]);
        } else {
          comparator = (o1, o2) -> Double.compare((Double) o2.getValues()[index], (Double) o1.getValues()[index]);
        }
        break;
      case BYTES:
        if (ascending) {
          comparator = (o1, o2) -> ByteArray.compare(BytesUtils.toBytes(o1.getValues()[index]),
              BytesUtils.toBytes(o2.getValues()[index]));
        } else {
          comparator = (o1, o2) -> ByteArray.compare(BytesUtils.toBytes(o2.getValues()[index]),
              BytesUtils.toBytes(o1.getValues()[index]));
        }
        break;
      case STRING:
      default:
        if (ascending) {
          comparator = Comparator.comparing(o -> (String) o.getValues()[index]);
        } else {
          comparator = (o1, o2) -> ((String) o2.getValues()[index]).compareTo((String) o1.getValues()[index]);
        }
        break;
    }
    return comparator;
  }

  private static Comparator<Record> getAggregationComparator(boolean ascending, int index,
      AggregationFunction aggregationFunction) {

    Comparator<Record> comparator;

    if (ascending) {
      comparator = new Comparator<Record>() {
        @Override
        public int compare(Record v1, Record v2) {

          return ComparableComparator.getInstance()
              .compare(aggregationFunction.extractFinalResult(v1.getValues()[index]),
                  aggregationFunction.extractFinalResult(v2.getValues()[index]));
        }
      };
    } else {
      comparator = (v1, v2) -> ComparableComparator.getInstance()
          .compare(aggregationFunction.extractFinalResult(v2.getValues()[index]),
              aggregationFunction.extractFinalResult(v1.getValues()[index]));
    }
    return comparator;
  }
}
