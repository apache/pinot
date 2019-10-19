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

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import org.apache.commons.collections.comparators.ComparableComparator;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;


/**
 * Helper class for trimming and sorting records in the IndexedTable, based on the order by information
 */
class OrderedIndexedTableResizer extends IndexedTableResizer {

  private List<OrderByInfo> _orderByInfos;
  private Comparator<IntermediateRecord> _intermediateRecordComparator;
  private int _numOrderBy;

  OrderedIndexedTableResizer(DataSchema dataSchema, List<AggregationInfo> aggregationInfos,
      List<SelectionSort> orderBy) {

    int numAggregations = aggregationInfos.size();
    int numKeyColumns = dataSchema.size() - numAggregations;

    Map<String, Integer> keyIndexMap = new HashMap<>();
    for (int i = 0; i < numKeyColumns; i++) {
      keyIndexMap.put(dataSchema.getColumnName(i), i);
    }

    Map<String, Integer> aggregationColumnToIndex = new HashMap<>();
    Map<String, AggregationInfo> aggregationColumnToInfo = new HashMap<>(aggregationInfos.size());
    for (int i = 0; i < numAggregations; i++) {
      AggregationInfo aggregationInfo = aggregationInfos.get(i);
      String aggregationColumn = AggregationFunctionUtils.getAggregationColumnName(aggregationInfo);
      aggregationColumnToIndex.put(aggregationColumn, i);
      aggregationColumnToInfo.put(aggregationColumn, aggregationInfo);
    }

    _numOrderBy = orderBy.size();
    _orderByInfos = new ArrayList<>(_numOrderBy);
    Comparator[] comparators = new Comparator[_numOrderBy];

    for (int i = 0; i < _numOrderBy; i++) {
      SelectionSort selectionSort = orderBy.get(i);
      String column = selectionSort.getColumn();

      if (keyIndexMap.containsKey(column)) {
        int index = keyIndexMap.get(column);
        _orderByInfos.add(createOrderByInfoForKeyColumn(index));
      } else if (aggregationColumnToIndex.containsKey(column)) {
        int index = aggregationColumnToIndex.get(column);
        AggregationFunction aggregationFunction =
            AggregationFunctionUtils.getAggregationFunctionContext(aggregationColumnToInfo.get(column))
                .getAggregationFunction();
        _orderByInfos.add(createOrderByInfoForAggregationColumn(index, aggregationFunction));
      } else {
        throw new UnsupportedOperationException("Could not find column " + column + " in data schema");
      }

      comparators[i] = ComparableComparator.getInstance();
      if (!selectionSort.isIsAsc()) {
        comparators[i] = comparators[i].reversed();
      }
    }

    _intermediateRecordComparator = (o1, o2) -> {

      for (int i = 0; i < _numOrderBy; i++) {
        int result = comparators[i].compare(o1.getValues()[i], o2.getValues()[i]);
        if (result != 0) {
          return result;
        }
      }
      return 0;
    };
  }

  /**
   * Constructs an IntermediateRecord from Record
   * The IntermediateRecord::key is the same Record::key
   * The IntermediateRecord::values contains only the order by columns, in the query's sort sequence
   * For aggregation values in the order by, the final result is extracted if the intermediate result is non-comparable
   */
  @VisibleForTesting
  IntermediateRecord getIntermediateRecord(Record record) {
    Comparable[] intermediateRecordValues = new Comparable[_numOrderBy];

    Object[] keyColumns = record.getKey().getColumns();
    Object[] aggregationColumns = record.getValues();
    for (int i = 0; i < _numOrderBy; i++) {
      OrderByInfo orderByInfo = _orderByInfos.get(i);
      Comparable comparable;
      if (orderByInfo.getOrderByType().equals(OrderByType.KEY_COLUMN)) {
        Object keyColumn = keyColumns[orderByInfo.getIndex()];
        comparable = (Comparable) keyColumn; // FIXME: is this the right way to get Comparable? will it work for BYTES?
      } else {
        Object aggregationColumn = aggregationColumns[orderByInfo.getIndex()];
        AggregationFunction aggregationFunction = orderByInfo.getAggregationFunction();
        if (!aggregationFunction.isIntermediateResultComparable()) {
          comparable = aggregationFunction.extractFinalResult(aggregationColumn);
        } else {
          comparable = (Comparable) aggregationColumn;
        }
      }
      intermediateRecordValues[i] = comparable;
    }
    return new IntermediateRecord(record.getKey(), intermediateRecordValues);
  }

  /**
   * Trim recordsMap to trimToSize, based on order by information
   */
  @Override
  void resizeRecordsMap(Map<Key, Record> recordsMap, int trimToSize) {

    // make min heap of elements to evict
    int heapSize = recordsMap.size() - trimToSize;
    PriorityQueue<IntermediateRecord> minHeap = new PriorityQueue<>(heapSize, _intermediateRecordComparator);

    for (Record record : recordsMap.values()) {

      IntermediateRecord intermediateRecord = getIntermediateRecord(record);
      if (minHeap.size() < heapSize) {
        minHeap.offer(intermediateRecord);
      } else {
        IntermediateRecord peek = minHeap.peek();
        if (minHeap.comparator().compare(peek, intermediateRecord) < 0) {
          minHeap.poll();
          minHeap.offer(intermediateRecord);
        }
      }
    }

    for (IntermediateRecord evictRecord : minHeap) {
      recordsMap.remove(evictRecord.getKey());
    }
  }

  /**
   * Sort the records in the recordsMap according to order by
   */
  @Override
  List<Record> sortRecordsMap(Map<Key, Record> recordsMap) {
    int numRecords = recordsMap.size();
    List<Record> sortedRecords = new ArrayList<>(numRecords);
    List<IntermediateRecord> intermediateRecords = new ArrayList<>(numRecords);
    for (Record record : recordsMap.values()) {
      intermediateRecords.add(getIntermediateRecord(record));
    }
    intermediateRecords.sort(_intermediateRecordComparator);
    for (IntermediateRecord intermediateRecord : intermediateRecords) {
      sortedRecords.add(recordsMap.get(intermediateRecord.getKey()));
    }
    return sortedRecords;
  }

  /**
   * Enum to differentiate between different types of order by
   * Order by can be on the key column (group by values or distinct), or on the aggregation column
   */
  private enum OrderByType {
    KEY_COLUMN, // order by on a key column (e.g. group by or distinct)
    AGGREGATION_COLUMN // order by on aggregation (e.g. aggregations in aggregation only or group by)
  }

  /**
   * Defines the information needed for one SelectionSort of an order by clause
   */
  private class OrderByInfo {
    private OrderByType _orderByType;
    private int _index; // if orderByType = KEY_COLUMN, index in the Record::key, else index in the Record::value
    private AggregationFunction _aggregationFunction; // required if orderByType = AGGREGATION_COLUMN

    private OrderByInfo(OrderByType orderByType, int index, AggregationFunction aggregationFunction) {
      _orderByType = orderByType;
      _index = index;
      _aggregationFunction = aggregationFunction;
    }

    private OrderByInfo(OrderByType orderByType, int index) {
      _orderByType = orderByType;
      _index = index;
    }

    OrderByType getOrderByType() {
      return _orderByType;
    }

    int getIndex() {
      return _index;
    }

    AggregationFunction getAggregationFunction() {
      return _aggregationFunction;
    }
  }

  private OrderByInfo createOrderByInfoForKeyColumn(int index) {
    return new OrderByInfo(OrderByType.KEY_COLUMN, index);
  }

  private OrderByInfo createOrderByInfoForAggregationColumn(int index, AggregationFunction aggregationFunction) {
    return new OrderByInfo(OrderByType.AGGREGATION_COLUMN, index, aggregationFunction);
  }
}
