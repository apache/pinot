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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.postaggregation.PostAggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.utils.ByteArray;


/**
 * Helper class for trimming and sorting records in the IndexedTable, based on the order by information
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class TableResizer {
  private final DataSchema _dataSchema;
  private final int _numGroupByExpressions;
  private final Map<ExpressionContext, Integer> _groupByExpressionIndexMap;
  private final AggregationFunction[] _aggregationFunctions;
  private final Map<FunctionContext, Integer> _aggregationFunctionIndexMap;
  private final int _numOrderByExpressions;
  private final OrderByValueExtractor[] _orderByValueExtractors;
  private final Comparator<IntermediateRecord> _intermediateRecordComparator;

  public TableResizer(DataSchema dataSchema, QueryContext queryContext) {
    _dataSchema = dataSchema;

    // NOTE: The data schema will always have group-by expressions in the front, followed by aggregation functions of
    //       the same order as in the query context. This is handled in AggregationGroupByOrderByOperator.

    List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
    assert groupByExpressions != null;
    _numGroupByExpressions = groupByExpressions.size();
    _groupByExpressionIndexMap = new HashMap<>();
    for (int i = 0; i < _numGroupByExpressions; i++) {
      _groupByExpressionIndexMap.put(groupByExpressions.get(i), i);
    }

    _aggregationFunctions = queryContext.getAggregationFunctions();
    assert _aggregationFunctions != null;
    _aggregationFunctionIndexMap = queryContext.getAggregationFunctionIndexMap();
    assert _aggregationFunctionIndexMap != null;

    List<OrderByExpressionContext> orderByExpressions = queryContext.getOrderByExpressions();
    assert orderByExpressions != null;
    _numOrderByExpressions = orderByExpressions.size();
    _orderByValueExtractors = new OrderByValueExtractor[_numOrderByExpressions];
    Comparator[] comparators = new Comparator[_numOrderByExpressions];
    for (int i = 0; i < _numOrderByExpressions; i++) {
      OrderByExpressionContext orderByExpression = orderByExpressions.get(i);
      _orderByValueExtractors[i] = getOrderByValueExtractor(orderByExpression.getExpression());
      comparators[i] = orderByExpression.isAsc() ? Comparator.naturalOrder() : Comparator.reverseOrder();
    }
    _intermediateRecordComparator = (o1, o2) -> {
      for (int i = 0; i < _numOrderByExpressions; i++) {
        int result = comparators[i].compare(o1._values[i], o2._values[i]);
        if (result != 0) {
          return result;
        }
      }
      return 0;
    };
  }

  /**
   * Helper method to construct a OrderByValueExtractor based on the given expression.
   */
  private OrderByValueExtractor getOrderByValueExtractor(ExpressionContext expression) {
    if (expression.getType() == ExpressionContext.Type.LITERAL) {
      return new LiteralExtractor(expression.getLiteral());
    }
    Integer groupByExpressionIndex = _groupByExpressionIndexMap.get(expression);
    if (groupByExpressionIndex != null) {
      // Group-by expression
      return new GroupByExpressionExtractor(groupByExpressionIndex);
    }
    FunctionContext function = expression.getFunction();
    Preconditions.checkState(function != null, "Failed to find ORDER-BY expression: %s in the GROUP-BY clause",
        expression);
    if (function.getType() == FunctionContext.Type.AGGREGATION) {
      // Aggregation function
      return new AggregationFunctionExtractor(_aggregationFunctionIndexMap.get(function));
    } else {
      // Post-aggregation function
      return new PostAggregationFunctionExtractor(function);
    }
  }

  /**
   * Constructs an IntermediateRecord from Record
   * The IntermediateRecord::key is the same Record::key
   * The IntermediateRecord::values contains only the order by columns, in the query's sort sequence
   * For aggregation values in the order by, the final result is extracted if the intermediate result is non-comparable
   */
  private IntermediateRecord getIntermediateRecord(Key key, Record record) {
    Comparable[] intermediateRecordValues = new Comparable[_numOrderByExpressions];
    for (int i = 0; i < _numOrderByExpressions; i++) {
      intermediateRecordValues[i] = _orderByValueExtractors[i].extract(record);
    }
    return new IntermediateRecord(key, intermediateRecordValues);
  }

  /**
   * Trim recordsMap to trimToSize, based on order by information
   * Resize only if number of records is greater than trimToSize
   * The resizer smartly chooses to create PQ of records to evict or records to retain, based on the number of records and the number of records to evict
   */
  public Map<Key, Record> resizeRecordsMap(Map<Key, Record> recordsMap, int trimToSize) {
    int numRecordsToEvict = recordsMap.size() - trimToSize;
    if (numRecordsToEvict > 0) {
      // TODO: compare the performance of converting to IntermediateRecord vs keeping Record, in cases where we do not need to extract final results
      if (numRecordsToEvict < trimToSize) {
        // num records to evict is smaller than num records to retain
        // make PQ of records to evict
        PriorityQueue<IntermediateRecord> priorityQueue =
            convertToIntermediateRecordsPQ(recordsMap, numRecordsToEvict, _intermediateRecordComparator);
        for (IntermediateRecord evictRecord : priorityQueue) {
          recordsMap.remove(evictRecord._key);
        }
        return recordsMap;
      } else {
        // num records to retain is smaller than num records to evict
        // make PQ of records to retain
        // TODO - Consider reusing the same map by removing record from the map
        // at the time it is evicted from PQ
        Map<Key, Record> trimmedRecordsMap;
        if (recordsMap instanceof ConcurrentMap) {
          // invoked by ConcurrentIndexedTable
          trimmedRecordsMap = new ConcurrentHashMap<>();
        } else {
          // invoked by SimpleIndexedTable
          trimmedRecordsMap = new HashMap<>();
        }
        Comparator<IntermediateRecord> comparator = _intermediateRecordComparator.reversed();
        PriorityQueue<IntermediateRecord> priorityQueue =
            convertToIntermediateRecordsPQ(recordsMap, trimToSize, comparator);
        for (IntermediateRecord recordToRetain : priorityQueue) {
          trimmedRecordsMap.put(recordToRetain._key, recordsMap.get(recordToRetain._key));
        }
        return trimmedRecordsMap;
      }
    }
    return recordsMap;
  }

  private PriorityQueue<IntermediateRecord> convertToIntermediateRecordsPQ(Map<Key, Record> recordsMap, int size,
      Comparator<IntermediateRecord> comparator) {
    PriorityQueue<IntermediateRecord> priorityQueue = new PriorityQueue<>(size, comparator);
    for (Map.Entry<Key, Record> entry : recordsMap.entrySet()) {
      IntermediateRecord intermediateRecord = getIntermediateRecord(entry.getKey(), entry.getValue());
      if (priorityQueue.size() < size) {
        priorityQueue.offer(intermediateRecord);
      } else {
        IntermediateRecord peek = priorityQueue.peek();
        if (comparator.compare(peek, intermediateRecord) < 0) {
          priorityQueue.poll();
          priorityQueue.offer(intermediateRecord);
        }
      }
    }
    return priorityQueue;
  }

  /**
   * Sorts the recordsMap using a priority queue and returns a sorted list of records
   * This method is to be called from IndexedTable::finish, if both resize and sort is needed
   */
  public List<Record> sortRecordsMap(Map<Key, Record> recordsMap, int trimToSize) {
    int numRecords = recordsMap.size();
    if (numRecords == 0) {
      return Collections.emptyList();
    }
    int numRecordsToRetain = Math.min(numRecords, trimToSize);
    // make PQ of sorted records to retain
    PriorityQueue<IntermediateRecord> priorityQueue =
        convertToIntermediateRecordsPQ(recordsMap, numRecordsToRetain, _intermediateRecordComparator.reversed());
    Record[] sortedArray = new Record[numRecordsToRetain];
    while (!priorityQueue.isEmpty()) {
      IntermediateRecord intermediateRecord = priorityQueue.poll();
      Record record = recordsMap.get(intermediateRecord._key);
      sortedArray[--numRecordsToRetain] = record;
    }
    return Arrays.asList(sortedArray);
  }

  /**
   * Helper class to store a subset of Record fields
   * IntermediateRecord is derived from a Record
   * Some of the main properties of an IntermediateRecord are:
   *
   * 1. Key in IntermediateRecord is expected to be identical to the one in the Record
   * 2. For values, IntermediateRecord should only have the columns needed for order by
   * 3. Inside the values, the columns should be ordered by the order by sequence
   * 4. For order by on aggregations, final results should extracted if the intermediate result is non-comparable
   */
  private static class IntermediateRecord {
    final Key _key;
    final Comparable[] _values;

    IntermediateRecord(Key key, Comparable[] values) {
      _key = key;
      _values = values;
    }
  }

  /**
   * Extractor for the order-by value from a Record.
   */
  private interface OrderByValueExtractor {

    /**
     * Returns the ColumnDataType of the value extracted.
     */
    ColumnDataType getValueType();

    /**
     * Extracts the value from the given Record.
     */
    Comparable extract(Record record);
  }

  /**
   * Extractor for a literal.
   */
  private static class LiteralExtractor implements OrderByValueExtractor {
    final String _literal;

    LiteralExtractor(String literal) {
      _literal = literal;
    }

    @Override
    public ColumnDataType getValueType() {
      return ColumnDataType.STRING;
    }

    @Override
    public String extract(Record record) {
      return _literal;
    }
  }

  /**
   * Extractor for a group-by expression.
   */
  private class GroupByExpressionExtractor implements OrderByValueExtractor {
    final int _index;

    GroupByExpressionExtractor(int groupByExpressionIndex) {
      _index = groupByExpressionIndex;
    }

    @Override
    public ColumnDataType getValueType() {
      return _dataSchema.getColumnDataType(_index);
    }

    @Override
    public Comparable extract(Record record) {
      return (Comparable) record.getValues()[_index];
    }
  }

  /**
   * Extractor for an aggregation function.
   */
  private class AggregationFunctionExtractor implements OrderByValueExtractor {
    final int _index;
    final AggregationFunction _aggregationFunction;

    AggregationFunctionExtractor(int aggregationFunctionIndex) {
      _index = aggregationFunctionIndex + _numGroupByExpressions;
      _aggregationFunction = _aggregationFunctions[aggregationFunctionIndex];
    }

    @Override
    public ColumnDataType getValueType() {
      return _aggregationFunction.getFinalResultColumnType();
    }

    @Override
    public Comparable extract(Record record) {
      return _aggregationFunction.extractFinalResult(record.getValues()[_index]);
    }
  }

  /**
   * Extractor for a post-aggregation function.
   */
  private class PostAggregationFunctionExtractor implements OrderByValueExtractor {
    final Object[] _arguments;
    final OrderByValueExtractor[] _argumentExtractors;
    final PostAggregationFunction _postAggregationFunction;

    PostAggregationFunctionExtractor(FunctionContext function) {
      assert function.getType() == FunctionContext.Type.TRANSFORM;

      List<ExpressionContext> arguments = function.getArguments();
      int numArguments = arguments.size();
      _arguments = new Object[numArguments];
      _argumentExtractors = new OrderByValueExtractor[numArguments];
      ColumnDataType[] argumentTypes = new ColumnDataType[numArguments];
      for (int i = 0; i < numArguments; i++) {
        OrderByValueExtractor argumentExtractor = getOrderByValueExtractor(arguments.get(i));
        _argumentExtractors[i] = argumentExtractor;
        argumentTypes[i] = argumentExtractor.getValueType();
      }
      _postAggregationFunction = new PostAggregationFunction(function.getFunctionName(), argumentTypes);
    }

    @Override
    public ColumnDataType getValueType() {
      return _postAggregationFunction.getResultType();
    }

    @Override
    public Comparable extract(Record record) {
      int numArguments = _arguments.length;
      for (int i = 0; i < numArguments; i++) {
        _arguments[i] = _argumentExtractors[i].extract(record);
      }
      Object result = _postAggregationFunction.invoke(_arguments);
      if (_postAggregationFunction.getResultType() == ColumnDataType.BYTES) {
        return new ByteArray((byte[]) result);
      } else {
        return (Comparable) result;
      }
    }
  }
}
