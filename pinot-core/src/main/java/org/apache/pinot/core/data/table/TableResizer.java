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
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
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
  private final Map<Pair<FunctionContext, FilterContext>, Integer> _filteredAggregationIndexMap;
  private final List<Pair<AggregationFunction, FilterContext>> _filteredAggregationFunctions;
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
    _filteredAggregationIndexMap = queryContext.getFilteredAggregationsIndexMap();
    _filteredAggregationFunctions = queryContext.getFilteredAggregationFunctions();

    List<OrderByExpressionContext> orderByExpressions = queryContext.getOrderByExpressions();
    assert orderByExpressions != null;
    _numOrderByExpressions = orderByExpressions.size();
    _orderByValueExtractors = new OrderByValueExtractor[_numOrderByExpressions];
    Comparator[] comparators = new Comparator[_numOrderByExpressions];
    int[] nullComparisonResults = new int[_numOrderByExpressions];
    for (int i = 0; i < _numOrderByExpressions; i++) {
      OrderByExpressionContext orderByExpression = orderByExpressions.get(i);
      _orderByValueExtractors[i] = getOrderByValueExtractor(orderByExpression.getExpression());
      comparators[i] = orderByExpression.isAsc() ? Comparator.naturalOrder() : Comparator.reverseOrder();
      nullComparisonResults[i] = orderByExpression.isNullsLast() ? -1 : 1;
    }
    boolean nullHandlingEnabled = queryContext.isNullHandlingEnabled();
    if (nullHandlingEnabled) {
      _intermediateRecordComparator = (o1, o2) -> {
        for (int i = 0; i < _numOrderByExpressions; i++) {
          Object v1 = o1._values[i];
          Object v2 = o2._values[i];
          if (v1 == null) {
            if (v2 == null) {
              continue;
            }
            return -nullComparisonResults[i];
          } else if (v2 == null) {
            return nullComparisonResults[i];
          }
          int result = comparators[i].compare(v1, v2);
          if (result != 0) {
            return result;
          }
        }
        return 0;
      };
    } else {
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
  }

  /**
   * Helper method to construct a OrderByValueExtractor based on the given expression.
   */
  private OrderByValueExtractor getOrderByValueExtractor(ExpressionContext expression) {
    if (expression.getType() == ExpressionContext.Type.LITERAL) {
      return new LiteralExtractor(expression.getLiteral().getStringValue());
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
    } else if (function.getType() == FunctionContext.Type.TRANSFORM
        && "FILTER".equalsIgnoreCase(function.getFunctionName())) {
      FunctionContext aggregation = function.getArguments().get(0).getFunction();
      ExpressionContext filterExpression = function.getArguments().get(1);
      FilterContext filter = RequestContextUtils.getFilter(filterExpression);

      int functionIndex = _filteredAggregationIndexMap.get(Pair.of(aggregation, filter));
      AggregationFunction aggregationFunction = _filteredAggregationFunctions.get(functionIndex).getLeft();
      return new AggregationFunctionExtractor(functionIndex, aggregationFunction);
    } else {
      // Post-aggregation function
      return new PostAggregationFunctionExtractor(function);
    }
  }

  /**
   * Constructs an IntermediateRecord by extracting the order-by values from the record.
   */
  private IntermediateRecord getIntermediateRecord(Key key, Record record) {
    Comparable[] orderByValues = new Comparable[_numOrderByExpressions];
    for (int i = 0; i < _numOrderByExpressions; i++) {
      orderByValues[i] = _orderByValueExtractors[i].extract(record);
    }
    return new IntermediateRecord(key, record, orderByValues);
  }

  /**
   * Resizes the recordsMap to the given size.
   */
  public void resizeRecordsMap(Map<Key, Record> recordsMap, int size) {
    int numRecordsToEvict = recordsMap.size() - size;
    if (numRecordsToEvict <= 0) {
      return;
    }
    if (numRecordsToEvict <= size) {
      // Fewer records to evict than retain, make a heap of records to evict
      IntermediateRecord[] recordsToEvict =
          getTopRecordsHeap(recordsMap, numRecordsToEvict, _intermediateRecordComparator);
      for (IntermediateRecord recordToEvict : recordsToEvict) {
        recordsMap.remove(recordToEvict._key);
      }
    } else {
      // Fewer records to retain than evict, make a heap of records to retain
      IntermediateRecord[] recordsToRetain =
          getTopRecordsHeap(recordsMap, size, _intermediateRecordComparator.reversed());
      recordsMap.clear();
      for (IntermediateRecord recordToRetain : recordsToRetain) {
        recordsMap.put(recordToRetain._key, recordToRetain._record);
      }
    }
  }

  /**
   * Returns a heap of the top records from the recordsMap.
   */
  private IntermediateRecord[] getTopRecordsHeap(Map<Key, Record> recordsMap, int size,
      Comparator<IntermediateRecord> comparator) {
    // Should not reach here when map size <= heap size because there is no need to create a heap
    assert recordsMap.size() > size;
    Iterator<Map.Entry<Key, Record>> mapEntryIterator = recordsMap.entrySet().iterator();

    // Initialize a heap with the first 'size' map entries
    IntermediateRecord[] heap = new IntermediateRecord[size];
    for (int i = 0; i < size; i++) {
      Map.Entry<Key, Record> entry = mapEntryIterator.next();
      heap[i] = getIntermediateRecord(entry.getKey(), entry.getValue());
    }
    makeHeap(heap, size, comparator);

    // Keep updating the heap with the remaining map entries
    while (mapEntryIterator.hasNext()) {
      Map.Entry<Key, Record> entry = mapEntryIterator.next();
      IntermediateRecord intermediateRecord = getIntermediateRecord(entry.getKey(), entry.getValue());
      if (comparator.compare(intermediateRecord, heap[0]) > 0) {
        heap[0] = intermediateRecord;
        downHeap(heap, size, 0, comparator);
      }
    }

    return heap;
  }

  /**
   * Borrowed from {@link it.unimi.dsi.fastutil.objects.ObjectHeaps}.
   */
  private static void makeHeap(IntermediateRecord[] heap, int size, Comparator<IntermediateRecord> c) {
    int i = size >>> 1;
    while (i-- != 0) {
      downHeap(heap, size, i, c);
    }
  }

  /**
   * Borrowed from {@link it.unimi.dsi.fastutil.objects.ObjectHeaps} without the redundant checks.
   */
  private static void downHeap(IntermediateRecord[] heap, int size, int i, Comparator<IntermediateRecord> c) {
    IntermediateRecord e = heap[i];
    int child;
    while ((child = (i << 1) + 1) < size) {
      IntermediateRecord t = heap[child];
      int right = child + 1;
      if (right < size && c.compare(heap[right], t) < 0) {
        child = right;
        t = heap[child];
      }
      if (c.compare(e, t) <= 0) {
        break;
      }
      heap[i] = t;
      i = child;
    }
    heap[i] = e;
  }

  /**
   * Returns the top records from the recordsMap.
   */
  public Collection<Record> getTopRecords(Map<Key, Record> recordsMap, int size, boolean sort) {
    return sort ? getSortedTopRecords(recordsMap, size) : getUnsortedTopRecords(recordsMap, size);
  }

  @VisibleForTesting
  List<Record> getSortedTopRecords(Map<Key, Record> recordsMap, int size) {
    int numRecords = recordsMap.size();
    if (numRecords == 0) {
      return Collections.emptyList();
    }
    if (numRecords <= size) {
      // Use quick sort if all the records are top records
      IntermediateRecord[] intermediateRecords = new IntermediateRecord[numRecords];
      int index = 0;
      for (Map.Entry<Key, Record> entry : recordsMap.entrySet()) {
        intermediateRecords[index++] = getIntermediateRecord(entry.getKey(), entry.getValue());
      }
      Arrays.sort(intermediateRecords, _intermediateRecordComparator);
      Record[] sortedTopRecords = new Record[numRecords];
      for (int i = 0; i < numRecords; i++) {
        sortedTopRecords[i] = intermediateRecords[i]._record;
      }
      return Arrays.asList(sortedTopRecords);
    } else {
      // Use heap sort if only partial records are top records
      Comparator<IntermediateRecord> comparator = _intermediateRecordComparator.reversed();
      IntermediateRecord[] topRecordsHeap = getTopRecordsHeap(recordsMap, size, comparator);
      Record[] sortedTopRecords = new Record[size];
      while (size-- > 0) {
        sortedTopRecords[size] = topRecordsHeap[0]._record;
        topRecordsHeap[0] = topRecordsHeap[size];
        downHeap(topRecordsHeap, size, 0, comparator);
      }
      return Arrays.asList(sortedTopRecords);
    }
  }

  private Collection<Record> getUnsortedTopRecords(Map<Key, Record> recordsMap, int size) {
    int numRecords = recordsMap.size();
    if (numRecords <= size) {
      return recordsMap.values();
    } else {
      IntermediateRecord[] topRecords = getTopRecordsHeap(recordsMap, size, _intermediateRecordComparator.reversed());
      Record[] unsortedTopRecords = new Record[size];
      int index = 0;
      for (IntermediateRecord topRecord : topRecords) {
        unsortedTopRecords[index++] = topRecord._record;
      }
      return Arrays.asList(unsortedTopRecords);
    }
  }

  /**
   * Trims the aggregation results using a heap and returns the top records.
   * This method is to be called from individual segment if the intermediate results need to be trimmed.
   */
  public List<IntermediateRecord> trimInSegmentResults(GroupKeyGenerator groupKeyGenerator,
      GroupByResultHolder[] groupByResultHolders, int size) {
    // Should not reach here when numGroups <= heap size because there is no need to create a heap
    assert groupKeyGenerator.getNumKeys() > size;
    Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator = groupKeyGenerator.getGroupKeys();
    Comparator<IntermediateRecord> comparator = _intermediateRecordComparator.reversed();

    // Initialize a heap with the first 'size' groups
    IntermediateRecord[] heap = new IntermediateRecord[size];
    for (int i = 0; i < size; i++) {
      heap[i] = getIntermediateRecord(groupKeyIterator.next(), groupByResultHolders);
    }
    makeHeap(heap, size, comparator);

    // Keep updating the heap with the remaining groups
    while (groupKeyIterator.hasNext()) {
      IntermediateRecord intermediateRecord = getIntermediateRecord(groupKeyIterator.next(), groupByResultHolders);
      if (comparator.compare(intermediateRecord, heap[0]) > 0) {
        heap[0] = intermediateRecord;
        downHeap(heap, size, 0, comparator);
      }
    }

    return Arrays.asList(heap);
  }

  /**
   * Constructs an IntermediateRecord for the given group.
   */
  private IntermediateRecord getIntermediateRecord(GroupKeyGenerator.GroupKey groupKey,
      GroupByResultHolder[] groupByResultHolders) {
    int numAggregationFunctions = _aggregationFunctions.length;
    int numColumns = numAggregationFunctions + _numGroupByExpressions;
    Object[] keys = groupKey._keys;
    Object[] values = Arrays.copyOf(keys, numColumns);
    int groupId = groupKey._groupId;
    for (int i = 0; i < numAggregationFunctions; i++) {
      values[_numGroupByExpressions + i] =
          _aggregationFunctions[i].extractGroupByResult(groupByResultHolders[i], groupId);
    }
    return getIntermediateRecord(new Key(keys), new Record(values));
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

    AggregationFunctionExtractor(int aggregationFunctionIndex, AggregationFunction aggregationFunction) {
      _index = aggregationFunctionIndex + _numGroupByExpressions;
      _aggregationFunction = aggregationFunction;
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
