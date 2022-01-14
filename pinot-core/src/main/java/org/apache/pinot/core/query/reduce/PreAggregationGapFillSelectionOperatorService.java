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
package org.apache.pinot.core.query.reduce;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionFactory;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.core.util.GapfillUtils;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.DateTimeGranularitySpec;


/**
 * The <code>PreAggregationGapFillSelectionOperatorService</code> class provides the services for selection queries with
 * <code>ORDER BY</code>.
 * <p>Expected behavior:
 * <ul>
 *   <li>
 *     Return selection results with the same order of columns as user passed in.
 *     <ul>
 *       <li>Eg. SELECT colB, colA, colC FROM table -> [valB, valA, valC]</li>
 *     </ul>
 *   </li>
 *   <li>
 *     For 'SELECT *', return columns with alphabetically order.
 *     <ul>
 *       <li>Eg. SELECT * FROM table -> [valA, valB, valC]</li>
 *     </ul>
 *   </li>
 *   <li>
 *     Order by does not change the order of columns in selection results.
 *     <ul>
 *       <li>Eg. SELECT colB, colA, colC FROM table ORDER BY calC -> [valB, valA, valC]</li>
 *     </ul>
 *   </li>
 * </ul>
 */
@SuppressWarnings("rawtypes")
public class PreAggregationGapFillSelectionOperatorService {
  private final List<String> _columns;
  private final DataSchema _dataSchema;
  private final int _limitForAggregatedResult;
  private final int _limitForGapfilledResult;
  private final PriorityQueue<Object[]> _rows;

  private final DateTimeGranularitySpec _dateTimeGranularity;
  private final DateTimeFormatSpec _dateTimeFormatter;
  private final long _startMs;
  private final long _endMs;
  private final long _timeBucketSize;
  private final QueryContext _queryContext;
  private final QueryContext _preAggregateGapFillQueryContext;

  private final int _numOfGroupByKeys;
  private final List<Integer> _groupByKeyIndexes;
  private final boolean [] _isGroupBySelections;
  private final Set<Key> _groupByKeys;
  private final List<Key> _groupByKeyList;
  private final Map<Key, Integer> _groupByKeyMappings;
  private final Map<Key, Object[]> _previousByGroupKey;
  private final Map<String, ExpressionContext> _fillExpressions;
  private final FilterContext _gapFillFilterContext;

  /**
   * Constructor for <code>SelectionOperatorService</code> with {@link DataSchema}. (Inter segment)
   *
   * @param queryContext Selection order-by query
   * @param dataSchema data schema.
   */
  public PreAggregationGapFillSelectionOperatorService(QueryContext queryContext, DataSchema dataSchema) {
    _columns = Arrays.asList(dataSchema.getColumnNames());
    _dataSchema = dataSchema;
    _limitForAggregatedResult = queryContext.getLimit();
    _limitForGapfilledResult = queryContext.getPreAggregateGapFillQueryContext().getLimit();
    _rows = new PriorityQueue<>(Math.min(_limitForAggregatedResult,
        SelectionOperatorUtils.MAX_ROW_HOLDER_INITIAL_CAPACITY),
        getTypeCompatibleComparator());

    _queryContext = queryContext;
    _preAggregateGapFillQueryContext = queryContext.getPreAggregateGapFillQueryContext();
    ExpressionContext gapFillSelection = null;
    for (ExpressionContext expressionContext : _preAggregateGapFillQueryContext.getSelectExpressions()) {
      if (GapfillUtils.isPreAggregateGapfill(expressionContext)) {
        gapFillSelection = expressionContext;
        break;
      }
    }

    List<ExpressionContext> args = gapFillSelection.getFunction().getArguments();
    Preconditions.checkArgument(
        args.size() > 5, "PreAggregateGapFill does not have correct number of arguments.");
    Preconditions.checkArgument(
        args.get(1).getLiteral() != null, "The second argument of PostAggregateGapFill should be TimeFormatter.");
    Preconditions.checkArgument(
        args.get(2).getLiteral() != null, "The third argument of PostAggregateGapFill should be start time.");
    Preconditions.checkArgument(
        args.get(3).getLiteral() != null, "The fourth argument of PostAggregateGapFill should be end time.");
    Preconditions.checkArgument(
        args.get(4).getLiteral() != null, "The fifth argument of PostAggregateGapFill should be time bucket size.");

    _gapFillFilterContext = _queryContext.getFilter();
    _dateTimeFormatter = new DateTimeFormatSpec(args.get(1).getLiteral());
    String start = args.get(2).getLiteral();
    String end = args.get(3).getLiteral();
    _dateTimeGranularity = new DateTimeGranularitySpec(args.get(4).getLiteral());
    _startMs = truncate(_dateTimeFormatter.fromFormatToMillis(start));
    _endMs = truncate(_dateTimeFormatter.fromFormatToMillis(end));
    _timeBucketSize = _dateTimeGranularity.granularityToMillis();

    ExpressionContext timeseriesOn = null;
    _fillExpressions = new HashMap<>();
    for (int i = 5; i < args.size(); i++) {
      if (GapfillUtils.isFill(args.get(i))) {
        ExpressionContext fillExpression = args.get(i);
        _fillExpressions.put(fillExpression.getFunction().getArguments().get(0).getIdentifier(), fillExpression);
      } else if (GapfillUtils.isTimeSeriesOn(args.get(i))) {
        Preconditions.checkArgument(timeseriesOn == null, "Duplicate TimeSeriesOn expressions are specified.");
        timeseriesOn = args.get(i);
      }
    }

    _previousByGroupKey = new HashMap<>();
    _groupByKeyIndexes = new ArrayList<>();
    _isGroupBySelections = new boolean[dataSchema.getColumnDataTypes().length];
    _groupByKeys = new HashSet<>();
    _groupByKeyList = new ArrayList<>();
    _groupByKeyMappings = new HashMap<>();

    Map<String, Integer> indexes = new HashMap<>();
    for (int i = 0; i < _columns.size(); i++) {
      indexes.put(_columns.get(i), i);
    }

    Preconditions.checkArgument(timeseriesOn != null, "The TimeSeriesOn expressions should be specified.");
    _numOfGroupByKeys = timeseriesOn.getFunction().getArguments().size() - 1;
    List<ExpressionContext> timeSeries = timeseriesOn.getFunction().getArguments();
    for (int i = 1; i < timeSeries.size(); i++) {
      int index = indexes.get(timeSeries.get(i).getIdentifier());
      _isGroupBySelections[index] = true;
      _groupByKeyIndexes.add(index);
    }
  }

  private Key constructGroupKeys(Object[] row) {
    Object [] groupKeys = new Object[_numOfGroupByKeys];
    for (int i = 0; i < _numOfGroupByKeys; i++) {
      groupKeys[i] = row[_groupByKeyIndexes.get(i)];
    }
    return new Key(groupKeys);
  }

  private long truncate(long epoch) {
    int sz = _dateTimeGranularity.getSize();
    return epoch / sz * sz;
  }

  List<Object[]> gapFillAndAggregate(List<Object[]> sortedRows, DataSchema dataSchemaForAggregatedResult) {
    List<Object[]> result = new ArrayList<>();
    Iterator<Object[]> sortedIterator = sortedRows.iterator();
    ColumnDataType[] resultColumnDataTypes = _dataSchema.getColumnDataTypes();
    int numResultColumns = resultColumnDataTypes.length;

    PreAggregateGapfillFilterHandler postGapfillFilterHandler = null;
    if (_gapFillFilterContext != null) {
      postGapfillFilterHandler = new PreAggregateGapfillFilterHandler(_gapFillFilterContext, _dataSchema);
    }
    PreAggregateGapfillFilterHandler postAggregateFilterHandler = null;
    if (_queryContext.getHavingFilter() != null) {
      postAggregateFilterHandler = new PreAggregateGapfillFilterHandler(
          _queryContext.getHavingFilter(), dataSchemaForAggregatedResult);
    }
    Object[] row = null;
    int numberOfGapfilledRows = 0;
    for (long time = _startMs; time < _endMs; time += _timeBucketSize) {
      List<Object[]> bucketedResult = new ArrayList<>();
      Set<Key> keys = new HashSet<>(_groupByKeys);
      if (row == null && sortedIterator.hasNext()) {
        row = sortedIterator.next();
      }

      while (row != null) {
        Object[] resultRow = row;
        for (int i = 0; i < resultColumnDataTypes.length; i++) {
          resultRow[i] = resultColumnDataTypes[i].format(resultRow[i]);
        }

        long timeCol = _dateTimeFormatter.fromFormatToMillis(String.valueOf(resultRow[1]));
        if (timeCol > time) {
          break;
        }
        if (timeCol == time) {
          if (postGapfillFilterHandler == null || postGapfillFilterHandler.isMatch(row)) {
            bucketedResult.add(resultRow);
            numberOfGapfilledRows++;
            if (numberOfGapfilledRows >= _limitForGapfilledResult) {
              break;
            }
          }
          Key key = constructGroupKeys(resultRow);
          keys.remove(key);
          _previousByGroupKey.put(key, resultRow);
        }
        if (sortedIterator.hasNext()) {
          row = sortedIterator.next();
        } else {
          row = null;
        }
      }

      for (Key key : keys) {
        Object[] gapfillRow = new Object[numResultColumns];
        int keyIndex = 0;
        gapfillRow[0] = time;
        if (resultColumnDataTypes[1] == ColumnDataType.LONG) {
          gapfillRow[1] = Long.valueOf(_dateTimeFormatter.fromMillisToFormat(time));
        } else {
          gapfillRow[1] = _dateTimeFormatter.fromMillisToFormat(time);
        }
        for (int i = 2; i < _isGroupBySelections.length; i++) {
          if (_isGroupBySelections[i]) {
            gapfillRow[i] = key.getValues()[keyIndex++];
          } else {
            gapfillRow[i] = getFillValue(i, _dataSchema.getColumnName(i), key, resultColumnDataTypes[i]);
          }
        }

        if (postGapfillFilterHandler == null || postGapfillFilterHandler.isMatch(gapfillRow)) {
          bucketedResult.add(gapfillRow);
          numberOfGapfilledRows++;
          if (numberOfGapfilledRows > _limitForGapfilledResult) {
            break;
          }
        }
      }
      Object[] aggregatedRow = aggregateGapfilledData(bucketedResult, time);
      if (postAggregateFilterHandler == null || postAggregateFilterHandler.isMatch(aggregatedRow)) {
        result.add(aggregatedRow);
      }
      if (result.size() >= _limitForAggregatedResult) {
        break;
      }
    }
    return result;
  }

  Object[] aggregateGapfilledData(List<Object[]> bucketedRows, long time) {
      Object[] resultRow = new Object[_queryContext.getSelectExpressions().size()];
      String formattedTime = _dateTimeFormatter.fromMillisToFormat(time);
      Map<ExpressionContext, BlockValSet> blockValSetMap = new HashMap<>();
      blockValSetMap.put(ExpressionContext.forIdentifier(_dataSchema.getColumnName(0)),
          new BlockValSetImpl(_dataSchema.getColumnDataType(0), bucketedRows, 0));
      for (int i = 2; i < _dataSchema.getColumnNames().length - 1; i++) {
        blockValSetMap.put(ExpressionContext.forIdentifier(_dataSchema.getColumnName(i)),
            new BlockValSetImpl(_dataSchema.getColumnDataType(i), bucketedRows, i));
      }

      for (int i = 0; i < _queryContext.getSelectExpressions().size(); i++) {
        ExpressionContext expressionContext = _queryContext.getSelectExpressions().get(i);
        if (expressionContext.getType() != ExpressionContext.Type.FUNCTION) {
          resultRow[i] = formattedTime;
        } else {
          FunctionContext functionContext = expressionContext.getFunction();
          AggregationFunction aggregationFunction =
              AggregationFunctionFactory.getAggregationFunction(functionContext, _queryContext);
          GroupByResultHolder firstGroupByResultHolder =
              aggregationFunction.createGroupByResultHolder(_groupByKeys.size(), _groupByKeys.size());
          int[] groupKeyArray = new int[bucketedRows.size()];
          aggregationFunction
              .aggregateGroupBySV(bucketedRows.size(), groupKeyArray, firstGroupByResultHolder, blockValSetMap);
          resultRow[i] = aggregationFunction.extractGroupByResult(firstGroupByResultHolder, 0);
          resultRow[i] = aggregationFunction.extractFinalResult(resultRow[i]);
        }
      }
      return resultRow;
  }

  Object getFillValue(int columnIndex, String columnName, Object key, ColumnDataType dataType) {
    ExpressionContext expressionContext = _fillExpressions.get(columnName);
    if (expressionContext != null
        && expressionContext.getFunction() != null
        && GapfillUtils.isFill(expressionContext)) {
      List<ExpressionContext> args = expressionContext.getFunction().getArguments();
      if (args.get(1).getLiteral() == null) {
        throw new UnsupportedOperationException("Wrong Sql.");
      }
      GapfillUtils.FillType fillType = GapfillUtils.FillType.valueOf(args.get(1).getLiteral());
      if (fillType == GapfillUtils.FillType.FILL_DEFAULT_VALUE) {
        // TODO: may fill the default value from sql in the future.
        return GapfillUtils.getDefaultValue(dataType);
      } else if (fillType == GapfillUtils.FillType.FILL_PREVIOUS_VALUE) {
        Object[] row = _previousByGroupKey.get(key);
        if (row != null) {
          return row[columnIndex];
        } else {
          return GapfillUtils.getDefaultValue(dataType);
        }
      } else {
        throw new UnsupportedOperationException("unsupported fill type.");
      }
    } else {
      return GapfillUtils.getDefaultValue(dataType);
    }
  }

  /**
   * Helper method to get the type-compatible {@link Comparator} for selection rows. (Inter segment)
   * <p>Type-compatible comparator allows compatible types to compare with each other.
   *
   * @return flexible {@link Comparator} for selection rows.
   */
  private Comparator<Object[]> getTypeCompatibleComparator() {
    ColumnDataType[] columnDataTypes = _dataSchema.getColumnDataTypes();

    return (o1, o2) -> {
      Object v1 = o1[0];
      Object v2 = o2[0];
      int result;
      if (columnDataTypes[0].isNumber()) {
        result = Double.compare(((Number) v1).doubleValue(), ((Number) v2).doubleValue());
      } else {
        //noinspection unchecked
        result = ((Comparable) v1).compareTo(v2);
      }
      return result;
    };
  }

  /**
   * Get the selection results.
   *
   * @return selection results.
   */
  public PriorityQueue<Object[]> getRows() {
    return _rows;
  }

  /**
   * Reduces a collection of {@link DataTable}s to selection rows for selection queries with <code>ORDER BY</code>.
   * (Broker side)
   */
  public List<Object[]> reduceWithOrdering(Collection<DataTable> dataTables) {
    for (DataTable dataTable : dataTables) {
      int numRows = dataTable.getNumberOfRows();
      for (int rowId = 0; rowId < numRows; rowId++) {
        Object[] row = SelectionOperatorUtils.extractRowFromDataTable(dataTable, rowId);
        _rows.add(row);
      }
    }
    LinkedList<Object[]> sortedRows = new LinkedList<>();
    while (!_rows.isEmpty()) {
      Object[] row = _rows.poll();
      sortedRows.add(row);
      Key k = constructGroupKeys(row);
      if (!_groupByKeys.contains(k)) {
        _groupByKeys.add(k);
        _groupByKeyMappings.put(k, _groupByKeyList.size());
        _groupByKeyList.add(k);
      }
    }
    return sortedRows;
  }
}
