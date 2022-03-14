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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionFactory;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.util.GapfillUtils;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.DateTimeGranularitySpec;


/**
 * Helper class to reduce and set gap fill results into the BrokerResponseNative
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class GapFillProcessor {
  private final QueryContext _queryContext;

  private final int _limitForAggregatedResult;
  private final DateTimeGranularitySpec _dateTimeGranularity;
  private final DateTimeFormatSpec _dateTimeFormatter;
  private final long _startMs;
  private final long _endMs;
  private final long _timeBucketSize;
  private final int _numOfTimeBuckets;
  private final List<Integer> _groupByKeyIndexes;
  private final Set<Key> _groupByKeys;
  private final Map<Key, Object[]> _previousByGroupKey;
  private final Map<String, ExpressionContext> _fillExpressions;
  private final List<ExpressionContext> _timeSeries;
  private final GapfillUtils.GapfillType _gapfillType;
  private int _limitForGapfilledResult;
  private boolean[] _isGroupBySelections;
  private final int _timeBucketColumnIndex;
  private int[] _sourceColumnIndexForResultSchema = null;

  GapFillProcessor(QueryContext queryContext) {
    _queryContext = queryContext;
    _gapfillType = queryContext.getGapfillType();
    _limitForAggregatedResult = queryContext.getLimit();
    if (_gapfillType == GapfillUtils.GapfillType.AGGREGATE_GAP_FILL
        || _gapfillType == GapfillUtils.GapfillType.GAP_FILL) {
      _limitForGapfilledResult = queryContext.getLimit();
    } else {
      _limitForGapfilledResult = queryContext.getSubQueryContext().getLimit();
    }

    ExpressionContext gapFillSelection = GapfillUtils.getGapfillExpressionContext(queryContext);
    _timeBucketColumnIndex = GapfillUtils.findTimeBucketColumnIndex(queryContext);

    List<ExpressionContext> args = gapFillSelection.getFunction().getArguments();

    _dateTimeFormatter = new DateTimeFormatSpec(args.get(1).getLiteral());
    _dateTimeGranularity = new DateTimeGranularitySpec(args.get(4).getLiteral());
    String start = args.get(2).getLiteral();
    _startMs = truncate(_dateTimeFormatter.fromFormatToMillis(start));
    String end = args.get(3).getLiteral();
    _endMs = truncate(_dateTimeFormatter.fromFormatToMillis(end));
    _timeBucketSize = _dateTimeGranularity.granularityToMillis();
    _numOfTimeBuckets = (int) ((_endMs - _startMs) / _timeBucketSize);

    _fillExpressions = GapfillUtils.getFillExpressions(gapFillSelection);

    _previousByGroupKey = new HashMap<>();
    _groupByKeyIndexes = new ArrayList<>();
    _groupByKeys = new HashSet<>();

    ExpressionContext timeseriesOn = GapfillUtils.getTimeSeriesOnExpressionContext(gapFillSelection);
    _timeSeries = timeseriesOn.getFunction().getArguments();
  }

  private int findBucketIndex(long time) {
    return (int) ((time - _startMs) / _timeBucketSize);
  }

  private void replaceColumnNameWithAlias(DataSchema dataSchema) {
    QueryContext queryContext;
    if (_gapfillType == GapfillUtils.GapfillType.AGGREGATE_GAP_FILL_AGGREGATE) {
      queryContext = _queryContext.getSubQueryContext().getSubQueryContext();
    } else if (_gapfillType == GapfillUtils.GapfillType.GAP_FILL) {
      queryContext = _queryContext;
    } else {
      queryContext = _queryContext.getSubQueryContext();
    }
    List<String> aliasList = queryContext.getAliasList();
    Map<String, String> columnNameToAliasMap = new HashMap<>();
    for (int i = 0; i < aliasList.size(); i++) {
      if (aliasList.get(i) != null) {
        ExpressionContext selection = queryContext.getSelectExpressions().get(i);
        if (GapfillUtils.isGapfill(selection)) {
          selection = selection.getFunction().getArguments().get(0);
        }
        columnNameToAliasMap.put(selection.toString(), aliasList.get(i));
      }
    }
    for (int i = 0; i < dataSchema.getColumnNames().length; i++) {
      if (columnNameToAliasMap.containsKey(dataSchema.getColumnNames()[i])) {
        dataSchema.getColumnNames()[i] = columnNameToAliasMap.get(dataSchema.getColumnNames()[i]);
      }
    }
  }

  /**
   * Here are three things that happen
   * 1. Sort the result sets from all pinot servers based on timestamp
   * 2. Gapfill the data for missing entities per time bucket
   * 3. Aggregate the dataset per time bucket.
   */
  public void process(BrokerResponseNative brokerResponseNative) {
    DataSchema dataSchema = brokerResponseNative.getResultTable().getDataSchema();
    DataSchema resultTableSchema = getResultTableDataSchema(dataSchema);
    if (brokerResponseNative.getResultTable().getRows().isEmpty()) {
      brokerResponseNative.setResultTable(new ResultTable(resultTableSchema, Collections.emptyList()));
      return;
    }

    String[] columns = dataSchema.getColumnNames();

    Map<String, Integer> indexes = new HashMap<>();
    for (int i = 0; i < columns.length; i++) {
      indexes.put(columns[i], i);
    }

    _isGroupBySelections = new boolean[dataSchema.getColumnDataTypes().length];

    // The first one argument of timeSeries is time column. The left ones are defining entity.
    for (ExpressionContext entityColum : _timeSeries) {
      int index = indexes.get(entityColum.getIdentifier());
      _isGroupBySelections[index] = true;
    }

    for (int i = 0; i < _isGroupBySelections.length; i++) {
      if (_isGroupBySelections[i]) {
        _groupByKeyIndexes.add(i);
      }
    }

    List<Object[]>[] timeBucketedRawRows = putRawRowsIntoTimeBucket(brokerResponseNative.getResultTable().getRows());

    List<Object[]> resultRows;
    replaceColumnNameWithAlias(dataSchema);

    if (_queryContext.getAggregationFunctions() == null) {

      Map<String, Integer> sourceColumnsIndexes = new HashMap<>();
      for (int i = 0; i < dataSchema.getColumnNames().length; i++) {
        sourceColumnsIndexes.put(dataSchema.getColumnName(i), i);
      }
      _sourceColumnIndexForResultSchema = new int[resultTableSchema.getColumnNames().length];
      for (int i = 0; i < _sourceColumnIndexForResultSchema.length; i++) {
        _sourceColumnIndexForResultSchema[i] = sourceColumnsIndexes.get(resultTableSchema.getColumnName(i));
      }
    }

    if (_gapfillType == GapfillUtils.GapfillType.GAP_FILL_AGGREGATE || _gapfillType == GapfillUtils.GapfillType.GAP_FILL
        || _gapfillType == GapfillUtils.GapfillType.GAP_FILL_SELECT) {
      List<Object[]> gapfilledRows = gapFillAndAggregate(timeBucketedRawRows, resultTableSchema, dataSchema);
      if (_gapfillType == GapfillUtils.GapfillType.GAP_FILL_SELECT) {
        resultRows = new ArrayList<>(gapfilledRows.size());
        resultRows.addAll(gapfilledRows);
      } else {
        resultRows = gapfilledRows;
      }
    } else {
      resultRows = gapFillAndAggregate(timeBucketedRawRows, resultTableSchema, dataSchema);
    }
    brokerResponseNative.setResultTable(new ResultTable(resultTableSchema, resultRows));
  }

  /**
   * Constructs the DataSchema for the ResultTable.
   */
  private DataSchema getResultTableDataSchema(DataSchema dataSchema) {
    if (_gapfillType == GapfillUtils.GapfillType.GAP_FILL) {
      return dataSchema;
    }

    int numOfColumns = _queryContext.getSelectExpressions().size();
    String[] columnNames = new String[numOfColumns];
    ColumnDataType[] columnDataTypes = new ColumnDataType[numOfColumns];
    for (int i = 0; i < numOfColumns; i++) {
      ExpressionContext expressionContext = _queryContext.getSelectExpressions().get(i);
      if (GapfillUtils.isGapfill(expressionContext)) {
        expressionContext = expressionContext.getFunction().getArguments().get(0);
      }
      if (expressionContext.getType() != ExpressionContext.Type.FUNCTION) {
        columnNames[i] = expressionContext.getIdentifier();
        columnDataTypes[i] = ColumnDataType.STRING;
      } else {
        FunctionContext functionContext = expressionContext.getFunction();
        AggregationFunction aggregationFunction =
            AggregationFunctionFactory.getAggregationFunction(functionContext, _queryContext);
        columnDataTypes[i] = aggregationFunction.getFinalResultColumnType();
        columnNames[i] = functionContext.toString();
      }
    }
    return new DataSchema(columnNames, columnDataTypes);
  }

  private Key constructGroupKeys(Object[] row) {
    Object[] groupKeys = new Object[_groupByKeyIndexes.size()];
    for (int i = 0; i < _groupByKeyIndexes.size(); i++) {
      groupKeys[i] = row[_groupByKeyIndexes.get(i)];
    }
    return new Key(groupKeys);
  }

  private long truncate(long epoch) {
    int sz = _dateTimeGranularity.getSize();
    return epoch / sz * sz;
  }

  private List<Object[]> gapFillAndAggregate(List<Object[]>[] timeBucketedRawRows,
      DataSchema dataSchemaForAggregatedResult, DataSchema dataSchema) {
    List<Object[]> result = new ArrayList<>();

    GapfillFilterHandler postGapfillFilterHandler = null;
    if (_queryContext.getSubQueryContext() != null && _queryContext.getFilter() != null) {
      postGapfillFilterHandler = new GapfillFilterHandler(_queryContext.getFilter(), dataSchema);
    }
    GapfillFilterHandler postAggregateHavingFilterHandler = null;
    if (_queryContext.getHavingFilter() != null) {
      postAggregateHavingFilterHandler =
          new GapfillFilterHandler(_queryContext.getHavingFilter(), dataSchemaForAggregatedResult);
    }
    for (long time = _startMs; time < _endMs; time += _timeBucketSize) {
      int index = findBucketIndex(time);
      List<Object[]> bucketedResult = gapfill(time, timeBucketedRawRows[index], dataSchema, postGapfillFilterHandler);
      if (_queryContext.getAggregationFunctions() == null) {
        for (Object [] row : bucketedResult) {
          Object[] resultRow = new Object[_sourceColumnIndexForResultSchema.length];
          for (int i = 0; i < _sourceColumnIndexForResultSchema.length; i++) {
            resultRow[i] = row[_sourceColumnIndexForResultSchema[i]];
          }
          result.add(resultRow);
        }
      } else if (bucketedResult.size() > 0) {
        List<Object[]> aggregatedRows = aggregateGapfilledData(bucketedResult, dataSchema);
        for (Object[] aggregatedRow : aggregatedRows) {
          if (postAggregateHavingFilterHandler == null || postAggregateHavingFilterHandler.isMatch(aggregatedRow)) {
            result.add(aggregatedRow);
          }
          if (result.size() >= _limitForAggregatedResult) {
            return result;
          }
        }
      }
    }
    return result;
  }

  private List<Object[]> gapfill(long bucketTime, List<Object[]> rawRowsForBucket, DataSchema dataSchema,
      GapfillFilterHandler postGapfillFilterHandler) {
    List<Object[]> bucketedResult = new ArrayList<>();
    ColumnDataType[] resultColumnDataTypes = dataSchema.getColumnDataTypes();
    int numResultColumns = resultColumnDataTypes.length;
    Set<Key> keys = new HashSet<>(_groupByKeys);

    if (rawRowsForBucket != null) {
      for (Object[] resultRow : rawRowsForBucket) {
        for (int i = 0; i < resultColumnDataTypes.length; i++) {
          resultRow[i] = resultColumnDataTypes[i].format(resultRow[i]);
        }

        long timeCol = _dateTimeFormatter.fromFormatToMillis(String.valueOf(resultRow[0]));
        if (timeCol > bucketTime) {
          break;
        }
        if (timeCol == bucketTime) {
          if (postGapfillFilterHandler == null || postGapfillFilterHandler.isMatch(resultRow)) {
            if (bucketedResult.size() >= _limitForGapfilledResult) {
              _limitForGapfilledResult = 0;
              break;
            } else {
              bucketedResult.add(resultRow);
            }
          }
          Key key = constructGroupKeys(resultRow);
          keys.remove(key);
          _previousByGroupKey.put(key, resultRow);
        }
      }
    }

    for (Key key : keys) {
      Object[] gapfillRow = new Object[numResultColumns];
      int keyIndex = 0;
      if (resultColumnDataTypes[0] == ColumnDataType.LONG) {
        gapfillRow[0] = Long.valueOf(_dateTimeFormatter.fromMillisToFormat(bucketTime));
      } else {
        gapfillRow[0] = _dateTimeFormatter.fromMillisToFormat(bucketTime);
      }
      for (int i = 1; i < _isGroupBySelections.length; i++) {
        if (_isGroupBySelections[i]) {
          gapfillRow[i] = key.getValues()[keyIndex++];
        } else {
          gapfillRow[i] = getFillValue(i, dataSchema.getColumnName(i), key, resultColumnDataTypes[i]);
        }
      }

      if (postGapfillFilterHandler == null || postGapfillFilterHandler.isMatch(gapfillRow)) {
        if (bucketedResult.size() >= _limitForGapfilledResult) {
          break;
        } else {
          bucketedResult.add(gapfillRow);
        }
      }
    }
    if (_limitForGapfilledResult > _groupByKeys.size()) {
      _limitForGapfilledResult -= _groupByKeys.size();
    } else {
      _limitForGapfilledResult = 0;
    }
    return bucketedResult;
  }

  private List<Object[]> aggregateGapfilledData(List<Object[]> bucketedRows, DataSchema dataSchema) {
    List<ExpressionContext> groupbyExpressions = _queryContext.getGroupByExpressions();
    Preconditions.checkArgument(groupbyExpressions != null, "No GroupBy Clause.");
    Map<String, Integer> indexes = new HashMap<>();
    for (int i = 0; i < dataSchema.getColumnNames().length; i++) {
      indexes.put(dataSchema.getColumnName(i), i);
    }

    Map<List<Object>, Integer> groupKeyIndexes = new HashMap<>();
    int[] groupKeyArray = new int[bucketedRows.size()];
    List<Object[]> aggregatedResult = new ArrayList<>();
    for (int i = 0; i < bucketedRows.size(); i++) {
      Object[] bucketedRow = bucketedRows.get(i);
      List<Object> groupKey = new ArrayList<>(groupbyExpressions.size());
      for (ExpressionContext groupbyExpression : groupbyExpressions) {
        int columnIndex = indexes.get(groupbyExpression.toString());
        groupKey.add(bucketedRow[columnIndex]);
      }
      if (groupKeyIndexes.containsKey(groupKey)) {
        groupKeyArray[i] = groupKeyIndexes.get(groupKey);
      } else {
        // create the new groupBy Result row and fill the group by key
        groupKeyArray[i] = groupKeyIndexes.size();
        groupKeyIndexes.put(groupKey, groupKeyIndexes.size());
        Object[] row = new Object[_queryContext.getSelectExpressions().size()];
        for (int j = 0; j < _queryContext.getSelectExpressions().size(); j++) {
          ExpressionContext expressionContext = _queryContext.getSelectExpressions().get(j);
          if (expressionContext.getType() != ExpressionContext.Type.FUNCTION) {
            row[j] = bucketedRow[indexes.get(expressionContext.toString())];
          }
        }
        aggregatedResult.add(row);
      }
    }

    Map<ExpressionContext, BlockValSet> blockValSetMap = new HashMap<>();
    for (int i = 1; i < dataSchema.getColumnNames().length; i++) {
      blockValSetMap.put(ExpressionContext.forIdentifier(dataSchema.getColumnName(i)),
          new ColumnDataToBlockValSetConverter(dataSchema.getColumnDataType(i), bucketedRows, i));
    }

    for (int i = 0; i < _queryContext.getSelectExpressions().size(); i++) {
      ExpressionContext expressionContext = _queryContext.getSelectExpressions().get(i);
      if (expressionContext.getType() == ExpressionContext.Type.FUNCTION) {
        FunctionContext functionContext = expressionContext.getFunction();
        AggregationFunction aggregationFunction =
            AggregationFunctionFactory.getAggregationFunction(functionContext, _queryContext);
        GroupByResultHolder groupByResultHolder =
            aggregationFunction.createGroupByResultHolder(_groupByKeys.size(), _groupByKeys.size());
        aggregationFunction.aggregateGroupBySV(bucketedRows.size(), groupKeyArray, groupByResultHolder, blockValSetMap);
        for (int j = 0; j < groupKeyIndexes.size(); j++) {
          Object[] row = aggregatedResult.get(j);
          row[i] = aggregationFunction.extractGroupByResult(groupByResultHolder, j);
          row[i] = aggregationFunction.extractFinalResult(row[i]);
        }
      }
    }
    return aggregatedResult;
  }

  private Object getFillValue(int columnIndex, String columnName, Object key, ColumnDataType dataType) {
    ExpressionContext expressionContext = _fillExpressions.get(columnName);
    if (expressionContext != null && expressionContext.getFunction() != null && GapfillUtils
        .isFill(expressionContext)) {
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
   * Merge all result tables from different pinot servers and sort the rows based on timebucket.
   */
  private List<Object[]>[] putRawRowsIntoTimeBucket(List<Object[]> rows) {
    List<Object[]>[] bucketedItems = new List[_numOfTimeBuckets];

    for (Object[] row: rows) {
      long timeBucket = _dateTimeFormatter.fromFormatToMillis(String.valueOf(row[_timeBucketColumnIndex]));
      int index = findBucketIndex(timeBucket);
      if (bucketedItems[index] == null) {
        bucketedItems[index] = new ArrayList<>();
      }
      bucketedItems[index].add(row);
      _groupByKeys.add(constructGroupKeys(row));
    }
    return bucketedItems;
  }
}
