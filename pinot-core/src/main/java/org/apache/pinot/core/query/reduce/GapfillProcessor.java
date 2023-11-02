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
import org.apache.pinot.core.operator.docvalsets.RowBasedBlockValSet;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionFactory;
import org.apache.pinot.core.query.aggregation.function.CountAggregationFunction;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.util.GapfillUtils;


/**
 * Helper class to reduce and set gap fill results into the BrokerResponseNative
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class GapfillProcessor extends BaseGapfillProcessor {

  private final Set<Key> _groupByKeys;
  private final Map<String, ExpressionContext> _fillExpressions;
  private int[] _sourceColumnIndexForResultSchema = null;

  GapfillProcessor(QueryContext queryContext, GapfillUtils.GapfillType gapfillType) {
    super(queryContext, gapfillType);

    _fillExpressions = GapfillUtils.getFillExpressions(_gapFillSelection);

    _groupByKeys = new HashSet<>();
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

    List<Object[]> resultRows = gapFillAndAggregate(timeBucketedRawRows, resultTableSchema, dataSchema);
    brokerResponseNative.setResultTable(new ResultTable(resultTableSchema, resultRows));
  }

  private List<Object[]> gapFillAndAggregate(List<Object[]>[] timeBucketedRawRows,
      DataSchema dataSchemaForAggregatedResult, DataSchema dataSchema) {
    List<Object[]> result = new ArrayList<>();

    GapfillFilterHandler postGapfillFilterHandler = null;
    if (_queryContext.getSubquery() != null && _queryContext.getFilter() != null) {
      postGapfillFilterHandler = new GapfillFilterHandler(_queryContext.getFilter(), dataSchema);
    }
    GapfillFilterHandler postAggregateHavingFilterHandler = null;
    if (_queryContext.getHavingFilter() != null) {
      postAggregateHavingFilterHandler =
          new GapfillFilterHandler(_queryContext.getHavingFilter(), dataSchemaForAggregatedResult);
    }
    long start = _startMs;
    ColumnDataType[] resultColumnDataTypes = dataSchema.getColumnDataTypes();
    List<Object[]> bucketedResult = new ArrayList<>();
    for (long time = _startMs; time < _endMs; time += _gapfillTimeBucketSize) {
      int index = findGapfillBucketIndex(time);
      gapfill(time, bucketedResult, timeBucketedRawRows[index], dataSchema, postGapfillFilterHandler);
      if (_queryContext.getAggregationFunctions() == null) {
        for (Object[] row : bucketedResult) {
          Object[] resultRow = new Object[_sourceColumnIndexForResultSchema.length];
          for (int i = 0; i < _sourceColumnIndexForResultSchema.length; i++) {
            resultRow[i] = row[_sourceColumnIndexForResultSchema[i]];
          }
          result.add(resultRow);
        }
        bucketedResult.clear();
      } else if (index % _aggregationSize == _aggregationSize - 1) {
        if (bucketedResult.size() > 0) {
          Object timeCol;
          if (resultColumnDataTypes[_timeBucketColumnIndex] == ColumnDataType.LONG) {
            timeCol = Long.valueOf(_dateTimeFormatter.fromMillisToFormat(start));
          } else {
            timeCol = _dateTimeFormatter.fromMillisToFormat(start);
          }
          List<Object[]> aggregatedRows = aggregateGapfilledData(timeCol, bucketedResult, dataSchema);
          for (Object[] aggregatedRow : aggregatedRows) {
            if (postAggregateHavingFilterHandler == null || postAggregateHavingFilterHandler.isMatch(aggregatedRow)) {
              result.add(aggregatedRow);
            }
            if (result.size() >= _limitForAggregatedResult) {
              return result;
            }
          }
          bucketedResult.clear();
        }
        start = time + _gapfillTimeBucketSize;
      }
    }
    return result;
  }

  private void gapfill(long bucketTime, List<Object[]> bucketedResult, List<Object[]> rawRowsForBucket,
      DataSchema dataSchema, GapfillFilterHandler postGapfillFilterHandler) {
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
      if (resultColumnDataTypes[_timeBucketColumnIndex] == ColumnDataType.LONG) {
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
  }

  private List<Object[]> aggregateGapfilledData(Object timeCol, List<Object[]> bucketedRows, DataSchema dataSchema) {
    List<ExpressionContext> groupbyExpressions = _queryContext.getGroupByExpressions();
    Preconditions.checkArgument(groupbyExpressions != null, "No GroupBy Clause.");
    Map<String, Integer> indexes = new HashMap<>();
    for (int i = 0; i < dataSchema.getColumnNames().length; i++) {
      indexes.put(dataSchema.getColumnName(i), i);
    }

    for (Object[] bucketedRow : bucketedRows) {
      bucketedRow[_timeBucketColumnIndex] = timeCol;
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
          new RowBasedBlockValSet(dataSchema.getColumnDataType(i), bucketedRows, i,
              _queryContext.getNullMode().nullAtQueryTime()));
    }

    for (int i = 0; i < _queryContext.getSelectExpressions().size(); i++) {
      ExpressionContext expressionContext = _queryContext.getSelectExpressions().get(i);
      if (expressionContext.getType() == ExpressionContext.Type.FUNCTION) {
        FunctionContext functionContext = expressionContext.getFunction();
        AggregationFunction aggregationFunction =
            AggregationFunctionFactory.getAggregationFunction(functionContext, _queryContext.getNullMode());
        GroupByResultHolder groupByResultHolder =
            aggregationFunction.createGroupByResultHolder(groupKeyIndexes.size(), groupKeyIndexes.size());
        if (aggregationFunction instanceof CountAggregationFunction) {
          aggregationFunction.aggregateGroupBySV(bucketedRows.size(), groupKeyArray, groupByResultHolder,
              new HashMap<ExpressionContext, BlockValSet>());
        } else {
          aggregationFunction
              .aggregateGroupBySV(bucketedRows.size(), groupKeyArray, groupByResultHolder, blockValSetMap);
        }
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
      GapfillUtils.FillType fillType = GapfillUtils.FillType.valueOf(args.get(1).getLiteral().getStringValue());
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

    for (Object[] row : rows) {
      long timeBucket = _dateTimeFormatter.fromFormatToMillis(String.valueOf(row[_timeBucketColumnIndex]));
      int index = findGapfillBucketIndex(timeBucket);
      if (index >= _numOfTimeBuckets) {
        // the data will not be used for gapfill, skip it
        continue;
      }
      Key key = constructGroupKeys(row);
      _groupByKeys.add(key);
      if (index < 0) {
        // the data can potentially be used for previous value
        _previousByGroupKey.compute(key, (k, previousRow) -> {
          if (previousRow == null) {
            return row;
          } else {
            long previousTimeBucket =
                _dateTimeFormatter.fromFormatToMillis(String.valueOf(previousRow[_timeBucketColumnIndex]));
            if (timeBucket > previousTimeBucket) {
              return row;
            } else {
              return previousRow;
            }
          }
        });
      } else {
        if (bucketedItems[index] == null) {
          bucketedItems[index] = new ArrayList<>();
        }
        bucketedItems[index].add(row);
      }
    }
    return bucketedItems;
  }
}
