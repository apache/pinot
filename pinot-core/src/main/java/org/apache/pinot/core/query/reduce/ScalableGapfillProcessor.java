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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionFactory;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.util.GapfillUtils;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.DateTimeGranularitySpec;


/**
 * Helper class to reduce and set gap fill results into the BrokerResponseNative
 */
abstract class ScalableGapfillProcessor {
  protected final QueryContext _queryContext;

  protected final int _limitForAggregatedResult;
  protected final DateTimeGranularitySpec _gapfillDateTimeGranularity;
  protected final DateTimeFormatSpec _dateTimeFormatter;
  protected final long _startMs;
  protected final long _endMs;
  protected final long _gapfillTimeBucketSize;
  protected final int _numOfTimeBuckets;
  protected final List<Integer> _groupByKeyIndexes;
  protected final Map<Key, Object[]> _previousByGroupKey;
  protected long _count = 0;
  protected final List<ExpressionContext> _timeSeries;
  protected final int _timeBucketColumnIndex;
  protected GapfillFilterHandler _postGapfillFilterHandler = null;
  protected GapfillFilterHandler _postAggregateHavingFilterHandler = null;

  ScalableGapfillProcessor(QueryContext queryContext, GapfillUtils.GapfillType gapfillType) {
    _queryContext = queryContext;
    _limitForAggregatedResult = queryContext.getLimit();

    ExpressionContext gapFillSelection = GapfillUtils.getGapfillExpressionContext(queryContext, gapfillType);
    _timeBucketColumnIndex = GapfillUtils.findTimeBucketColumnIndex(queryContext, gapfillType);

    List<ExpressionContext> args = gapFillSelection.getFunction().getArguments();

    _dateTimeFormatter = new DateTimeFormatSpec(args.get(1).getLiteral());
    _gapfillDateTimeGranularity = new DateTimeGranularitySpec(args.get(4).getLiteral());
    String start = args.get(2).getLiteral();
    _startMs = truncate(_dateTimeFormatter.fromFormatToMillis(start));
    String end = args.get(3).getLiteral();
    _endMs = truncate(_dateTimeFormatter.fromFormatToMillis(end));
    _gapfillTimeBucketSize = _gapfillDateTimeGranularity.granularityToMillis();
    _numOfTimeBuckets = (int) ((_endMs - _startMs) / _gapfillTimeBucketSize);

    _previousByGroupKey = new HashMap<>();
    _groupByKeyIndexes = new ArrayList<>();

    ExpressionContext timeseriesOn = GapfillUtils.getTimeSeriesOnExpressionContext(gapFillSelection);
    _timeSeries = timeseriesOn.getFunction().getArguments();
  }

  protected int findGapfillBucketIndex(long time) {
    return (int) ((time - _startMs) / _gapfillTimeBucketSize);
  }

  protected void replaceColumnNameWithAlias(DataSchema dataSchema) {
    QueryContext queryContext;
    queryContext = _queryContext.getSubquery().getSubquery();
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
    DataSchema resultTableSchema = getResultTableDataSchema();
    if (brokerResponseNative.getResultTable().getRows().isEmpty()) {
      brokerResponseNative.setResultTable(new ResultTable(resultTableSchema, Collections.emptyList()));
      return;
    }

    String[] columns = dataSchema.getColumnNames();

    Map<String, Integer> indexes = new HashMap<>();
    for (int i = 0; i < columns.length; i++) {
      indexes.put(columns[i], i);
    }

    boolean[] isGroupBySelections = new boolean[dataSchema.getColumnDataTypes().length];

    // The first one argument of timeSeries is time column. The left ones are defining entity.
    for (ExpressionContext entityColum : _timeSeries) {
      int index = indexes.get(entityColum.getIdentifier());
      isGroupBySelections[index] = true;
    }

    for (int i = 0; i < isGroupBySelections.length; i++) {
      if (isGroupBySelections[i]) {
        _groupByKeyIndexes.add(i);
      }
    }

    List<Object[]> rows = brokerResponseNative.getResultTable().getRows();
    List<Object[]> resultRows = gapFillAndAggregate(rows, dataSchema, resultTableSchema);
    brokerResponseNative.setResultTable(new ResultTable(resultTableSchema, resultRows));
  }

  /**
   * Constructs the DataSchema for the ResultTable.
   */
  private DataSchema getResultTableDataSchema() {
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

  protected Key constructGroupKeys(Object[] row) {
    Object[] groupKeys = new Object[_groupByKeyIndexes.size()];
    for (int i = 0; i < _groupByKeyIndexes.size(); i++) {
      groupKeys[i] = row[_groupByKeyIndexes.get(i)];
    }
    return new Key(groupKeys);
  }

  private long truncate(long epoch) {
    int sz = _gapfillDateTimeGranularity.getSize();
    return epoch / sz * sz;
  }

  abstract protected List<Object[]> gapFillAndAggregate(
      List<Object[]> rows, DataSchema dataSchema, DataSchema resultTableSchema);
}
