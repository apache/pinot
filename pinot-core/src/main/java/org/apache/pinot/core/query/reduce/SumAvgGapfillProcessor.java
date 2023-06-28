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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.util.GapfillUtils;

/**
 * Helper class to reduce and set gap fill results into the BrokerResponseNative
 *
 * {@link SumAvgGapfillProcessor} is only applying the sum and/or avg aggregation function on the gapfilled result for
 * each time bucket.
 *
 * {@link SumAvgGapfillProcessor} is different from {@link GapfillProcessor} that {@link GapfillProcessor} will create
 * the gapfilled entries, but {@link SumAvgGapfillProcessor} does not generate the gapfilled entries. It just updated
 * the aggregated sum and/or avg counters as necessary.
 */
class SumAvgGapfillProcessor extends BaseGapfillProcessor {
  private final double [] _sumes;
  private final int [] _columnTypes;
  private final int [] _sumArgIndexes;
  private final static int COLUMN_TYPE_SUM = 1;
  private final static int COLUMN_TYPE_AVG = 2;
  protected Map<Integer, Integer> _filteredMap;
  protected final Map<Key, Integer> _groupByKeys;

  SumAvgGapfillProcessor(QueryContext queryContext, GapfillUtils.GapfillType gapfillType) {
    super(queryContext, gapfillType);
    _groupByKeys = new HashMap<>();
    _columnTypes = new int[_queryContext.getSelectExpressions().size()];
    _sumArgIndexes = new int[_columnTypes.length];
    _sumes = new double[_columnTypes.length];
  }

  protected void initializeAggregationValues(List<Object[]> rows, DataSchema dataSchema) {
    for (int i = 0; i < _columnTypes.length; i++) {
      ExpressionContext expressionContext = _queryContext.getSelectExpressions().get(i);
      if (expressionContext.getType() == ExpressionContext.Type.FUNCTION) {
        if (expressionContext.getFunction().getFunctionName().equalsIgnoreCase("sum")) {
          _columnTypes[i] = COLUMN_TYPE_SUM;
        } else {
          _columnTypes[i] = COLUMN_TYPE_AVG;
        }
        ExpressionContext arg = expressionContext.getFunction().getArguments().get(0);
        for (int j = 0; j < dataSchema.getColumnNames().length; j++) {
          if (arg.getIdentifier().equalsIgnoreCase(dataSchema.getColumnName(j))) {
            _sumArgIndexes[i] = j;
            break;
          }
        }
      }
    }

    for (Map.Entry<Key, Integer> entry : _groupByKeys.entrySet()) {
      if (_previousByGroupKey.containsKey(entry.getKey())) {
        if (_postGapfillFilterHandler == null
            || _postGapfillFilterHandler.isMatch(_previousByGroupKey.get(entry.getKey()))) {
          _filteredMap.put(entry.getValue(), entry.getValue());
          for (int i = 0; i < _columnTypes.length; i++) {
            if (_columnTypes[i] != 0) {
              _sumes[i] += ((Number) rows.get(entry.getValue())[_sumArgIndexes[i]]).doubleValue();
            }
          }
          _count++;
        }
      }
    }
  }

  @Override
  protected List<Object[]> gapFillAndAggregate(
      List<Object[]> rows, DataSchema dataSchema, DataSchema resultTableSchema) {
    int [] timeBucketedRawRows = new int[_numOfTimeBuckets + 1];
    int timeBucketedRawRowsIndex = 0;
    for (int i = 0; i < rows.size(); i++) {
      Object[] row = rows.get(i);
      long time = _dateTimeFormatter.fromFormatToMillis(String.valueOf(row[_timeBucketColumnIndex]));
      int index = findGapfillBucketIndex(time);
      if (index >= _numOfTimeBuckets) {
        timeBucketedRawRows[timeBucketedRawRowsIndex++] = i;
        break;
      }
      Key key = constructGroupKeys(row);
      _groupByKeys.putIfAbsent(key, _groupByKeys.size());
      if (index < 0) {
        // the data can potentially be used for previous value
        _previousByGroupKey.compute(key, (k, previousRow) -> {
          if (previousRow == null) {
            return row;
          } else {
            if ((Long) row[_timeBucketColumnIndex] > (Long) previousRow[_timeBucketColumnIndex]) {
              return row;
            } else {
              return previousRow;
            }
          }
        });
      } else if (index >= timeBucketedRawRowsIndex) {
        while (index >= timeBucketedRawRowsIndex) {
          timeBucketedRawRows[timeBucketedRawRowsIndex++] = i;
        }
      }
    }
    while (timeBucketedRawRowsIndex < _numOfTimeBuckets + 1) {
      timeBucketedRawRows[timeBucketedRawRowsIndex++] = rows.size();
    }

    _filteredMap = new HashMap<>();

    if (_queryContext.getSubquery() != null && _queryContext.getFilter() != null) {
      _postGapfillFilterHandler = new GapfillFilterHandler(_queryContext.getFilter(), dataSchema);
    }

    if (_queryContext.getHavingFilter() != null) {
      _postAggregateHavingFilterHandler =
          new GapfillFilterHandler(_queryContext.getHavingFilter(), resultTableSchema);
    }

    initializeAggregationValues(rows, dataSchema);

    List<Object[]> result = new ArrayList<>();

    double [] aggregatedSum = new double[_columnTypes.length];
    long aggregatedCount = 0;
    for (long time = _startMs; time < _endMs; time += _gapfillTimeBucketSize) {
      int timeBucketIndex = findGapfillBucketIndex(time);
      for (int i = timeBucketedRawRows[timeBucketIndex]; i < timeBucketedRawRows[timeBucketIndex + 1]; i++) {
        Object[] resultRow = rows.get(i);
        Key key = constructGroupKeys(resultRow);
        int groupKeyIndex = _groupByKeys.get(key);
        if ((_filteredMap.containsKey(groupKeyIndex))) {
          for (int j = 0; j < _columnTypes.length; j++) {
            if (_columnTypes[j] == 0) {
              continue;
            }
            _sumes[j] -= ((Number) (rows.get(_filteredMap.get(groupKeyIndex))[_sumArgIndexes[j]])).doubleValue();
          }
          _filteredMap.remove(groupKeyIndex);
          _count--;
        }
        if (_postGapfillFilterHandler == null || _postGapfillFilterHandler.isMatch(resultRow)) {
          _count++;
          for (int j = 0; j < _columnTypes.length; j++) {
            if (_columnTypes[j] == 0) {
              continue;
            }
            _sumes[j] += ((Number) (resultRow[_sumArgIndexes[j]])).doubleValue();
          }
          _filteredMap.put(groupKeyIndex, i);
        }
      }
      if (_count > 0) {
        aggregatedCount += _count;
        for (int i = 0; i < _columnTypes.length; i++) {
          if (_columnTypes[i] != 0) {
            aggregatedSum[i] += _sumes[i];
          }
        }
      }
      if ((timeBucketIndex + 1) % _aggregationSize == 0 && aggregatedCount > 0) {
        Object[] aggregatedRow = new Object[_queryContext.getSelectExpressions().size()];
        for (int i = 0; i < _columnTypes.length; i++) {
          if (_columnTypes[i] == 0) {
            if (dataSchema.getColumnDataType(_timeBucketColumnIndex) == DataSchema.ColumnDataType.LONG) {
              aggregatedRow[i] = time - (_aggregationSize - 1) * _gapfillTimeBucketSize;
            } else {
              aggregatedRow[i] = _dateTimeFormatter.fromMillisToFormat(
                  time - (_aggregationSize - 1) * _gapfillTimeBucketSize);
            }
          } else if (_columnTypes[i] == COLUMN_TYPE_SUM) {
            aggregatedRow[i] = aggregatedSum[i];
          } else { //COLUMN_TYPE_AVG
            aggregatedRow[i] = aggregatedSum[i] / aggregatedCount;
          }
        }
        aggregatedSum = new double[_columnTypes.length];
        aggregatedCount = 0;

        if (_postAggregateHavingFilterHandler == null || _postAggregateHavingFilterHandler.isMatch(aggregatedRow)) {
          result.add(aggregatedRow);
        }
        if (result.size() >= _limitForAggregatedResult) {
          return result;
        }
      }
    }
    return result;
  }
}
