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
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.util.GapfillUtils;


/**
 * Helper class to reduce and set gap fill results into the BrokerResponseNative
 */
@SuppressWarnings({"rawtypes", "unchecked"})
class ScalableGapfillProcessorForCount extends ScalableGapfillProcessor {

  ScalableGapfillProcessorForCount(QueryContext queryContext, GapfillUtils.GapfillType gapfillType) {
    super(queryContext, gapfillType);
  }

  @Override
  protected void initializeAggregationValues(List<Object[]> rows, DataSchema dataSchema) {
    for (Map.Entry<Key, Integer> entry : _groupByKeys.entrySet()) {
      if (_previousByGroupKey.containsKey(entry.getKey())) {
        if (_postGapfillFilterHandler == null
            || _postGapfillFilterHandler.isMatch(_previousByGroupKey.get(entry.getKey()))) {
          _filteredArray[entry.getValue()] = entry.getValue();
          _count++;
        }
      }
    }
  }

  @Override
  protected List<Object[]> gapFillAndAggregate(List<Integer> timeBucketedRawRows, List<Object[]> rows) {
    List<Object[]> result = new ArrayList<>();

    for (long time = _startMs; time < _endMs; time += _gapfillTimeBucketSize) {
      int timeBucketIndex = findGapfillBucketIndex(time);
      int start = timeBucketedRawRows.get(timeBucketIndex);
      int end = timeBucketedRawRows.get(timeBucketIndex + 1);
      if (start < end) {
        for (int i = start; i < end; i++) {
          Object[] resultRow = rows.get(i);
          boolean isFilter = _postGapfillFilterHandler == null || _postGapfillFilterHandler.isMatch(resultRow);
          Key key = constructGroupKeys(resultRow);
          int groupKeyIndex = _groupByKeys.get(key);
          if ((_filteredArray[groupKeyIndex] >= 0) != isFilter) {
            if (isFilter) {
              _count++;
            } else {
              _count--;
            }
          }
          if (isFilter) {
            _filteredArray[groupKeyIndex] = i;
          } else {
            _filteredArray[groupKeyIndex] = -1;
          }
        }
      }
      if (_count > 0) {
        Object[] aggregatedRow = new Object[_queryContext.getSelectExpressions().size()];
        aggregatedRow[0] = _dateTimeFormatter.fromMillisToFormat(time);
        aggregatedRow[1] = _count;

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
