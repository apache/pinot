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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.util.GapfillUtils;


/**
 * Helper class to reduce and set gap fill results into the BrokerResponseNative
 *
 * {@link CountGapfillProcessor} is only applying the count aggregation function on the gapfilled result for each time
 * bucket.
 *
 * {@link CountGapfillProcessor} is different from {@link GapfillProcessor} that {@link GapfillProcessor} will create
 * the gapfilled entries, but {@link CountGapfillProcessor} does not generate the gapfilled entries. It just updated
 * the aggregated count as necessary.
 */
class CountGapfillProcessor extends BaseGapfillProcessor {
  protected final Set<Key> _filteredSet;

  CountGapfillProcessor(QueryContext queryContext, GapfillUtils.GapfillType gapfillType) {
    super(queryContext, gapfillType);
    _filteredSet = new HashSet<>();
  }

  @Override
  protected List<Object[]> gapFillAndAggregate(
      List<Object[]> rows, DataSchema dataSchema, DataSchema resultTableSchema) {
    DataSchema.ColumnDataType timeColumnDataType = resultTableSchema.getColumnDataTypes()[0];
    if (_queryContext.getSubquery() != null && _queryContext.getFilter() != null) {
      _postGapfillFilterHandler = new GapfillFilterHandler(_queryContext.getFilter(), dataSchema);
    }
    if (_queryContext.getHavingFilter() != null) {
      _postAggregateHavingFilterHandler =
          new GapfillFilterHandler(_queryContext.getHavingFilter(), resultTableSchema);
    }

    int rowIndex = 0;
    while (rowIndex < rows.size()) {
      Object[] row = rows.get(rowIndex);
      long rowTimestamp = extractTimeColumn(row, timeColumnDataType);
      int bucketIndex = findGapfillBucketIndex(rowTimestamp);
      if (bucketIndex >= 0) {
        break;
      }
      updateCounter(row);
      rowIndex++;
    }

    List<Object[]> result = new ArrayList<>();

    long aggregatedCount = 0;
    for (long time = _startMs; time < _endMs; time += _gapfillTimeBucketSize) {
      while (rowIndex < rows.size()) {
        Object[] row = rows.get(rowIndex);
        long rowTimestamp = extractTimeColumn(row, timeColumnDataType);
        if (rowTimestamp == time) {
          updateCounter(row);
          rowIndex++;
        } else {
          break;
        }
      }
      int timeBucketIndex = findGapfillBucketIndex(time);
      aggregatedCount += _count;
      if (aggregatedCount > 0 && (timeBucketIndex + 1) % _aggregationSize == 0) {
        Object[] aggregatedRow = new Object[_queryContext.getSelectExpressions().size()];
        long aggregationTimeBucketTimestamp = time - (_aggregationSize - 1) * _gapfillTimeBucketSize;
        aggregatedRow[0] = (timeColumnDataType == DataSchema.ColumnDataType.LONG)
            ? aggregationTimeBucketTimestamp : _dateTimeFormatter.fromMillisToFormat(aggregationTimeBucketTimestamp);
        aggregatedRow[1] = aggregatedCount;
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

  private long extractTimeColumn(Object[] row, DataSchema.ColumnDataType columnDataType) {
    if (columnDataType == DataSchema.ColumnDataType.LONG) {
      return (Long) row[_timeBucketColumnIndex];
    } else {
      return _dateTimeFormatter.fromFormatToMillis((String) row[_timeBucketColumnIndex]);
    }
  }

  private void updateCounter(Object[] row) {
    Key key = constructGroupKeys(row);
    boolean isFilter = _postGapfillFilterHandler == null || _postGapfillFilterHandler.isMatch(row);
    if (_filteredSet.contains(key) != isFilter) {
      if (isFilter) {
        _count++;
      } else {
        _count--;
      }
    }
    if (isFilter) {
      _filteredSet.add(key);
    } else {
      _filteredSet.remove(key);
    }
  }
}
