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
 */
class ScalableGapfillProcessorForCount extends ScalableGapfillProcessor {
  protected Set<Key> _filteredSet;

  ScalableGapfillProcessorForCount(QueryContext queryContext, GapfillUtils.GapfillType gapfillType) {
    super(queryContext, gapfillType);
  }

  @Override
  protected List<Object[]> gapFillAndAggregate(
      List<Object[]> rows, DataSchema dataSchema, DataSchema resultTableSchema) {
    DataSchema.ColumnDataType columnDataType = resultTableSchema.getColumnDataTypes()[0];
    if (columnDataType == DataSchema.ColumnDataType.LONG) {
      return gapFillAndAggregateWithLongAsTimeCol(rows, dataSchema, resultTableSchema);
    } else {
      return gapFillAndAggregateWithStringAsTimeCol(rows, dataSchema, resultTableSchema);
    }
  }

  protected List<Object[]> gapFillAndAggregateWithLongAsTimeCol(
      List<Object[]> rows, DataSchema dataSchema, DataSchema resultTableSchema) {
    replaceColumnNameWithAlias(dataSchema);
    if (_queryContext.getSubquery() != null && _queryContext.getFilter() != null) {
      _postGapfillFilterHandler = new GapfillFilterHandler(_queryContext.getFilter(), dataSchema);
    }
    if (_queryContext.getHavingFilter() != null) {
      _postAggregateHavingFilterHandler =
          new GapfillFilterHandler(_queryContext.getHavingFilter(), resultTableSchema);
    }
    _filteredSet = new HashSet<>();

    int index = 0;
    while (index < rows.size()) {
      Object[] row = rows.get(index);
      long time = (Long) row[_timeBucketColumnIndex];
      int bucketIndex = findGapfillBucketIndex(time);
      if (bucketIndex >= 0) {
        break;
      }
      updateCounter(row);
    }

    List<Object[]> result = new ArrayList<>();

    int bucketIndex = -1;
    long bucketTimestamp = -1;
    while (index < rows.size()) {
      Object[] row = rows.get(index);
      long time = (Long) row[_timeBucketColumnIndex];
      int timeBucketIndex = findGapfillBucketIndex(time);
      if (bucketIndex == -1) {
        bucketIndex = timeBucketIndex;
        bucketTimestamp = _startMs;
      } else if (bucketIndex != timeBucketIndex) {
        if (_count > 0) {
          Object[] aggregatedRow = new Object[_queryContext.getSelectExpressions().size()];
          aggregatedRow[0] = bucketTimestamp;
          aggregatedRow[1] = _count;

          if (_postAggregateHavingFilterHandler == null || _postAggregateHavingFilterHandler.isMatch(aggregatedRow)) {
            result.add(aggregatedRow);
            bucketIndex++;
            bucketTimestamp += _gapfillTimeBucketSize;
            while (bucketIndex < timeBucketIndex) {
              time += _gapfillTimeBucketSize;
              aggregatedRow = new Object[_queryContext.getSelectExpressions().size()];
              aggregatedRow[0] = bucketTimestamp;
              aggregatedRow[1] = _count;
              result.add(aggregatedRow);
              bucketIndex++;
              bucketTimestamp += _gapfillTimeBucketSize;
            }
          }
          if (result.size() >= _limitForAggregatedResult) {
            return result;
          }
        }
        bucketIndex = timeBucketIndex;
        bucketTimestamp = _startMs + bucketIndex * _gapfillTimeBucketSize;
      }
      if (timeBucketIndex < _numOfTimeBuckets) {
        updateCounter(row);
        index++;
      }
    }
    return result;
  }

  protected List<Object[]> gapFillAndAggregateWithStringAsTimeCol(
      List<Object[]> rows, DataSchema dataSchema, DataSchema resultTableSchema) {
    replaceColumnNameWithAlias(dataSchema);
    if (_queryContext.getSubquery() != null && _queryContext.getFilter() != null) {
      _postGapfillFilterHandler = new GapfillFilterHandler(_queryContext.getFilter(), dataSchema);
    }
    if (_queryContext.getHavingFilter() != null) {
      _postAggregateHavingFilterHandler =
          new GapfillFilterHandler(_queryContext.getHavingFilter(), resultTableSchema);
    }

    int index = 0;
    while (index < rows.size()) {
      Object[] row = rows.get(index);
      long time = _dateTimeFormatter.fromFormatToMillis(String.valueOf(row[_timeBucketColumnIndex]));
      int bucketIndex = findGapfillBucketIndex(time);
      if (bucketIndex >= 0) {
        break;
      }
      updateCounter(row);
    }

    _filteredSet = new HashSet<>();

    List<Object[]> result = new ArrayList<>();

    int bucketIndex = -1;
    while (index < rows.size()) {
      Object[] row = rows.get(index);
      long time = _dateTimeFormatter.fromFormatToMillis(String.valueOf(row[_timeBucketColumnIndex]));
      int timeBucketIndex = findGapfillBucketIndex(time);
      if (bucketIndex == -1) {
        bucketIndex = timeBucketIndex;
      } else if (bucketIndex != timeBucketIndex) {
        if (_count > 0) {
          Object[] aggregatedRow = new Object[_queryContext.getSelectExpressions().size()];
          aggregatedRow[0] = _dateTimeFormatter.fromMillisToFormat(_startMs + bucketIndex * _gapfillTimeBucketSize);
          aggregatedRow[1] = _count;

          if (_postAggregateHavingFilterHandler == null || _postAggregateHavingFilterHandler.isMatch(aggregatedRow)) {
            result.add(aggregatedRow);
            bucketIndex++;
            while (bucketIndex < timeBucketIndex) {
              time += _gapfillTimeBucketSize;
              aggregatedRow = new Object[_queryContext.getSelectExpressions().size()];
              aggregatedRow[0] = _dateTimeFormatter.fromMillisToFormat(_startMs + bucketIndex * _gapfillTimeBucketSize);
              aggregatedRow[1] = _count;
              result.add(aggregatedRow);
              bucketIndex++;
            }
          }
          if (result.size() >= _limitForAggregatedResult) {
            return result;
          }
        }
        bucketIndex = timeBucketIndex;
      }
      if (timeBucketIndex < _numOfTimeBuckets) {
        updateCounter(row);
        index++;
      }
    }
    return result;
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
