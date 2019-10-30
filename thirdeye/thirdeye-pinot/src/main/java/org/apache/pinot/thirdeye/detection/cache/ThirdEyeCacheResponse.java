/*
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

package org.apache.pinot.thirdeye.detection.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.thirdeye.datasource.MetricFunction;
import org.apache.pinot.thirdeye.datasource.ThirdEyeResponse;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;

public class ThirdEyeCacheResponse {
  private final ThirdEyeCacheRequest request;
  private List<TimeSeriesDataPoint> rows;

  public ThirdEyeCacheResponse(ThirdEyeCacheRequest request, List<TimeSeriesDataPoint> rows) {
    this.request = request;
    this.rows = rows;
  }

  public ThirdEyeCacheRequest getCacheRequest() { return request; }
  public List<TimeSeriesDataPoint> getRows() { return rows; }

  public int getNumRows() { return rows.size(); }
  public boolean hasNoRows() { return rows.isEmpty(); }
  public long getFirstTimestamp() { return rows.get(0).getTimestamp(); }
  public long getLastTimestamp() { return rows.get(rows.size() - 1).getTimestamp(); }

  public boolean isMissingSlice(long sliceStart, long sliceEnd) {
    return isMissingStartSlice(sliceStart) || isMissingEndSlice(sliceEnd);
  }

  public boolean isMissingStartSlice(long sliceStart) {
    if (this.hasNoRows()) {
      return true;
    }
    long timeGranularity = request.getRequest().getGroupByTimeGranularity().toMillis();
    return sliceStart <= getFirstTimestamp() - timeGranularity;
  }

  public boolean isMissingEndSlice(long sliceEnd) {
    if (this.hasNoRows()) {
      return true;
    }

    // note: sliceEnd is exclusive so we don't need equality check.
    long timeGranularity = request.getRequest().getGroupByTimeGranularity().toMillis();
    return sliceEnd > getLastTimestamp() + timeGranularity;
  }

  public void mergeSliceIntoRows(ThirdEyeResponse slice, MergeSliceType type) {

    List<TimeSeriesDataPoint> sliceRows = new ArrayList<>();

    for (MetricFunction metric : slice.getMetricFunctions()) {
      String metricUrn = MetricEntity.fromMetric(slice.getRequest().getFilterSet().asMap(), metric.getMetricId()).getUrn();
      for (int i = 0; i < slice.getNumRowsFor(metric); i++) {
        Map<String, String> row = slice.getRow(metric, i);
        sliceRows.add(new TimeSeriesDataPoint(metricUrn, Long.valueOf(row.get(CacheConstants.TIMESTAMP)), metric.getMetricId(), row.get(metric.toString())));
      }
    }

    if (type == MergeSliceType.PREPEND) {
      sliceRows.addAll(rows);
      this.rows = sliceRows;
    } else if (type == MergeSliceType.APPEND) {
      rows.addAll(sliceRows);
    }
  }
}
