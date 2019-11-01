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

import java.util.List;
import java.util.Map;
import org.apache.pinot.thirdeye.datasource.MetricFunction;
import org.apache.pinot.thirdeye.datasource.ThirdEyeResponse;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;


/**
 * Class used to represent the response from fetch request to centralized cache.
 */

public class ThirdEyeCacheResponse {
  private final ThirdEyeCacheRequest request;
  private List<TimeSeriesDataPoint> rows;
  private long firstTimestamp;
  private long lastTimestamp;

  public ThirdEyeCacheResponse(ThirdEyeCacheRequest request, List<TimeSeriesDataPoint> rows) {
    this.request = request;
    this.rows = rows;
    this.initializeFirstAndLastTimestamps();
  }

  /**
   * sets the first and last timestamps from the cache result. If no documents
   * were fetched (we had a complete cache miss), firstTimestamp and lastTimestamp
   * will be initialized to Long.MAX_VALUE and Long.MIN_VALUE respectively so that
   * they don't cause issues with checking for missing data slices.
   */
  private void initializeFirstAndLastTimestamps() {
    if (this.hasNoRows()) {
      this.setFirstTimestamp(Long.MAX_VALUE);
      this.setLastTimestamp(Long.MIN_VALUE);
    } else {
      this.setFirstTimestamp(rows.get(0).getTimestamp());
      this.setLastTimestamp(rows.get(rows.size() - 1).getTimestamp());
    }
  }

  public ThirdEyeCacheRequest getCacheRequest() { return request; }
  public List<TimeSeriesDataPoint> getRows() { return rows; }

  public int getNumRows() { return rows.size(); }
  public boolean hasNoRows() { return rows.isEmpty(); }
  public long getFirstTimestamp() { return firstTimestamp; }
  public long getLastTimestamp() { return lastTimestamp; }

  public void setFirstTimestamp(long timestamp) {
    this.firstTimestamp = timestamp;
  }

  public void setLastTimestamp(long timestamp) {
    this.lastTimestamp = timestamp;
  }

  /**
   * Checks if cache response is missing any data
   * @param sliceStart original requested start time
   * @param sliceEnd original requested end time
   * @return whether data fetched from cache is missing any data for the original request
   */

  public boolean isMissingSlice(long sliceStart, long sliceEnd) {
    return isMissingStartSlice(sliceStart) || isMissingEndSlice(sliceEnd);
  }

  /**
   * Checks if cache response is missing data from the start of the time series.
   * @param sliceStart
   * @return true or false
   */
  public boolean isMissingStartSlice(long sliceStart) {
    if (this.hasNoRows()) {
      return true;
    }
    long timeGranularity = request.getRequest().getGroupByTimeGranularity().toMillis();
    return sliceStart <= getFirstTimestamp() - timeGranularity;
  }

  /**
   * Checks if cache response is missing data from the end of the time series.
   * @param sliceEnd
   * @return true or false
   */
  public boolean isMissingEndSlice(long sliceEnd) {
    if (this.hasNoRows()) {
      return true;
    }

    // note: sliceEnd is exclusive so we don't need equality check.
    long timeGranularity = request.getRequest().getGroupByTimeGranularity().toMillis();
    return sliceEnd > getLastTimestamp() + timeGranularity;
  }

  /**
   * Used to merge in some new time-series slice into the rows we currently have.
   * The main use case is for when we have a partial cache hit, i.e. only part of the
   * requested data was found in the cache, and need to merge in the new slice that
   * we fetched from the data source to "complete" the time-series. Also updates
   * the first and last timestamps if needed.
   * @param slice response object from fetching from data source
   */

  public void mergeSliceIntoRows(ThirdEyeResponse slice) {

    for (MetricFunction metric : slice.getMetricFunctions()) {
      String metricUrn = MetricEntity.fromMetric(slice.getRequest().getFilterSet().asMap(), metric.getMetricId()).getUrn();
      for (int i = 0; i < slice.getNumRowsFor(metric); i++) {
        Map<String, String> row = slice.getRow(metric, i);
        long timestamp = Long.valueOf(row.get(CacheConstants.TIMESTAMP));

        if (timestamp < this.getFirstTimestamp()) {
          this.setFirstTimestamp(timestamp);
        }
        if (timestamp > this.getLastTimestamp()) {
          this.setLastTimestamp(timestamp);
        }

        rows.add(new TimeSeriesDataPoint(metricUrn, timestamp, metric.getMetricId(), row.get(metric.toString())));
      }
    }
  }
}
